const async = require('async');
const http = require('http');
const jsonStream = require('JSONStream');

const { Logger } = require('werelogs');
const { jsutil, errors } = require('arsenal');

const getBucketdURL = require('../VerifyBucketSproxydKeys/getBucketdURL');
const getRepairStrategy = require('./RepairObjects/getRepairStrategy');

const {
    BUCKETD_HOSTPORT, SPROXYD_HOSTPORT,
} = process.env;

const VERBOSE = process.env.VERBOSE === '1';

const USAGE = `
CompareRaftMembers/repairObjects.js

This script checks each entry from stdin, corresponding to a line from
the output of followerDiff.js, and repairs the object on metadata if
deemed safe to do so with a readable version.

Usage:
    node CompareRaftMembers/repairObjects.js

Mandatory environment variables:
    BUCKETD_HOSTPORT: ip:port of bucketd endpoint
    SPROXYD_HOSTPORT: ip:port of sproxyd endpoint

Optional environment variables:
    VERBOSE: set to 1 for more verbose output (shows one line for every sproxyd key checked)
`;

if (!BUCKETD_HOSTPORT) {
    console.error('ERROR: BUCKETD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}
if (!SPROXYD_HOSTPORT) {
    console.error('ERROR: SPROXYD_HOSTPORT not defined');
    console.error(USAGE);
    process.exit(1);
}

const log = new Logger('s3utils:CompareRaftMembers:repairObjects');

const countByStatus = {
    AutoRepair: 0,
    AutoRepairError: 0,
    ManualRepair: 0,
    NotRepairable: 0,
    UpdatedByClient: 0,
};

const httpAgent = new http.Agent({
    keepAlive: true,
});

let sproxydAlias;

function httpRequest(method, url, requestBody, cb) {
    const cbOnce = jsutil.once(cb);
    const urlObj = new URL(url);
    const req = http.request({
        hostname: urlObj.hostname,
        port: urlObj.port,
        path: `${urlObj.pathname}${urlObj.search}`,
        method,
        agent: httpAgent,
    }, res => {
        if (method === 'HEAD') {
            return cbOnce(null, res);
        }
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.once('end', () => {
            const body = chunks.join('');
            // eslint-disable-next-line no-param-reassign
            res.body = body;
            return cbOnce(null, res);
        });
        res.once('error', err => cbOnce(new Error(
            'error reading response from HTTP request '
                + `to ${url}: ${err.message}`
        )));
        return undefined;
    });
    req.once('error', err => cbOnce(new Error(
        `error sending HTTP request to ${url}: ${err.message}`
    )));
    req.end(requestBody || undefined);
}

function getSproxydAlias(cb) {
    const url = `http://${SPROXYD_HOSTPORT}/.conf`;
    httpRequest('GET', url, null, (err, res) => {
        if (err) {
            return cb(err);
        }
        if (res.statusCode !== 200) {
            return cb(new Error(
                `GET ${url} returned status ${res.statusCode}`
            ));
        }
        const resp = JSON.parse(res.body);
        sproxydAlias = resp['ring_driver:0'].alias;
        return cb();
    });
}

function checkSproxydKeys(bucketdUrl, locations, cb) {
    if (!locations) {
        return process.nextTick(cb);
    }
    return async.eachSeries(locations, (loc, locDone) => {
        // existence check
        const sproxydUrl = `http://${SPROXYD_HOSTPORT}/${sproxydAlias}/${loc.key}`;
        httpRequest('HEAD', sproxydUrl, null, (err, res) => {
            if (err) {
                log.error('sproxyd check error', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                    error: { message: err.message },
                });
                locDone(err);
            } else if (res.statusCode === 404) {
                log.error('sproxyd check reported missing key', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                });
                locDone(errors.LocationNotFound);
            } else if (res.statusCode !== 200) {
                log.error('sproxyd check returned HTTP error', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                    httpCode: res.statusCode,
                });
                locDone(err);
            } else {
                if (VERBOSE) {
                    log.info('sproxyd check returned success', {
                        bucketdUrl,
                        sproxydKey: loc.key,
                    });
                }
                locDone();
            }
        });
    }, cb);
}

function parseDiffKey(diffKey) {
    const slashPos = diffKey.indexOf('/');
    const [bucket, key] = [diffKey.slice(0, slashPos), diffKey.slice(slashPos + 1)];
    return { bucket, key };
}

function repairObjectMD(bucketdUrl, repairMd, cb) {
    return httpRequest('POST', bucketdUrl, repairMd, cb);
}

function repairDiffEntry(diffEntry, cb) {
    const { bucket, key } = parseDiffKey(diffEntry[0] ? diffEntry[0].key : diffEntry[1].key);
    const bucketdUrl = getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        Key: key,
    });
    async.waterfall([
        next => async.map(diffEntry, (diffItem, itemDone) => {
            if (!diffItem) {
                return itemDone(null, false);
            }
            const parsedMd = JSON.parse(diffItem.value);
            checkSproxydKeys(bucketdUrl, parsedMd.location, err => itemDone(null, err ? false : true));
        }, next),
        ([
            followerIsReadable,
            leaderIsReadable,
        ], next) => httpRequest('GET', bucketdUrl, null, (err, res) => {
            if (err || (res.statusCode !== 200 && res.statusCode !== 404)) {
                log.error('error during HTTP request to check object metadata prior to repair', {
                    bucket,
                    key,
                    error: err && err.message,
                    statusCode: res.statusCode,
                });
                return next(err);
            }
            const refreshedLeaderMd = (res.statusCode === 200 ? res.body : null);
            const followerState = {
                diffMd: diffEntry[0] && diffEntry[0].value,
                isReadable: followerIsReadable,
            };
            const leaderState = {
                diffMd: diffEntry[1] && diffEntry[1].value,
                refreshedMd: refreshedLeaderMd,
                isReadable: leaderIsReadable,
            };
            return next(null, followerState, leaderState);
        }),
        (followerState, leaderState, next) => {
            const repairStrategy = getRepairStrategy(followerState, leaderState);
            const logFunc = repairStrategy.status === 'NotRepairable' ? log.warn : log.info;
            logFunc.bind(log)(repairStrategy.message, {
                bucket,
                key,
                repairStatus: repairStrategy.status,
            });
            countByStatus[repairStrategy.status] += 1;
            if (repairStrategy.status !== 'AutoRepair') {
                return next();
            }
            const repairMd = repairStrategy.source === 'Follower'
                  ? followerState.diffMd
                  : leaderState.diffMd;
            return repairObjectMD(bucketdUrl, repairMd, err => {
                if (err) {
                    countByStatus.AutoRepairError += 1;
                    return next(err);
                }
                return next();
            });
        },
        (repairResult, next) => {
            if (repairResult.statusCode !== 200) {
                log.error('HTTP request to repair object returned an error status', {
                    bucket,
                    key,
                    statusCode: repairResult.statusCode,
                    statusMessage: repairResult.body,
                });
                countByStatus.AutoRepairError += 1;
                return next(errors.InternalError);
            }
            return next();
        }
    ], () => cb());
}

function consumeStdin(cb) {
    const diffEntryStream = process.stdin.pipe(jsonStream.parse());
    diffEntryStream
        .on('data', diffEntry => {
            diffEntryStream.pause();
            return repairDiffEntry(diffEntry, () => {
                diffEntryStream.resume();
            });
        })
        .on('end', () => {
            cb();
        })
        .on('error', err => {
            log.error('error parsing JSON input', {
                error: err.message,
            });
            return cb(err);
        });
}

function main() {
    async.series([
        done => getSproxydAlias(done),
        done => consumeStdin(done),
    ], err => {
        if (err) {
            log.error('an error occurred during repair', {
                error: { message: err.message },
            });
            process.exit(1);
        } else {
            log.info('completed repair', { countByStatus });
            process.exit(0);
        }
    });
}

main();

function stop() {
    log.info('stopping execution');
    process.exit(0);
}

process.on('SIGINT', stop);
process.on('SIGHUP', stop);
process.on('SIGQUIT', stop);
process.on('SIGTERM', stop);
