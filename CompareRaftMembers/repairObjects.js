/* eslint-disable no-console */

const async = require('async');
const http = require('http');
const jsonStream = require('JSONStream');

const { Logger } = require('werelogs');
const { jsutil, errors, versioning } = require('arsenal');

const getBucketdURL = require('../VerifyBucketSproxydKeys/getBucketdURL');
const getRepairStrategy = require('./RepairObjects/getRepairStrategy');

const VID_SEP = versioning.VersioningConstants.VersionId.Separator;

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

const retryDelayMs = 1000;
const maxRetryDelayMs = 10000;

const retryParams = {
    times: 20,
    interval: retryCount => Math.min(
        // the first retry comes as "retryCount=2", hence substract 2
        retryDelayMs * (2 ** (retryCount - 2)),
        maxRetryDelayMs,
    ),
};

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
            `error reading response from HTTP request to ${url}: ${err.message}`,
        )));
        return undefined;
    });
    req.once('error', err => cbOnce(new Error(
        `error sending HTTP request to ${url}: ${err.message}`,
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
                `GET ${url} returned status ${res.statusCode}`,
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
        let locationNotFound = false;
        async.retry(retryParams, reqDone => httpRequest('HEAD', sproxydUrl, null, (err, res) => {
            if (err) {
                log.error('sproxyd check error', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                    error: { message: err.message },
                });
                reqDone(err);
            } else if (res.statusCode === 404) {
                log.error('sproxyd check reported missing key', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                });
                locationNotFound = true;
                reqDone();
            } else if (res.statusCode !== 200) {
                log.error('sproxyd check returned HTTP error', {
                    bucketdUrl,
                    sproxydKey: loc.key,
                    httpCode: res.statusCode,
                });
                reqDone(err);
            } else {
                if (VERBOSE) {
                    log.info('sproxyd check returned success', {
                        bucketdUrl,
                        sproxydKey: loc.key,
                    });
                }
                reqDone();
            }
        }), err => {
            if (err) {
                return locDone(err);
            }
            if (locationNotFound) {
                return locDone(errors.LocationNotFound);
            }
            return locDone();
        });
    }, cb);
}

function parseDiffKey(diffKey) {
    const slashPos = diffKey.indexOf('/');
    const [bucket, key] = [diffKey.slice(0, slashPos), diffKey.slice(slashPos + 1)];

    // fetching info from the master key in case we are repairing a
    // version key: in such case if the associated master key is the
    // same version or does not exist, we also need to repair it with
    // the same metadata to keep the object consistency.
    const vidSepPos = key.indexOf(VID_SEP);
    const masterKeyInfo = vidSepPos !== -1 ? {
        key: key.slice(0, vidSepPos),
        versionId: key.slice(vidSepPos + 1),
    } : null;
}

function repairObjectMD(bucketdUrl, repairMd, cb) {
    return httpRequest('POST', bucketdUrl, repairMd, cb);
}

function repairDiffEntry(diffEntry, cb) {
    const {
        bucket,
        key,
        masterKeyInfo,
    } = parseDiffKey(diffEntry[0] ? diffEntry[0].key : diffEntry[1].key);
    const bucketdUrl = getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        Key: key,
    });
    const masterBucketdUrl = masterKeyInfo ? getBucketdURL(BUCKETD_HOSTPORT, {
        Bucket: bucket,
        Key: masterKeyInfo.key,
    }) : null;

    async.waterfall([
        next => async.map(diffEntry, (diffItem, itemDone) => {
            if (!diffItem) {
                return itemDone(null, false);
            }
            const parsedMd = JSON.parse(diffItem.value);
            return checkSproxydKeys(
                bucketdUrl,
                parsedMd.location,
                err => itemDone(null, !err),
            );
        }, next),
        ([
            followerIsReadable,
            leaderIsReadable,
        ], next) => async.map(
            [bucketdUrl, masterBucketdUrl],
            (url, urlDone) => {
                if (!url) {
                    return urlDone();
                }
                return async.retry(
                    retryParams,
                    reqDone => httpRequest('GET', url, null, (err, res) => {
                        if (err || (res.statusCode !== 200 && res.statusCode !== 404)) {
                            log.error('error during HTTP request to check object metadata prior to repair', {
                                bucket,
                                key,
                                url,
                                error: err && err.message,
                                statusCode: res.statusCode,
                            });
                            return reqDone(err);
                        }
                        return reqDone(null, res);
                    }),
                    urlDone,
                );
            },
            (err, [keyRes, masterKeyRes]) => {
                if (err) {
                    return next(err);
                }
                let repairMaster = false;
                if (masterKeyRes) {
                    if (masterKeyRes.statusCode === 404) {
                        repairMaster = true;
                    } else {
                        const parsedMasterKeyMd = JSON.parse(masterKeyRes.body);
                        repairMaster = (parsedMasterKeyMd.versionId === masterKeyInfo.versionId);
                    }
                }
                const refreshedLeaderMd = (keyRes.statusCode === 200 ? keyRes.body : null);
                const followerState = {
                    diffMd: diffEntry[0] && diffEntry[0].value,
                    isReadable: followerIsReadable,
                };
                const leaderState = {
                    diffMd: diffEntry[1] && diffEntry[1].value,
                    refreshedMd: refreshedLeaderMd,
                    isReadable: leaderIsReadable,
                };
                return next(null, followerState, leaderState, repairMaster);
            },
        ),
        (followerState, leaderState, repairMaster, next) => {
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
            return async.each(
                [bucketdUrl, repairMaster ? masterBucketdUrl : null],
                (url, urlDone) => {
                    if (!url) {
                        return urlDone();
                    }
                    return repairObjectMD(url, repairMd, (err, repairResult) => {
                        if (err) {
                            return urlDone(err);
                        }
                        if (repairResult.statusCode !== 200) {
                            log.error('HTTP request to repair object returned an error status', {
                                bucket,
                                key,
                                url,
                                statusCode: repairResult.statusCode,
                                statusMessage: repairResult.body,
                            });
                            return urlDone(errors.InternalError);
                        }
                        return urlDone();
                    });
                },
                err => {
                    if (err) {
                        countByStatus.AutoRepairError += 1;
                        return next(err);
                    }
                    return next();
                },
            );
        },
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
                countByStatus,
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
