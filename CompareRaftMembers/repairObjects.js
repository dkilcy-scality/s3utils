const async = require('async');
const http = require('http');
const jsonStream = require('JSONStream');

const { Logger } = require('werelogs');
const { jsutil, errors } = require('arsenal');

const getBucketdURL = require('../VerifyBucketSproxydKeys/getBucketdURL');

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

function repairObject(bucketdUrl, mdBlob, cb) {
    httpRequest('POST', bucketdUrl, mdBlob, cb);
}

function checkDiffEntry(diffEntry, cb) {
    if (!diffEntry[0]) {
        const { bucket, key } = parseDiffKey(diffEntry[1].key);
        const bucketdUrl = getBucketdURL(BUCKETD_HOSTPORT, {
            Bucket: bucket,
            Key: key,
        });
        const md = JSON.parse(diffEntry[1].value);
        checkSproxydKeys(bucketdUrl, md.location, err => {
            if (err) {
                log.warn('object is not repairable: absent from follower\'s view '
                         + 'and not readable from leader\'s view',
                         { bucket, key });
                return cb();
            }
            // if there was no error, the object is not corrupted so
            // we can safely repair its metadata
            return repairObject(bucket, key, md, err => {
                if (err) {
                    log.error('an error occurred trying to repair object using '
                              + 'leader\'s view',
                              { bucket, key, error: err.message });
                } else {
                    log.info('object absent from follower repaired using leader\'s view',
                             { bucket, key });
                }
                cb();
            });
        });
    } else if (!diffEntry[1]) {
        const { bucket, key } = parseDiffKey(diffEntry[0].key);
        const bucketdUrl = getBucketdURL(BUCKETD_HOSTPORT, {
            Bucket: bucket,
            Key: key,
        });
        const md = JSON.parse(diffEntry[0].value);
        checkSproxydKeys(bucketdUrl, md.location, err => {
            if (err) {
                log.warn('object is not repairable: absent from leader\'s view '
                         + 'and not readable from follower\'s view',
                         { bucket, key });
                return cb();
            }
            // if there was no error, the object is not corrupted so
            // we can safely repair its metadata
            return repairObject(bucket, key, md, err => {
                if (err) {
                    log.error('an error occurred trying to repair object using '
                              + 'follower\'s view',
                              { bucket, key, error: err.message });
                } else {
                    log.info('object absent from leader repaired using follower\'s view',
                             { bucket, key });
                }
                cb();
            });
        });
    } else {
        const { bucket, key } = parseDiffKey(diffEntry[0].key);
        const bucketdUrl = getBucketdURL(BUCKETD_HOSTPORT, {
            Bucket: bucket,
            Key: key,
        });
        async.map(diffEntry, (diffItem, itemDone) => {
            const md = JSON.parse(diffItem.value);
            checkSproxydKeys(bucketdUrl, md.location, err => itemDone(null, err ? false : true));
        }, (_, diffItemIsValid) => {
            if (diffItemIsValid[0] && diffItemIsValid[1]) {
                log.warn('object requires a manual check: is readable from both '
                         + 'leader\'s view and follower\'s view but metadata is different',
                         { bucket, key });
                cb();
            } else if (diffItemIsValid[0]) {
                const validMd = diffEntry[0].value;
                return repairObject(bucketdUrl, validMd, err => {
                    if (err) {
                        log.error('an error occurred trying to repair object using '
                                  + 'follower\'s view',
                                  { bucket, key, error: err.message });
                    } else {
                        log.info('object not readable on leader repaired using follower\'s view',
                                 { bucket, key });
                    }
                    cb();
                });
            } else if (diffItemIsValid[1]) {
                const validMd = diffEntry[1].value;
                return repairObject(bucketdUrl, validMd, err => {
                    if (err) {
                        log.error('an error occurred trying to repair object from '
                                  + 'leader\'s view',
                                  { bucket, key, error: err.message });
                    } else {
                        log.info('object not readable on follower repaired using leader\'s view',
                                 { bucket, key });
                    }
                    cb();
                });
            } else {
                log.warn('object is not repairable: not readable from neither '
                         + 'leader\'s view nor follower\'s view',
                         { bucket, key });
                cb();
            }
        });
    }
}

function consumeStdin(cb) {
    const diffEntryStream = process.stdin.pipe(jsonStream.parse());
    diffEntryStream
        .on('data', diffEntry => {
            diffEntryStream.pause();
            return checkDiffEntry(diffEntry, () => {
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
            log.info('completed repair');
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
