const stream = require('stream');

const DiffStream = require('../../../CompareRaftMembers/DiffStream');

// parameterized by tests
let MOCK_BUCKET_STREAM_FULL_LISTING = null;

// populated by each test
let MOCK_BUCKET_STREAM_REQUESTS_MADE = null;

class MockBucketStream extends stream.Readable {
    constructor(params) {
        super({ objectMode: true });

        const { bucketdHost, bucketdPort, bucketName, marker, lastKey } = params;
        expect(bucketdHost).toEqual('dummy-host');
        expect(bucketdPort).toEqual(4242);

        MOCK_BUCKET_STREAM_REQUESTS_MADE.push({ bucketName, marker, lastKey });

        this.listingToSend = MOCK_BUCKET_STREAM_FULL_LISTING.filter(item => {
            const { key, value } = item;
            const slashIndex = key.indexOf('/');
            const [itemBucketName, objectKey] = [key.slice(0, slashIndex), key.slice(slashIndex + 1)];
            if (itemBucketName !== bucketName) {
                return false;
            }
            return (!marker || objectKey > marker)
                && (!lastKey || objectKey <= lastKey);
        });
    }

    _read() {
        process.nextTick(() => {
            if (this.listingToSend.length === 0) {
                this.push(null);
            } else {
                const item = this.listingToSend.shift();
                this.push(item);
            }
        });
    }
};

describe('DiffStream', () => {
    beforeEach(() => {
        MOCK_BUCKET_STREAM_REQUESTS_MADE = [];
    });
    describe('should compare a stream of { key, value } items by blocks', () => {
        [
            {
                desc: 'with no contents in db nor in bucketd',
                dbContents: [],
                bucketdContents: [],
                expectedOutput: [],
                expectedBucketStreamRequests: [],
            },
            {
                desc: 'with a single identical entry in db and bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                expectedOutput: [],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with a single entry with different key in db and bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key2', value: '{}' },
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                        null,
                    ],
                    [
                        null,
                        { key: 'bucket/key2', value: '{}' },
                    ],
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with a single entry with same key but different value in db and bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{"foo":"bar"}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{"foo":"qux"}' },
                ],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket/key1', value: '{"foo":"qux"}' },
                    ],
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with single entry in db and two entries in bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{}' },
                    { key: 'bucket/key2', value: '{"foo":"bar"}' },
                ],
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key2', value: '{"foo":"bar"}' },
                    ],
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with an empty db and a single entry in bucketd',
                dbContents: [],
                bucketdContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                // here the absolute difference is not empty but the
                // output is, because there is no input bucket from
                // the db, hence no request can be made to bucketd to
                // check for differences
                expectedOutput: [],
                expectedBucketStreamRequests: [],
            },
            {
                desc: 'with a single entry in db and an empty bucketd',
                dbContents: [
                    { key: 'bucket/key1', value: '{}' },
                ],
                bucketdContents: [],
                expectedOutput: [
                    [
                        { key: 'bucket/key1', value: '{}' },
                        null,
                    ],
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with two entries for two buckets in db and bucketd',
                dbContents: [
                    { key: 'bucket1/key1', value: '{}' },
                    { key: 'bucket2/key1', value: '{}' },
                ],
                bucketdContents: [
                    { key: 'bucket1/key1', value: '{}' },
                    { key: 'bucket2/key1', value: '{}' },
                ],
                expectedOutput: [],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket1',
                        marker: null,
                        lastKey: null,
                    },
                    {
                        bucketName: 'bucket2',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with two different entries for two buckets in db and bucketd',
                dbContents: [
                    { key: 'bucket1/key1', value: '{"foo":"bar"}' },
                    { key: 'bucket2/key1', value: '{"foo":"bar"}' },
                ],
                bucketdContents: [
                    { key: 'bucket1/key1', value: '{"foo":"qux"}' },
                    { key: 'bucket2/key1', value: '{"foo":"qux"}' },
                ],
                expectedOutput: [
                    [
                        { key: 'bucket1/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket1/key1', value: '{"foo":"qux"}' },
                    ],
                    [
                        { key: 'bucket2/key1', value: '{"foo":"bar"}' },
                        { key: 'bucket2/key1', value: '{"foo":"qux"}' },
                    ],
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket1',
                        marker: null,
                        lastKey: null,
                    },
                    {
                        bucketName: 'bucket2',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with 7777 identical entries in db and bucketd',
                dbContents: () => {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        dbList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return dbList;
                },
                bucketdContents: () => {
                    const bucketdList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        bucketdList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return bucketdList;
                },
                expectedOutput: [
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: 'key-001999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-001999',
                        lastKey: 'key-003999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-003999',
                        lastKey: 'key-005999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-005999',
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with 7777 entries only in db',
                dbContents: () => {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        dbList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return dbList;
                },
                bucketdContents: [],
                expectedOutput: () => {
                    const expectedDiff = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        expectedDiff.push([
                            { key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' },
                            null,
                        ]);
                    }
                    return expectedDiff;
                },
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: 'key-001999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-001999',
                        lastKey: 'key-003999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-003999',
                        lastKey: 'key-005999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-005999',
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with 7777 entries in db and bucketd with a few differences',
                dbContents: () => {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (i !== 2222) {
                            const paddedI = `000000${i}`.slice(-6);
                            let value;
                            if (i === 3333) {
                                value = '{"foo":"qux"}';
                            } else {
                                value = '{"foo":"bar"}';
                            }
                            dbList.push({ key: `bucket/key-${paddedI}`, value });
                        }
                    }
                    return dbList;
                },
                bucketdContents: () => {
                    const bucketdList = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (i !== 4444) {
                            const paddedI = `000000${i}`.slice(-6);
                            bucketdList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                        }
                    }
                    return bucketdList;
                },
                expectedOutput: [
                    [
                        null,
                        { key: 'bucket/key-002222', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key-003333', value: '{"foo":"qux"}' },
                        { key: 'bucket/key-003333', value: '{"foo":"bar"}' },
                    ],
                    [
                        { key: 'bucket/key-004444', value: '{"foo":"bar"}' },
                        null,
                    ],
                ],
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: 'key-001999',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-001999',
                        lastKey: 'key-004000',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-004000',
                        lastKey: 'key-006000',
                    },
                    {
                        bucketName: 'bucket',
                        marker: 'key-006000',
                        lastKey: null,
                    },
                ],
            },
            {
                desc: 'with two entries in db and 7777 entries in bucketd',
                dbContents: [
                    { key: 'bucket/key-001234', value: '{"foo":"bar"}' },
                    { key: 'bucket/key-006667', value: '{"foo":"bar"}' },
                ],
                bucketdContents: () => {
                    const dbList = [];
                    for (let i = 0; i < 7777; ++i) {
                        const paddedI = `000000${i}`.slice(-6);
                        dbList.push({ key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' });
                    }
                    return dbList;
                },
                expectedOutput: () => {
                    const expectedDiff = [];
                    for (let i = 0; i < 7777; ++i) {
                        if (![1234, 6667].includes(i)) {
                            const paddedI = `000000${i}`.slice(-6);
                            expectedDiff.push([
                                null,
                                { key: `bucket/key-${paddedI}`, value: '{"foo":"bar"}' },
                            ]);
                        }
                    }
                    return expectedDiff;
                },
                expectedBucketStreamRequests: [
                    {
                        bucketName: 'bucket',
                        marker: null,
                        lastKey: null,
                    },
                ],
            },
        ].forEach(testCase => {
            let expectedOutput;
            if (typeof testCase.expectedOutput === 'function') {
                expectedOutput = testCase.expectedOutput();
            } else {
                expectedOutput = testCase.expectedOutput;
            }
            test(`${testCase.desc} yielding ${expectedOutput.length} diff entries`, done => {
                if (typeof testCase.bucketdContents === 'function') {
                    MOCK_BUCKET_STREAM_FULL_LISTING = testCase.bucketdContents();
                } else {
                    MOCK_BUCKET_STREAM_FULL_LISTING = testCase.bucketdContents;
                }
                let dbContents;
                if (typeof testCase.dbContents === 'function') {
                    dbContents = testCase.dbContents();
                } else {
                    dbContents = testCase.dbContents;
                }
                const output = [];
                const diffStream = new DiffStream({
                    digestsDb: null,
                    bucketdHost: 'dummy-host',
                    bucketdPort: 4242,
                    maxBufferSize: 2000,
                    bucketStreamClass: MockBucketStream,
                });
                diffStream
                    .on('data', data => {
                        output.push(data);
                    })
                    .on('end', () => {
                        expect(output).toEqual(expectedOutput);
                        expect(MOCK_BUCKET_STREAM_REQUESTS_MADE)
                            .toEqual(testCase.expectedBucketStreamRequests);
                        done();
                    })
                    .on('error', done);
                dbContents.forEach(item => {
                    diffStream.write(item);
                });
                diffStream.end();
            });
        });
    });
});
