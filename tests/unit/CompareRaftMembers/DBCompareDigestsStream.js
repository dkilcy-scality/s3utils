const assert = require('assert');
const stream = require('stream');

const DBCompareDigestsStream = require('../../../CompareRaftMembers/DBCompareDigestsStream');

class MockDigestsReadStream extends stream.Readable {
    constructor(digests) {
        super({ objectMode: true });
        this.digests = digests;
        this.readIndex = 0;
    }

    _read() {
        setTimeout(() => {
            if (this.readIndex === this.digests.length) {
                this.push(null);
            } else {
                this.push(this.digests[this.readIndex]);
                this.readIndex += 1;
            }
        }, Math.random() * 10);
    }
}

class MockDigestsDB {
    constructor(digests) {
        this.digests = digests;
    }

    createReadStream(params) {
        assert.strictEqual(typeof params, 'object');
        assert.strictEqual(typeof params.gte, 'string');
        const startIndex = this.digests.findIndex(digest => digest.key >= params.gte);
        const digestsToStream = startIndex === -1
              ? [] : this.digests.slice(startIndex);
            
        return new MockDigestsReadStream(digestsToStream);
    }
}

class MockListing extends stream.Readable {
    constructor(listEntries) {
        super({ objectMode: true });
        this.listEntries = listEntries;
        this.readIndex = 0;
    }

    _read() {
        setTimeout(() => {
            if (this.readIndex === this.listEntries.length) {
                this.push(null);
            } else {
                this.push(this.listEntries[this.readIndex]);
                this.readIndex += 1;
            }
        }, Math.random() * 10);
    }
}

describe('DBCompareDigestsStream', () => {
    [
        {
            desc: '6 listed entries in a single bucket',
            listing: [
                { key: 'bucket/key-b', value: '{}' },
                { key: 'bucket/key-c', value: '{}' },
                { key: 'bucket/key-d', value: '{}' },
                { key: 'bucket/key-e', value: '{}' },
                { key: 'bucket/key-f', value: '{}' },
                { key: 'bucket/key-g', value: '{}' },
            ],
            testCases: [
                {
                    desc: 'no digests => unbounded range',
                    digests: [],
                    result: [],
                },
                {
                    desc: 'single digest with lastKey lower than first listed'
                        + ' => unbounded range',
                    digests: [
                        {
                            key: 'bucket/key-a',
                            value: '{"size":1,"digest":"mismatch"}',
                        },
                    ],
                    result: [],
                },
                {
                    desc: 'single digest with lastKey equal first listed and digest match'
                        + ' => unbounded range',
                    digests: [
                        {
                            key: 'bucket/key-b',
                            value: '{"size":1,"digest":"2d022275c6db18175e988b859f00c77f"}',
                        },
                    ],
                    result: [{ lastKey: 'bucket/key-b' }],
                },
                {
                    desc: 'single digest with lastKey equal first listed and digest mismatch'
                        + ' => unbounded range',
                    digests: [
                        {
                            key: 'bucket/key-b',
                            value: '{"size":1,"digest":"mismatch"}',
                        },
                    ],
                    result: [],
                },
                {
                    desc: 'single digest with lastKey equal last listed and digest mismatch'
                        + ' => unbounded range',
                    digests: [
                        {
                            key: 'bucket/key-g',
                            value: '{"size":6,"digest":"mismatch"}',
                        },
                    ],
                    result: [],
                },
                {
                    desc: 'single digest with lastKey in listed range and digest mismatch'
                        + ' => unbounded range',
                    digests: [
                        {
                            key: 'bucket/key-d2',
                            value: '{"size":3,"digest":"mismatch"}',
                        },
                    ],
                    result: [],
                },
                {
                    desc: 'single digest with lastKey higher than last listed and digest mismatch'
                        + ' => unbounded range',
                    digests: [
                        {
                            key: 'bucket/key-h',
                            value: '{"size":7,"digest":"mismatch"}',
                        },
                    ],
                    result: [],
                },
            ],
        },
    ].forEach(testCaseGroup => {
        testCaseGroup.testCases.forEach(testCase => {
            test(`${testCaseGroup.desc}, ${testCase.desc}`, done => {
                const listStream = new MockListing(testCaseGroup.listing);
                const digestsDb = new MockDigestsDB(testCase.digests);
                const compareStream = new DBCompareDigestsStream(digestsDb);
                listStream.pipe(compareStream);
                const result = [];
                compareStream
                    .on('data', data => {
                        console.log('COMPARE DATA', data);
                        result.push(data);
                    })
                    .on('end', () => {
                        console.log('COMPARE END');
                        expect(result).toEqual(testCase.result);
                        done();
                    })
                    .on('error', err => {
                        console.log('COMPARE ERROR', err);
                        fail(`an error occurred: ${err}`);
                    });
            });
        });
    });
});
