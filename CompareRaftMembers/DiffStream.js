const stream = require('stream');

const BlockDigestsStream = require('./BlockDigestsStream');
const BucketStream = require('./BucketStream');

class DiffStream extends stream.Transform {
    constructor(params) {
        super({ objectMode: true });
        const {
            digestsDb,
            bucketdHost, bucketdPort,
            maxBufferSize,
            bucketStreamClass,
        } = params;

        this.digestsDb = digestsDb;
        this.bucketdHost = bucketdHost;
        this.bucketdPort = bucketdPort;
        this.maxBufferSize = maxBufferSize || 1000;
        this.bucketStreamClass = bucketStreamClass || BucketStream;

        this.inputBuffer = [];
        this.currentBucket = null;
        this.marker = null;
    }

    _compareBuffers(inputBuffer, bucketBuffer) {
        //console.log('COMPARING BUFFERS', inputBuffer, 'AND', bucketBuffer);
        let [inputIndex, bucketIndex] = [0, 0];
        while (inputIndex < inputBuffer.length
               && bucketIndex < bucketBuffer.length) {
            const [inputItem, bucketItem] = [inputBuffer[inputIndex], bucketBuffer[bucketIndex]];
            if (inputItem.key < bucketItem.key) {
                this.push([inputItem, null]);
                inputIndex += 1;
            } else if (inputItem.key > bucketItem.key) {
                this.push([null, bucketItem]);
                bucketIndex += 1;
            } else {
                if (inputItem.value !== bucketItem.value) {
                    this.push([inputItem, bucketItem]);
                }
                inputIndex += 1;
                bucketIndex += 1;
            }
        }
        while (inputIndex < inputBuffer.length) {
            const inputItem = inputBuffer[inputIndex];
            this.push([inputItem, null]);
            inputIndex += 1;
        }
        while (bucketIndex < bucketBuffer.length) {
            const bucketItem = bucketBuffer[bucketIndex];
            this.push([null, bucketItem]);
            bucketIndex += 1;
        }
    }

    _checkInputBuffer(bucketName, marker, lastKey, cb) {
        console.log(`CHECK INPUT BUFFER bucketName=${bucketName} marker=${marker} lastKey=${lastKey}`);
        const bucketStream = new this.bucketStreamClass({
            bucketdHost: this.bucketdHost,
            bucketdPort: this.bucketdPort,
            bucketName,
            marker,
            lastKey,
        });
        let bucketBuffer = [];
        bucketStream
            .on('data', item => {
                bucketBuffer.push(item);
                // split the comparison job in chunks if the bucket
                // buffer gets too big: for this we may also need to
                // split the input buffer so that the comparison is
                // meaningful
                if (bucketBuffer.length === this.maxBufferSize) {
                    const maxKey = bucketBuffer[bucketBuffer.length - 1].key;
                    const inputBufferLimit = this.inputBuffer.findIndex(item => item.key > maxKey);
                    let inputBuffer;
                    if (inputBufferLimit !== -1) {
                        inputBuffer = this.inputBuffer.splice(0, inputBufferLimit);
                    } else {
                        inputBuffer = this.inputBuffer;
                        this.inputBuffer = [];
                    }
                    this._compareBuffers(inputBuffer, bucketBuffer);
                    bucketBuffer = [];
                }
            })
            .on('end', () => {
                const inputBuffer = this.inputBuffer;
                this.inputBuffer = [];
                this._compareBuffers(inputBuffer, bucketBuffer);
                cb();
            })
            .on('error', err => {
                // unrecoverable error after retries: raise the error
                this.emit('error', err);
            });
    }

    _transform(item, encoding, callback) {
        //console.log('TRANSFORM', item);
        const { key, value } = item;
        const slashIndex = key.indexOf('/');
        const [bucketName, objectKey] = [key.slice(0, slashIndex), key.slice(slashIndex + 1)];
        const isNewBucket = (this.currentBucket && bucketName !== this.currentBucket);
        if (isNewBucket || this.inputBuffer.length === this.maxBufferSize) {
            let lastKey = null;
            if (!isNewBucket) {
                const lastBufferedKey = this.inputBuffer[this.inputBuffer.length - 1].key;
                // since the new key belongs to the same bucket than
                // the previous key, the new key's slash index is
                // valid for the previous key
                lastKey = lastBufferedKey.slice(slashIndex + 1);
            }
            this._checkInputBuffer(this.currentBucket, this.marker, lastKey, () => {
                this.currentBucket = bucketName;
                this.marker = lastKey;
                this.inputBuffer.push(item);
                callback();
            });
        } else {
            this.currentBucket = bucketName;
            this.inputBuffer.push(item);
            callback();
        }
    }

    _flush(callback) {
        //console.log('FLUSH', this.inputBuffer);
        if (this.inputBuffer.length > 0) {
            this._checkInputBuffer(this.currentBucket, this.marker, null, callback);
        } else {
            callback();
        }
    }
}

module.exports = DiffStream;
