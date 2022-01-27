const fs = require('fs');
const werelogs = require('werelogs');

werelogs.configure({
    level: 'info',
    dump: 'error',
});

class PersistFileInterface {

    constructor() {
        this.folder = '/tmp';
        this.logger = new werelogs.Logger('PersistFileInterface');
        fs.access(this.folder, err => {
            if (err) {
                fs.mkdirSync(this.folder, { recursive: true });
            }
        });
    }

    getFileName(bucketName) {
        return `${this.folder}/${bucketName}.json`;
    }

    getOffsetFileName(bucketName) {
        return `${this.folder}/${bucketName}.offset.json`;
    }

    // cb(err, offset)
    load(bucketName, persistData, cb) {
        const fileName = this.getFileName(bucketName);
        const offsetFileName = this.getOffsetFileName(bucketName);
        let obj = {};
        fs.readFile(
            offsetFileName,
            'utf-8', (err, data) => {
                if (err) {
                    if (err.code === 'ENOENT') {
                        this.logger.info(`${offsetFileName} non-existent`);
                    } else {
                        this.logger.error('error loading', { err });
                        return cb(err);
                    }
                } else {
                    obj = JSON.parse(data);
                }
                if (fs.existsSync(fileName)) {
                    const file = fs.createReadStream(fileName);
                    persistData.loadState(file, err => {
                        if (err) {
                            return cb(err);
                        }
                        this.logger.info(`${fileName} loaded: offset ${obj.offset}`);
                        return cb(null, obj.offset);
                    });
                } else {
                    this.logger.info(`${fileName} non-existent`);
                    return cb(null, obj.offset);
                }
                return undefined;
            });
    }

    // cb(err)
    save(bucketName, persistData, offset, cb) {
        const fileName = this.getFileName(bucketName);
        const offsetFileName = this.getOffsetFileName(bucketName);
        const file = fs.createWriteStream(fileName);
        persistData.saveState(file, err => {
            if (err) {
                return cb(err);
            }
            const obj = {
                offset,
            };
            fs.writeFile(
                offsetFileName, JSON.stringify(obj),
                'utf-8',
                err => {
                    if (err) {
                        this.logger.error('error saving', { err });
                        return cb(err);
                    }
                    this.logger.info(`${fileName} saved: offset ${offset}`);
                    return cb();
                });
            return undefined;
        });
        return undefined;
    }
}

module.exports = PersistFileInterface;
