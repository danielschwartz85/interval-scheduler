var path = require('path');
var fs = require('fs');
var logrotate = require('logrotator');

LEVELS = {
    DEBUG: 1,
    INFO: 2,
    WARNING: 3,
    WARN: 3,
    ERROR: 4,
    FATAL: 5,
    AUDIT: 100
};

class Logger {

    constructor(config = {}) {
        this.rotate = config['rotate'] || { schedule: '1m', size: '10m', compress: true, count: 5 };
        this.level = LEVELS[(config['level'] || 'debug').toUpperCase()];
        this.isMaster = !!config['isMaster'];
        this.stream = process.stdout;
        if (config['file']) {
            let file = path.resolve(config['file']);
            this.stream = fs.createWriteStream(file, {flags: 'a'});
            this.rotator = logrotate.create();
            // check rotation every 1 minute, size of 10MB, gzip, keep 5 rotated files
            this.rotator.register(file, this.rotate);
            this.rotator.on('rotate', file => {
                this.stream.close();
                this.stream = fs.createWriteStream(file, {flags: 'a'});
            });
            this.rotator.on('error', error => {
                console.log(`ERROR: log rotation failed for ${file}: ${error.message || error}`);
            });
        }
    }

    log(level, msg) {
        if (LEVELS[level] < this.level) {
            return;
        }
        if (!this.isMaster) {
            process.send({ type: 'log', msg: `${new Date().toISOString()} ${level} ${msg}\n` });
        } else {
            this.write(`${new Date().toISOString()} ${level} ${msg}\n`);
        }
    }

    write(msg) {
        this.stream.write(msg);
    }
}

class TaggedLogger {
    constructor(tagOrTags) {
        if (!exports._logger) {
            throw new Error(`logger ${tagOrTags} was created while Logger was not initialized`);
        }

        this.tags = typeof tagOrTags === 'string' ? [tagOrTags] : tagOrTags;
        this.logger = exports._logger;
        let tags = this.tags.reduce((t1, t2) => `${t1}[${t2}]`,'');
        Object.keys(LEVELS).forEach((level) => {
            this[level.toLowerCase()] = (msg) => {
                this.logger.log(level, `${tags} ${msg}`);
            };
        });
    }
}


exports.init = (config) => {
    exports._logger = new Logger(config);
};

exports.tagged = (tag) => {
    return new TaggedLogger(tag);
};

exports.write = (msg) => {
    exports._logger.write(msg);
};

