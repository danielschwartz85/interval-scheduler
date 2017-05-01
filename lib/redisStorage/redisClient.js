const EventEmitter = require('events').EventEmitter;
const Utils = require ('../utils');
const crypto = require('crypto');
const redis = require('redis');
const bluebird = require('bluebird');
const fs = bluebird.promisifyAll(require("fs"));
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

class Redis extends EventEmitter {

    constructor(config, taskIntervalSeconds, logger = undefined) {
        Utils.assertKeys(config, 'port', 'host', 'db');
        super();
        this.config = JSON.parse(JSON.stringify(config));
        this.name = `${config['host']}:${config['port']}`;
        this.logger = Utils.tryLogger(logger, `redis ${this.name}`);
        this._settings = {
            scheduleLock : {
                randLength : 12,
                autoReleaseSeconds : 24 * 60 * 60
            },
            indexQueueName : 'mainIndex',
            scriptNames : ['unlock', 'existsupdate', 'existsdisable', 'pulltask'],
            minTaskIntervalSeconds : Utils.isLocal() ? 1 : 10,
            maxTaskIntervalSeconds : 60 * 60,
            scanBatchSize : 1000
        };
        this._taskIntervalSeconds = taskIntervalSeconds || 60;
        this._validate();
    }

    connect() {
        if (this.connected) {
            return Promise.resolve();
        }

        this._setupClient();
        let connect = new Promise((res, rej) => {
            this._client.once('ready', res).once('error', rej);
        }).then(() => {
            return this._loadScripts();
        }).then(() => {
            return Promise.resolve();
        });
        return connect;
    }

    settings(path) {
        return Utils.objGet(this._settings, path);
    }

    schedule(taskId, data, startOn, intervalSeconds = undefined) {
        let startOnQueue = this._roundToTaskInterval(startOn);
        let taskInfo = ['data', data, 'canceled', false];
        if (intervalSeconds) {
            taskInfo = taskInfo.concat(['interval', this._roundToTaskInterval(intervalSeconds * 1000)]);
        }
        let schedule = this._client.multi()
            .lpush(startOnQueue, this._taskName(taskId))
            .zadd(this.settings('indexQueueName'), startOnQueue, startOnQueue)
            .hmset(this._taskName(taskId), ...taskInfo)
            .execAsync()
            .then(replies => {
                return Promise.resolve(startOnQueue);
            });
        return schedule;
    }

    isScheduledCancel(taskId) {
        return this._lua('EXISTSDISABLE', this._taskName(taskId)).then(canceled => {
           return Promise.resolve(canceled === 1);
        });
    }

    isScheduledUpdate(taskId, data, intervalSeconds = undefined) {
        let interval = this._roundToTaskInterval(intervalSeconds * 1000);
        return this._lua('EXISTSUPDATE', this._taskName(taskId), data, interval);
    }

    pullTask() {
        let now = this._roundToTaskInterval(Date.now());
        let pull = this._lua('PULLTASK', this.settings('indexQueueName'), now).then(data => {
          return Promise.resolve(data || null);
        });
        return pull;
    }

    tryLock(lockId) {
        let rand = crypto.randomBytes(this.settings('scheduleLock/randLength')).toString('hex');
        let autoReleaseSeconds = this.settings('scheduleLock/autoReleaseSeconds');
        let lockName = this._lockName(lockId);
        let scheduleLock = this._client.setAsync(lockName, rand, 'NX', 'EX', autoReleaseSeconds).then(locked => {
            return Promise.resolve(locked === 'OK' ? {key: lockName, lockerId: rand} : false);
        });
        return scheduleLock;
    }

    unlock(lock) {
        let unlock = this._lua('UNLOCK', lock['key'], lock['lockerId']).then(res => {
            return Promise.resolve(1 === res);
        });
        return unlock;
    }

    tryAutoLock(lockId, autoReleaseSeconds) {
        let lockName = this._autoLockName(lockId);
        let scheduleLock = this._client.setAsync(lockName, true, 'NX', 'EX', autoReleaseSeconds).then(locked => {
            return Promise.resolve(locked === 'OK');
        });
        return scheduleLock;
    }

    clearAllTasks() {
        let scanBatchSize = this.settings('scanBatchSize');
        let totalDeleted = 0;
        let clearKeys = (cursor) => {
            this.logger.debug(`searching tasks to delete cursor ${cursor}`);
            let scan = this._client.scanAsync(cursor, 'MATCH', this._taskName('*'), 'COUNT', scanBatchSize)
                .then(res => {
                    let deleteTasks = res[1].length ? this._client.delAsync(...res[1]) : Promise.resolve(0);
                    return Promise.all([res[0], deleteTasks]);
                }).then(res => {
                    totalDeleted = totalDeleted + res[1];
                    if ('0' !== res[0]) {
                        return clearKeys(res[0]);
                    } else {
                        return Promise.resolve(totalDeleted);
                    }
                });
            return scan;
        };
        return clearKeys(0);
    }

    peekTask(taskId) {
        let getTask = this._client.hmgetAsync(this._taskName(taskId), 'data', 'canceled').then(res => {
            return Promise.resolve('true' !== res[1] ? res[0] : null);
        });
        return getTask;
    }

    queueSize() {
        return this._client.zcountAsync(this.settings('indexQueueName'), '-inf', '+inf')
    }

    get connected() {
        return this._client && this._client.connected;
    }

    get id() {
        return this.name;
    }

    // For debugging
    _peekTaskAllInfo(taskId) {
        return this._client.hgetallAsync(this._taskName(taskId));
    }

    _setupClient() {
        if (this._client) return;

        this._client = redis.createClient(this.config['port'], this.config['host'], { db: this.config['db'] });
        let onReady = (e) => { this._onReady(e) };
        let onError = (e) => { this._onError(e) };
        let onWarning = (e) => { this._onWarning(e) };
        let shutdown = (e) => {
            this._client.quit();
            this._client.removeListener('error', onError);
            this._client.removeListener('ready', onReady);
            this._client.removeListener('warning', onWarning);
            this._client.removeListener('end', shutdown);
            delete this._client;
        };
        this._client.on('error', onError).on('warning', onWarning).on('ready', onReady);
        this._client.once('end', shutdown);
    }

    _onReady(e) {
        this._client.once('end', () => this.emit('disconnect', this));
        this.emit('connect', this);
    }

    _onWarning(e) {
        this.logger.warn(e);
    }

    _onError(e) {
        this.logger.warn(e);
    }

    _loadScripts() {
        let scriptNames = this.settings('scriptNames');
        let loadScripts = Promise.all(scriptNames.map(scriptName => {
            let loadFile = fs.readFileAsync(`${__dirname}/lua/${scriptName}.lua`).then(content => {
                return Promise.resolve({ content: content.toString().trim(), name: scriptName });
            });
            return loadFile;
        })).then(scripts => {
            let result = {};
            scripts.forEach(script => {
                result[script['name'].toUpperCase()] = {
                    content: script['content'],
                    keys: parseInt((script['content'].match(/-- KEYS/g) || []).length)
                };
            });
            this._scripts = result;
            return Promise.resolve(this._scripts);
        }).then(scripts => {
            let getShas = Promise.all(Object.keys(scripts).map(scriptName => {
                return this._client.scriptAsync('load', scripts[scriptName]['content']).then(res => {
                    scripts[scriptName].sha = res;
                });
            }));
            return getShas;
        });

        return loadScripts;
    }

    _lua(name, ...args) {
        let lua = this._eval(name, ...args).then((...res) => {
          return [res];
        }).catch(err => {
            if (err && err.message.includes('NOSCRIPT')) {
                return Promise.all([null, this._loadScripts()]);
            } else {
                return Promise.reject(err);
            }
        }).then(results => {
            if (results[0]) {
                return Promise.resolve(...(results[0]));
            } else {
                return this._eval(name, ...args);
            }
        });
        return lua;
    }

    _eval(name, ...args) {
        return this._client.evalshaAsync(this._scripts[name]['sha'], this._scripts[name]['keys'], ...args);
    }

    _roundToTaskInterval(milliseconds) {
        let interval = this._taskIntervalSeconds;
        return Math.floor(milliseconds / (interval * 1000)) * interval * 1000;
    }

    _lockName(lockId) {
        return `lk:${lockId}`;
    }

    _autoLockName(lockId) {
        return `alk:${lockId}`;
    }

    _taskName(taskId) {
        return `tsk:${taskId && taskId || ''}`;
    }

    _validate() {
        if (!Number.isInteger(this._taskIntervalSeconds)) {
            throw new Error(`invalid taskIntervalSeconds: ${this._taskIntervalSeconds}`);
        }
        let min = this.settings('minTaskIntervalSeconds');
        let max = this.settings('maxTaskIntervalSeconds');
        if (this._taskIntervalSeconds < min || this._taskIntervalSeconds > max) {
            throw new Error(`taskIntervalSeconds is out of range: ${this._taskIntervalSeconds}`);
        }
    }

}

module.exports = Redis;