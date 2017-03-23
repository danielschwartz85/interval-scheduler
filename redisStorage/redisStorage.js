const Storage = require('../components/storage').Storage;
const Redis = require('./redisClient').Redis;
const Common = require ('./common');
const Promise = require("bluebird");

class RedisStorage extends Storage {

    constructor(config) {
        Common.assertKeys(config, 'instances', 'masterIndex');
        Common.assertKeys(config, 'reconnectIntervalSeconds', 'taskIntervalSeconds');
        super();
        this.config = config;

        let instancesConfig = JSON.parse(JSON.stringify(config['instances']));
        let masterConfig = instancesConfig.splice(config['masterIndex'], 1)[0];
        this._mainRedisClient = new Redis(masterConfig, this.config['taskIntervalSeconds']);
        this._redisClients = instancesConfig.map(inst => {
            return new Redis(inst, this.config['taskIntervalSeconds']);
        });
        this._storageSettings = {
            retryScheduleLockMilliseconds : 300
        };
        this._listeningToClients = false;
        this._validate();
    }

    connect() {
        this.logger.info('connecting..');
        this._setupListeners();
        let connect = Promise.all(this._clients.map(r =>  {
            return r.connected ? Promise.resolve() : r.connect();
        })).then(() => {
          this._online = true;
        });
        return connect;
    }

    assignTask(taskData, intervalSeconds = undefined) {
        let lock;
        let schedule = this._assertConnected().then(() => {
            return this._lockSchedule(taskData['id']);
        }).then(scheduleLock => {
            lock = scheduleLock;
            return this._isScheduledUpdate(taskData['id'], taskData['data'], intervalSeconds);
        }).then(isScheduled => {
            let res;
            if (!isScheduled) {
                res = this._schedule(taskData['id'], taskData['data'], taskData['startOn'], intervalSeconds);
            } else {
                res = Promise.resolve(false);
            }
            return res;
        }).catch(err => {
            lock && this._unlockSchedule(lock);
            return Promise.reject(err);
        }).then(didSchedule => {
            return Promise.all([didSchedule, this._unlockSchedule(lock)]);
        }).then(results => {
            lock = null;
            return results[0];
        }).catch(err => {
            this.logger.error(`failed to assign task err: ${err}`);
            return Promise.reject(err);
        });

        return schedule;
    }

    removeTask(taskId) {
        let remove = this._assertConnected().then(() => {
            return this._isScheduledCancel(taskId);
        }).then(scheduled => {
            let res;
            if (!scheduled) {
                res = Promise.resolve();
            } else {
                res = Promise.reject(new Error(`could not remove task: ${taskId}`));
            }
            return res;
        });
        return remove;
    }

    pullTask() {
        return this._someClient('pullTask');
    }

    tryLock(lockBy) {
        return this._mainRedisClient.tryLock(lockBy);
    }

    unlock(lock) {
        return this._mainRedisClient.unlock(lock);
    }

    tryAutoLock(lockId, autoReleaseSeconds) {
        return this._mainRedisClient.tryAutoLock(lockId, autoReleaseSeconds);
    }

    clearAllTasks() {
        let clearAll = Promise.all(this._onlineClients.map(c => c.clearAllTasks())).then(res => {
            return Promise.resolve(res.reduce((a, b) => a + b, 0));
        });
        return clearAll;
    }

    get connected() {
        return !!this._online;
    }

    get errors() {
        return this._clients.filter(r => !r.connected).map(r => `${r.name} is not connected`);
    }

    _settings(path) {
        return Common.objGet(this._storageSettings, path);
    }

    // Check if the task exists in any of the clients, if yes then cancel it.
    _isScheduledCancel(taskId) {
        return this._someClient('isScheduledCancel', taskId).then(res => !!res);
    }

    // Check if the task exists in any of the clients, if yes then update it and un cancel if needed.
    _isScheduledUpdate(taskId, data, intervalSeconds = undefined) {
        return this._someClient('isScheduledUpdate', taskId, data, intervalSeconds).then(res => !!res);
    }

    _lockSchedule(taskId) {
        let autoReleaseSeconds = this._mainRedisClient.settings('scheduleLock/autoReleaseSeconds');
        let endTime = Date.now() + ((autoReleaseSeconds + 1) * 1000);
        let _scheduleTryLock = () => {
            let tryLock = this.tryLock(taskId).then(lock => {
                if (lock) {
                    return Promise.resolve(lock);
                } else if (endTime - Date.now() > 0) {
                    return Promise.delay(this._settings('retryScheduleLockMilliseconds'));
                } else {
                    return Promise.reject(new Error('could not obtain schedule lock'));
                }
            }).then(lock => {
                if (lock) {
                    return Promise.resolve(lock);
                } else {
                    return _scheduleTryLock();
                }
            });
            return tryLock;
        };

        return _scheduleTryLock();
    }

    _unlockSchedule(lock) {
        return this.unlock(lock).then(unlocked => {
            if(!unlocked) {
                // Either timeout passed and lock does not exist or lock was acquired by other
                this.logger.error(`tried unlocking a free lock timeout must have passed`);
            }
            return Promise.resolve();
        });
    }

    _schedule(id, data, startOn, intervalSeconds = undefined) {
        let clients = this._onlineClients;
        let selectedRedis = clients[0];
        return selectedRedis.schedule(id, data, startOn, intervalSeconds);
    }

    _setupListeners() {
        if (this._listeningToClients) {
            return;
        }
        this._clients.forEach(redis => {
            redis.once('disconnect', this._onDisconnect.bind(this));
        });
        this._listeningToClients = true;
    }

    _onDisconnect(redis) {
        this.logger.error(`${redis.name} disconnected`);
        if (this._isMaster(redis)) {
            this._markAsOffline();
        }
        this._reconnect(redis);
    }

    _reconnect(redis) {
        let reconnect = redis.connect().then(() => {
            this.logger.info(`${redis.name} back online`);
            if (this._isMaster(redis)) {
                this._markAsOnline();
            }
            return Promise.resolve(true);
        }).catch(e => {
            return Promise.delay(this.config['reconnectIntervalSeconds'] * 1000);
        }).then(connected => {
            if (!connected) {
                return this._reconnect(redis);
            } else {
                return Promise.resolve();
            }
        });

        return reconnect;
    }

    _isMaster(redis) {
        return this._mainRedisClient.id == redis.id;
    }

    _markAsOnline() {
        this._online = true;
        this.emit('reconnect');
    }

    _markAsOffline() {
        this._online = false;
        this.emit('disconnect');
    }

    _assertConnected() {
        if (!this.connected) {
            return Promise.reject(new Error('redis storage is not connected'));
        } else {
           return Promise.resolve();
        }
    }

    // Call 'method' on all online clients, until one resolves with a result.
    _someClient(method, ...args) {
        let some = new Promise((res, rej) => {
            var clientResult;
            this._assertConnected().then(() => {
                let all = Promise.all(this._onlineClients.map(redis => {
                    return redis[method](...args).then((...result) => {
                        if (!!result[0]) {
                            clientResult = result[0];
                            res(clientResult);
                        }
                    });
                }));
                return all;
            }).then(() => {
                res(clientResult);
            }).catch(rej);
        });
        return some;
    }

    _validate() {
        if (!Number.isInteger(this.config['reconnectIntervalSeconds'])) {
            throw new Error(`invalid reconnectIntervalSeconds: ${this.config['reconnectIntervalSeconds']}`);
        }
        if (this.config['reconnectIntervalSeconds'] < 1 || this.config['reconnectIntervalSeconds'] > (60 * 60)) {
            throw new Error(`reconnectIntervalSeconds is out of range: ${this.config['reconnectIntervalSeconds']}`);
        }
    }

    get _clients() {
        this._allClients = this._allClients || [this._mainRedisClient].concat(this._redisClients);
        return this._allClients;
    }

    get _onlineClients() {
        return Common.shuffle(this._clients.filter(redis => redis.connected));
    }

}

exports.RedisStorage = RedisStorage;