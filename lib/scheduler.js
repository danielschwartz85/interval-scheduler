const EventEmitter = require('events').EventEmitter;
const Storage = require('./redisStorage/redisStorage');
const Utils = require ('./utils');
const Promise = require('bluebird');

/*
 * Emits 'offline' and 'online' general events and db specific events
 * 'db-disconnect' and 'db-reconnect' with a db information message.
 */
class Scheduler extends EventEmitter {

    constructor(config = {}, logger = undefined) {
        super();
        config['checkTasksEverySeconds'] = config['checkTasksEverySeconds'] || 10;
        this.config = config;
        this.logger = Utils.tryLogger(logger, 'scheduler');
        this._schedulerSettings = {
            retryLockMilliseconds : 300,
            minCheckTasksEverySeconds : Utils.isLocal() ? 1 : 10,
            maxCheckTasksEverySeconds : 60 * 60
        };
        this._storage = new Storage(config['storage'], logger);
        this.listeningToStorage = false;
        this._validate();
    }

    connect() {
        return this._setupStorage();
    }

    /*
     * executor - executor function accepts serialized task and resolves
     */
    startTaskExecute(executor) {
        if (this.executingTasks) {
            return Promise.reject(new Error('already executing tasks'));
        }
        this._execute = executor;

        let startExecute;
        let taskPull = () => {
            this._executingTasks = true;
            this._startTaskPull();
            return Promise.resolve();
        };
        if (!this._storage.connected) {
            startExecute = this._setupStorage().then(taskPull);
        } else {
            startExecute = taskPull();
        }

        this.logger.debug('starting task execute..');
        return startExecute;
    }

    stopTaskExecute() {
        this.logger.debug('stopping task execution..');
        this._executingTasks = false;
        return Promise.resolve();
    }

    assignTask(task) {
        let taskData = {
            id: task.id,
            data: task.serialize(),
            startOn: task.executeOn
        };
        return this._storage.assignTask(taskData, task.onExecuteRescheduleTo);
    }

    removeTask(taskId) {
        return this._storage.removeTask(taskId);
    }

    /*
     * acquire and return lock (auto released after 24 hours) should be unlocked
     */
    tryLock(lockBy) {
        return this._storage.tryLock(lockBy);
    }

    unlock(lock) {
        return this._storage.unlock(lock);
    }

    /*
     * acquire auto released lock (faster with RedisStorage)
     */
    tryAutoLock(lockBy, autoReleaseSeconds) {
        return this._storage.tryAutoLock(lockBy, autoReleaseSeconds);
    }

    /*
     * remove all tasks (on moment of execution) and return number of cleared tasks
     */
    clearAllTasks() {
        return this._storage.clearAllTasks();
    }

    /*
     * return task serialized data
     */
    isScheduled(taskId) {
        let peek = this._storage.peekTask(taskId).then(data => {
            return Promise.resolve(data || false);
        });
        return peek;
    }

    get online() {
        return this._storage.online;
    }

    get executingTasks() {
        return this._executingTasks;
    }

    _settings(path) {
        return Utils.objGet(this._schedulerSettings, path);
    }

    _setupStorage() {
        if (!this.listeningToStorage) {
            this._storage.on('offline', this._onStorageOffline.bind(this));
            this._storage.on('online', this._onStorageOnline.bind(this));
            this._storage.on('disconnect', this._onStorageDisconnect.bind(this));
            this._storage.on('reconnect', this._onStorageReconnect.bind(this));
            this.listeningToStorage = true;
        }
        return this._storage.connect();
    }

    _onStorageOnline() {
        this.emit('online');
    }

    _onStorageOffline() {
        this.emit('offline');
    }

    _onStorageDisconnect(db) {
        this.emit(`db-disconnect`, db);
    };

    _onStorageReconnect(db) {
        this.emit(`db-reconnect`, db);
    }

    _startTaskPull() {
        let delay = (this.config['checkTasksEverySeconds'] || 10) * 1000;
        let taskPull = () => {
            if (!this.executingTasks) {
                return null;
            }
            this._pullAndExecute().then(tasksPending => {
                if (tasksPending) {
                    this.logger.debug('pulling more tasks..');
                    return Promise.resolve();
                } else {
                    this.logger.debug('waiting for tasks..');
                    return Utils.delay(delay);
                }
            }).then(taskPull).catch(e => {
                this.logger.fatal(`failed to pull tasks err: ${e.message}`);
                Utils.delay(delay).then(taskPull);
            });
            return null;
        };
        taskPull();
    }

    _pullAndExecute() {
        let pullAndExecute = this._storage.pullTask().then(data => {
            if (data) {
                this._execute(data).catch(err => {
                    this.logger.error(`got error: ${err} during execute for task: ${data}`);
                });
            }
            return Promise.resolve(!!data);
        });
        return pullAndExecute;
    }

    _validate() {
        if (!Number.isInteger(this.config['checkTasksEverySeconds'])) {
            throw new Error(`invalid checkTasksEverySeconds: ${this.config['checkTasksEverySeconds']}`);
        }
        let min = this._settings('minCheckTasksEverySeconds');
        let max = this._settings('maxCheckTasksEverySeconds');
        if (this.config['checkTasksEverySeconds'] < min || this.config['checkTasksEverySeconds'] > max) {
            throw new Error(`taskIntervalSeconds is out of range: ${this.config['checkTasksEverySeconds']}`);
        }
    }
}

module.exports = Scheduler;