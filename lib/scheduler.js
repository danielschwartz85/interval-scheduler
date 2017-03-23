const Storage = require('./redisStorage/redisStorage');
const Utils = require ('./utils');
const Promise = require("bluebird");

class Scheduler {

    constructor(config, logger = undefined) {
        Utils.assertKeys(config, 'storage', 'checkTasksEverySeconds');
        this.config = config;
        this.logger = Utils.tryLogger(logger, 'scheduler');
        this._settings = {
            retryLockMilliseconds : 300,
            minCheckTasksEverySeconds : Utils.isLocal() ? 1 : 10,
            maxCheckTasksEverySeconds : 60 * 60
        };
        this._storage = new Storage(config['storage'], logger);
        this.listeningToStorage = false;
        this._validate();
    }

    startTaskAccept() {
        let startAccept;
        if (this.acceptingTasks) {
            return Promise.resolve();
        }
        if (!this._storage.connected) {
            startAccept = this._setupStorage();
        } else {
            startAccept = Promise.resolve();
        }
        this.logger.info('starting task accept..');
        this._acceptingTasks = true;
        return startAccept;
    }

    /*
     * executor - executor function accepts serialized task and resolves
     * deSerialize - deserialize task from hash object
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

        this.logger.info('starting task execute..');
        return startExecute;
    }

    stopTaskExecute() {
        this.logger.info('stopping task execution..');
        this._executingTasks = false;
        return Promise.resolve();
    }

    stopTaskAccept() {
        this.logger.info('stopping task accept..');
        this._acceptingTasks = false;
        return Promise.resolve();
    }

    assignTask(task) {
        if (!this.acceptingTasks) {
            return Promise.reject('not accepting tasks');
        }
        let taskData = {
            id: task.id,
            data: task.serialize(),
            startOn: task.executeOn
        };
        return this._storage.assignTask(taskData, task.onExecuteRescheduleTo);
    }

    removeTask(task) {
        if (!this.acceptingTasks) {
            return Promise.reject('not accepting tasks');
        }
        return this._storage.removeTask(task.id);
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
     * remove all tasks (on moment of execution)
     * terminates only if task count remains bounded to a given maximum size
     */
    clearAllTasks() {
        return this._storage.clearAllTasks();
    }

    /*
     * acquire auto released lock (faster with RedisStorage)
     */
    tryAutoLock(lockBy, autoReleaseSeconds) {
        return this._storage.tryAutoLock(lockBy, autoReleaseSeconds);
    }

    settings(path) {
        return Utils.objGet(this._settings, path);
    }

    get online() {
        return this._storage.connected;
    }

    get acceptingTasks() {
        return this._acceptingTasks;
    }

    get executingTasks() {
        return this._executingTasks;
    }

    get errors() {
        return this._storage.errors || [];
    }

    _setupStorage() {
        if (!this.listeningToStorage) {
            this._storage.on('disconnect', this._onStorageDisconnect.bind(this));
            this._storage.on('reconnect', this._onStorageReconnect.bind(this));
            this.listeningToStorage = true;
        }
        return this._storage.connect();
    }

    _onStorageDisconnect() {
        this.logger.fatal('storage is offline');
    };

    _onStorageReconnect() {
        this.logger.info('storage back online');
    }

    _startTaskPull() {
        let delay = (this.config['checkTasksEverySeconds'] || 10) * 1000;
        let taskPull = () => {
            if (!this.executingTasks) {
                return;
            }
            this._pullAndExecute().then(tasksPending => {
                if (tasksPending) {
                    this.logger.debug('pulling more tasks..');
                    return Promise.resolve();
                } else {
                    this.logger.debug('waiting for tasks..');
                    return Promise.delay(delay);
                }
            }).then(taskPull).catch(e => {
                this.logger.fatal(`failed to pull tasks err: ${e.message}`);
                Promise.delay(delay).then(taskPull);
            });
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
        let min = this.settings('minCheckTasksEverySeconds');
        let max = this.settings('maxCheckTasksEverySeconds');
        if (this.config['checkTasksEverySeconds'] < min || this.config['checkTasksEverySeconds'] > max) {
            throw new Error(`taskIntervalSeconds is out of range: ${this.config['checkTasksEverySeconds']}`);
        }
    }
}

module.exports = Scheduler;