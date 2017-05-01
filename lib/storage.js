const EventEmitter = require('events').EventEmitter;
const Utils = require ('./utils');

/*
 * Should emit 'offline' and 'online' events regarding the storage general status and emit
 * specific db events of 'disconnect' and 'reconnect' with a db information message.
 */
class Storage extends EventEmitter {

    constructor(config, logger = undefined) {
        super();
        this.logger = Utils.tryLogger(logger, 'storage');
    }

    connect() {
        throw new Error('connect not implemented');
    }

    /*
     * assign task and return resolve with true or update existing one and resolve with false
     * taskData - { id, data, start }
     * interval - [seconds]
     */
    assignTask(taskData, interval) {
        throw new Error('assignTask not implemented');
    }

    /*
     * acquire and return lock (auto released after 24 hours) that should be unlocked
     */
    tryLock(lockId) {
        throw new Error('tryLock not implemented');
    }

    /*
     * unlock lock
     */
    unlock(lock) {
        throw new Error('unlock not implemented');
    }

    /*
     * acquire auto released lock
     */
    tryAutoLock(lockId, autoReleaseSeconds) {
        throw new Error('tryAutoLock not implemented');
    }

    /*
     * remove task from storage, resolve with true if task was removed
     */
    removeTask(taskId) {
        throw new Error('removeTask not implemented');
    }

    /*
     * pull task and reschedule it is a recurring task
     */
    pullTask() {
        throw new Error('pullTask not implemented');
    }

    /*
     * remove all tasks (on moment of execution) and return number of cleared tasks
     */
    clearAllTasks() {
        throw new Error('clearAllTasks not implemented');
    }

    /*
     * resolve with serialized task data
     */
    peekTask(taskId) {
        throw new Error('peekTask not implemented');
    }

    /*
     * resolve with the number of time slot queues
     */
    queueSize() {
        throw new Error('queueSize not implemented');
    }

    /*
     * return true if is storage is online
     */
    get online() {
        throw new Error('online not implemented');
    }
}

module.exports = Storage;