const EventEmitter = require('events').EventEmitter;
const Logger = require('./logger');

/*
 * Should be implemented and emit 'disconnect' and 'reconnect' events.
 */
class Storage extends EventEmitter {

    constructor(config) {
        super();
        this.logger = Logger.tagged('storage');
    }

    connect() {
        throw new Error(`connect not implemented`);
    }

    /*
     * taskData - { id, data, start }
     * interval - [seconds]
     */
    assignTask(taskData, interval) {
        throw new Error(`assignTask not implemented`);
    }

    /*
     * acquire and return lock (auto released after 24 hours) that should be unlocked
     */
    tryLock(lockId) {
        throw new Error(`tryLock not implemented`);
    }

    /*
     * unlock lock
     */
    unlock(lock) {
        throw new Error(`unlock not implemented`);
    }

    /*
     * acquire auto released lock
     */
    tryAutoLock(lockId, autoReleaseSeconds) {
        throw new Error(`tryAutoLock not implemented`);
    }

    /*
     * remove task from storage
     */
    removeTask(taskId) {
        throw new Error(`removeTask not implemented`);
    }

    /*
     * pull task and reschedule it is a recurring task
     */
    pullTask() {
        throw new Error(`pullTask not implemented`);
    }

    /*
     * remove all tasks (on moment of execution)
     */
    clearAllTasks() {
        throw new Error(`clearAllTasks not implemented`);
    }

    /*
     * return true if is storage connected
     */
    get connected() {
        throw new Error(`connected not implemented`);
    }

    /*
     * return array of status errors
     */
    get errors() {
        throw new Error(`errors not implemented`);
    }
}

exports.Storage = Storage;