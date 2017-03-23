const Logger = require('./logger');

/*
 * Should be implemented.
 */
class Task {

    /*
     * serialize task to string
     */
    serialize() {
        throw new Error(`serialize not implemented`);
    }

    /*
     * when executed automatically re schedule
     * if this returns the number of seconds.
     */
    get onExecuteRescheduleTo() {
        throw new Error(`onExecuteRescheduleTo not implemented`);
    }

    /*
     * return unix epoch
     */
    get executeOn() {
        throw new Error(`executeOn not implemented`);
    }

    get id() {
        throw new Error(`id not implemented`);
    }

}

exports.Task = Task;