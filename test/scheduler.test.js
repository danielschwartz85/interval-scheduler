const expect = require('expect');
const Task = require('../lib/task');
const Scheduler = require('../lib/scheduler');
const Storage = require('../lib/storage');
const Promise = require("bluebird");

describe('scheduler', () => {

    let scheduler;

    before(done => {
        let config = {
            storage : {
                instances: [
                    {host: 'localhost', port: '1234', db: 0},
                    {host: 'localhost', port: '4321', db: 0}],
                masterIndex: 0,
                reconnectIntervalSeconds: 1,
                taskIntervalSeconds : 60
            },
            checkTasksEverySeconds : 10
        };
        scheduler = new Scheduler(config);
        scheduler._executor = new MockExecutor();
        scheduler._storage = new MockStorage();
        done();
    });

    afterEach(() => {
        expect.restoreSpies();
    });

    describe('assignTask', () => {
        it('should assign a recurring task', done => {
            expect.spyOn(scheduler._storage, 'assignTask').andReturn(Promise.resolve(true));
            let task = new MockTask();
            scheduler._acceptingTasks = true;
            scheduler.assignTask(task).then(res => {
                expect(res).toBe(true);
                expect(scheduler._storage.assignTask).toHaveBeenCalled();
                let assignArgs = scheduler._storage.assignTask.calls[0].arguments;
                expect(assignArgs).toExist();
                expect(assignArgs[1]).toBe(task.onExecuteRescheduleTo);
                expect(assignArgs[0]['data']).toEqual(task.serialize());
                expect(assignArgs[0]['id']).toBe(task.id);
            }).then(() => {
                scheduler._acceptingTasks = false;
            }).then(done);
        });

        it('should assign a one time task', done => {
            expect.spyOn(scheduler._storage, 'assignTask').andReturn(Promise.resolve(true));
            let task = new MockTask({ onExecuteRescheduleTo: null });
            scheduler._acceptingTasks = true;
            scheduler.assignTask(task).then(res => {
                expect(res).toBe(true);
                expect(scheduler._storage.assignTask).toHaveBeenCalled();
                let assignArgs = scheduler._storage.assignTask.calls[0].arguments;
                expect(assignArgs).toExist();
                expect(assignArgs[0]['data']).toEqual(task.serialize());
                expect(assignArgs[0]['id']).toBe(task.id);
                expect(assignArgs[1]).toNotExist();
            }).then(() => {
                scheduler._acceptingTasks = false;
            }).then(done);
        });
    });

    describe('removeTask', () => {
        it('should remove task', done => {
            expect.spyOn(scheduler._storage, 'removeTask').andReturn(Promise.resolve(true));
            let task = new MockTask({ onExecuteRescheduleTo: null });
            scheduler._acceptingTasks = true;
            scheduler.removeTask(task.id).then(res => {
                expect(res).toBe(true);
                expect(scheduler._storage.removeTask).toHaveBeenCalledWith(task.id);
            }).then(() => {
                scheduler._acceptingTasks = false;
            }).then(done);
        });
    });

    describe('_pullAndExecute', () => {
        it('should not execute when there are no tasks and resolve with false', done => {
            expect.spyOn(scheduler._storage, 'pullTask').andReturn(Promise.resolve());
            scheduler._execute = () => {};
            expect.spyOn(scheduler, '_execute');
            scheduler._pullAndExecute().then(res => {
                expect(res).toEqual(false);
                expect(scheduler._execute).toNotHaveBeenCalled();
            }).then(done);
        });

        it('should execute task if there is a pending task and resolve with true', done => {
            expect.spyOn(scheduler._storage, 'pullTask').andReturn(Promise.resolve('myTask'));
            scheduler._execute = () => {};
            expect.spyOn(scheduler, '_execute').andReturn(Promise.resolve());
            scheduler._pullAndExecute().then(res => {
                expect(res).toEqual(true);
                expect(scheduler._execute).toHaveBeenCalledWith('myTask');
            }).then(done);
        });

        it('should execute task if there is a pending task and resolve with true even if execution fails', done => {
            expect.spyOn(scheduler._storage, 'pullTask').andReturn(Promise.resolve('myTask'));
            scheduler._execute = () => {};
            expect.spyOn(scheduler, '_execute').andReturn(Promise.reject());
            scheduler._pullAndExecute().then(res => {
                expect(res).toEqual(true);
                expect(scheduler._execute).toHaveBeenCalledWith('myTask');
            }).then(done);
        });
    });

    describe('_startTaskPull', () => {
        it('should pull tasks as long as there are more tasks pending', done => {
            let i = 0;
            expect.spyOn(scheduler, '_pullAndExecute').andCall(() => {
                ++i;
                if (2 === i) {
                    scheduler._executingTasks = false;
                }
                return Promise.resolve(true);
            });
            scheduler._executingTasks = true;
            scheduler._startTaskPull();
            expect(scheduler._pullAndExecute).toHaveBeenCalled();
            expect(scheduler._pullAndExecute.calls.length).toBe(1);
            done();
        });

        it('should not pull tasks if there are no more tasks pending (waiting for tasks..)', done => {
            expect.spyOn(scheduler, '_pullAndExecute').andCall(() => {
                scheduler._executingTasks = false;
                return Promise.resolve(false);
            });
            scheduler._executingTasks = true;
            scheduler._startTaskPull();
            expect(scheduler._pullAndExecute).toHaveBeenCalled();
            expect(scheduler._pullAndExecute.calls.length).toBe(1);
            done();
        });

        it('should not pull tasks scheduler is not executing tasks', done => {
            expect.spyOn(scheduler, '_pullAndExecute');
            scheduler._executingTasks = false;
            scheduler._startTaskPull();
            expect(scheduler._pullAndExecute).toNotHaveBeenCalled();
            done();
        });
    });
});

class MockTask extends Task {

    constructor(options = {}) {
        super();
        this._serialize = options['serialize'] !== undefined ? options['serialize'] : { myData: true };
        this._onExecuteRescheduleTo = options['onExecuteRescheduleTo'] !== undefined ?  options['onExecuteRescheduleTo'] : 5 * 60;
        this._id = options['id'] !== undefined ? options['id'] : 'myTaskId';
        this._lockBy = options['lockBy'] !== undefined ? options['lockBy'] : null;
        this._executeOn = options['executeOn'] !== undefined ? options['executeOn'] : Date.now();
    }

    serialize() {
        return this._serialize
    }

    get onExecuteRescheduleTo() {
        return this._onExecuteRescheduleTo;
    }

    get id() {
        return this._id;
    }

    get executeOn() {
        return this._executeOn;
    }

    get lockBy() {
        return this._lockBy;
    }
}

class MockExecutor {

    constructor() {
    }

    connect() {
        return Promise.resolve();
    }

    execute(task) {
        return Promise.resolve(task);
    }
}

class MockStorage extends Storage {

    constructor() {
        super({});
        this.mockConnected = true;
    }

    connect() {
        return Promise.resolve(true);
    }

    get autoReleaseSeconds() {
        return this.mockAutoReleaseSeconds || 30;
    }

    get connected() {
        return this.mockConnected;
    }

    pullTask() {

    }
}