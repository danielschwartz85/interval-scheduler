const expect = require('expect');
const Scheduler = require('../lib/scheduler');
const Task = require('../lib/task');
const Utils = require('../lib/utils');

/*
 * 'integration' is connecting to actual local redis if one exists
 */
describe('integration', () => {

    let scheduler, redis;

    before(done => {
        let config = {
            storage : {
                instances: [
                    { host: 'localhost', port: '6379', db: 5 }
                ],
                masterIndex: 0,
                taskIntervalSeconds : 60
            },
            checkTasksEverySeconds : 10
        };

        let logger = {};
        ['debug', 'info', 'warn', 'error', 'fatal', 'audit'].forEach(level => {
            logger[level] = (msg) => { console.log(msg) };
        });

        scheduler = new Scheduler(config, logger);
        scheduler.connect().then(done).catch(() => {
            done();
        });
    });

    beforeEach(function(done) {
        if(!scheduler.online) {
            this.skip();
        } else {
            done();
        }
    });

    afterEach(() => {
        expect.restoreSpies();
    });

    describe('scheduler', () => {
        it('should assign a task', done => {
            let task = new MockTask({ id: 'myId' });
            scheduler.assignTask(task).then(res => {
                return scheduler.isScheduled(task.id);
            }).then(redisTask => {
                expect(redisTask).toEqual(task.serialize());
                done();
            });
        });

        it('should remove a task', done => {
            let task = new MockTask({ id: 'myId' });
            scheduler.assignTask(task).then(res => {
                return scheduler.removeTask(task.id);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(scheduled => {
                expect(scheduled).toBe(false);
                done();
            });
        });

        it('should assign multiple tasks', done => {
            let task = new MockTask({ id: 'myId' });
            let task2 = new MockTask({ id: 'myId2' });
            scheduler.assignTask(task).then(res => {
                return scheduler.assignTask(task2);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(redisTask => {
                expect(redisTask).toEqual(task.serialize());
            }).then(() => {
                return scheduler.isScheduled(task2.id);
            }).then(redisTask2 => {
                expect(redisTask2).toEqual(task2.serialize());
                done();
            });
        });

        it('should remove multiple tasks', done => {
            let task = new MockTask({ id: 'myId' });
            let task2 = new MockTask({ id: 'myId2' });
            scheduler.assignTask(task).then(() => {
                return scheduler.assignTask(task2);
            }).then(() => {
                return scheduler.removeTask(task.id);
            }).then(() => {
                return scheduler.removeTask(task2.id);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(scheduled => {
                expect(scheduled).toBe(false);
            }).then(() => {
                return scheduler.isScheduled(task2.id);
            }).then(scheduled => {
                expect(scheduled).toBe(false);
                done();
            });
        });

        it('should assign and remove multiple tasks', done => {
            let task = new MockTask({ id: 'myId' });
            let task2 = new MockTask({ id: 'myId2' });
            let task3 = new MockTask({ id: 'myId3' });
            scheduler.assignTask(task).then(() => {
                return scheduler.assignTask(task2);
            }).then(() => {
                return scheduler.assignTask(task3);
            }).then(() => {
                // remove task 2
                return scheduler.removeTask(task2.id);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(redisTask => {
                expect(redisTask).toEqual(task.serialize());
            }).then(() => {
                return scheduler.isScheduled(task3.id);
            }).then(redisTask3 => {
                expect(redisTask3).toEqual(task3.serialize());
            }).then(() => {
                return scheduler.isScheduled(task2.id);
            }).then(scheduled => {
                expect(scheduled).toBe(false);
                done();
            });
        });

        it('should assign task only once', done => {
            let task = new MockTask({ id: 'myId' });
            scheduler.assignTask(task).then(() => {
                return scheduler.assignTask(task);
            }).then(() => {
                return scheduler.assignTask(task);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(redisTask => {
                expect(redisTask).toEqual(task.serialize());
            }).then(() => {
                return scheduler.removeTask(task.id);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(scheduled => {
                expect(scheduled).toBe(false);
                done();
            });
        });

        it('should not throw when removing same task twice', done => {
            let task = new MockTask({ id: 'myId' });
            scheduler.assignTask(task).then(() => {
                return scheduler.assignTask(task);
            }).then(() => {
                return scheduler.assignTask(task);
            }).then(() => {
                return scheduler.removeTask(task.id);
            }).then(() => {
                return scheduler.removeTask(task.id);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(scheduled => {
                expect(scheduled).toBe(false);
                done();
            });
        });

        it('should be able to assign remove and re assign task', done => {
            let task = new MockTask({ id: 'myId' });
            scheduler.assignTask(task).then(() => {
                return scheduler.removeTask(task.id);
            }).then(() => {
                return scheduler.assignTask(task);
            }).then(() => {
                return scheduler.isScheduled(task.id);
            }).then(scheduled => {
                expect(scheduled).toEqual(task.serialize());
                done();
            });
        });

        it('should acquire a lock', done => {
            scheduler.tryLock('MyLock1').then(lock => {
                return Promise.all([lock, scheduler.tryLock('myLock')]);
            }).then(([lock, isLocked]) => {
                expect(isLocked).toBe(false);
                return scheduler.unlock(lock);
            }).then(() => done() );
        });

        it('should release a lock', done => {
            scheduler.tryLock('MyLock2').then(lock => {
                return scheduler.unlock(lock);
            }).then(() => {
                return scheduler.tryLock('myLock2');
            }).then(isLocked => {
                expect(isLocked).toBe(false);
                done();
            });
        });

        it('should lock many locks', done => {
            Promise.all([scheduler.tryLock('MyLock5'), scheduler.tryLock('MyLock6')]).then(locks => {
                return Promise.all([locks, scheduler.tryLock('MyLock5'), scheduler.tryLock('MyLock6')]);
            }).then(([locks, isLocked3, isLocked4]) => {
                expect(isLocked3).toBe(false);
                expect(isLocked4).toBe(false);
                return Promise.all([scheduler.unlock(locks[0]), scheduler.unlock(locks[1])]);
            }).then(() => {
                done();
            });
        });

        it('should release many locks', done => {
            Promise.all([scheduler.tryLock('MyLock7'), scheduler.tryLock('MyLock8')]).then(locks => {
                return Promise.all([scheduler.unlock(locks[0]), scheduler.unlock(locks[1])]);
            }).then(() => {
                return Promise.all([scheduler.tryLock('MyLock7'), scheduler.tryLock('MyLock8')])
            }).then(locks => {
                expect(locks[0]).toExist();
                expect(locks[1]).toExist();
                return Promise.all([scheduler.unlock(locks[0]), scheduler.unlock(locks[1])]);
            }).then(() => {
                done();
            });
        });

        it('should acquire an auto release lock', done => {
            scheduler.tryAutoLock('MyLock9', 5).then(()=> {
                return scheduler.tryAutoLock('MyLock9', 5);
            }).then(locked => {
                expect(locked).toBe(false);
                done();
            });
        });

        it('should acquire multiple auto release locks', done => {
            Promise.all([scheduler.tryAutoLock('MyLock10', 5), scheduler.tryAutoLock('MyLock11', 5)]).then(() => {
                return Promise.all([scheduler.tryAutoLock('MyLock10', 5), scheduler.tryAutoLock('MyLock11', 5)]);
            }).then(([locked10, locked11]) => {
                expect(locked10).toBe(false);
                expect(locked11).toBe(false);
                done();
            });
        });

        it('auto release lock should be released', done => {
            scheduler.tryAutoLock(`MyLock12${Math.random()}`, 1).then(() => {
                return Utils.delay(1000);
            }).then(() => {
                return scheduler.tryAutoLock('MyLock12', 1);
            }).then(locked => {
                expect(locked).toBe(true);
                done();
            });
        });
    });
});

class MockTask extends Task {

    constructor(options = {}) {
        super();
        this._serialize = options['serialize'] !== undefined ? options['serialize'] : { myData: true };
        this._id = options['id'] !== undefined ? options['id'] : 'myTaskId';
        this._lockBy = options['lockBy'] !== undefined ? options['lockBy'] : null;
        this._executeOn = options['executeOn'] !== undefined ? options['executeOn'] : Date.now();
        if (options['onExecuteRescheduleTo'] !== undefined) {
            this._onExecuteRescheduleTo = options['onExecuteRescheduleTo'];
        } else {
            this._onExecuteRescheduleTo = 5 * 60;
        }
    }

    serialize() {
        return JSON.stringify({id: this._id});
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