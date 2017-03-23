const expect = require('expect');
const Storage = require('../lib/redisStorage/redisStorage');

describe('redisStorage', () => {

    let storage;
    let mockClients = [
        { schedule: () => {}, connected: true, isScheduled: () => {} },
        { schedule: () => {}, connected: true, isScheduled: () => {} },
        { schedule: () => {}, connected: true, isScheduled: () => {} }
    ];

    before(done => {
        let config = {
            instances: [
                { host: 'localhost', port: '1234', db: 0 },
                { host: 'localhost', port: '4321', db: 0 }],
            masterIndex : 0,
            reconnectIntervalSeconds : 1,
            taskIntervalSeconds : 60
        };
        storage = new Storage(config);
        storage._allClients = mockClients;

        expect.spyOn(storage, 'connect').andCall(() => {
            storage._online = true;
            return Promise.resolve();
        });

        storage.connect().then(done);
    });

    afterEach(() => {
        expect.restoreSpies();
    });

    describe('_lockSchedule', () => {
        it('should lock task', done => {
            let taskId = 'myTaskId';
            let rand = 123456;
            expect.spyOn(storage._mainRedisClient, 'tryLock').andCall(lockId => {
                return Promise.resolve(rand);
            });
            storage._lockSchedule(taskId).then(lock => {
                expect(lock).toBe(rand);
            }).then(done);
        });

        it('should not lock a locked task then try again and if the task is unlocked then succeed', done => {
            let taskId = 'myTaskId';
            let rand = 123456;
            let callNum = 0;
            expect.spyOn(storage._mainRedisClient, 'tryLock').andCall(lockId => {
                return Promise.resolve(2 === ++callNum ? rand : false);
            });
            storage._lockSchedule(taskId).then(lock => {
                expect(lock).toBe(rand);
                expect(callNum).toBe(2);
            }).then(done);
        });
    });

    describe('_unlockSchedule', () => {
        it('should resolve even if lock was already unlocked', done => {
            let taskId = 'myTaskId';
            let lock = 654321;
            expect.spyOn(storage._mainRedisClient, 'unlock').andCall(lock => {
                return Promise.resolve(false);
            });
            storage._unlockSchedule(taskId).then(done);
        });

        it('should resolve even if lock was unlocked', done => {
            let taskId = 'myTaskId';
            let lock = 654321;
            expect.spyOn(storage._mainRedisClient, 'unlock').andCall(lock => {
                return Promise.resolve(true);
            });
            storage._unlockSchedule(taskId).then(done);
        });
    });

    describe('_schedule', () => {
        it('should lock an interval task', done => {
            let taskId = 'myTaskId';
            let data = { myData: true };
            let runOn = Date.now();
            let interval = 5 * 60 * 1000;

            let redis;
            storage._onlineClients.forEach((client, i) => {
                expect.spyOn(client, 'schedule').andCall((...args) => {
                    redis = client;
                    return Promise.resolve(true);
                });
            });
            storage._schedule(taskId, data, runOn, interval).then(scheduled => {
                expect(!!scheduled).toBe(true);
                expect(redis.schedule).toHaveBeenCalledWith(taskId, data, runOn, interval)
            }).then(done);
        });

        it('should lock a one time task', done => {
            let taskId = 'myTaskId';
            let data = { myData: true };
            let runOn = Date.now();
            let redis;
            storage._onlineClients.forEach((client, i) => {
                expect.spyOn(client, 'schedule').andCall((...args) => {
                    redis = client;
                    return Promise.resolve(true);
                });
            });
            storage._schedule(taskId, data, runOn).then(scheduled => {
                expect(!!scheduled).toBe(true);
                expect(redis.schedule).toHaveBeenCalledWith(taskId, data, runOn, undefined)
            }).then(done);
        });
    });

    describe('_isScheduledCancel', () => {
        it('should resolve with true if task is scheduled', done => {
            expect.spyOn(storage, '_someClient').andCall((...args) => {
                return Promise.resolve(true);
            });

            let taskId = 'myTaskId';
            storage._isScheduledCancel(taskId).then(scheduled => {
                expect(scheduled).toBe(true);
                expect(storage._someClient).toHaveBeenCalledWith('isScheduledCancel', taskId);
            }).then(done);
        });

        it('should resolve with false if task is not scheduled in any client', done => {
            expect.spyOn(storage, '_someClient').andCall((...args) => {
                return Promise.resolve(false);
            });

            let taskId = 'myTaskId'
            storage._isScheduledCancel(taskId).then(scheduled => {
                expect(storage._someClient).toHaveBeenCalledWith('isScheduledCancel', taskId);
            }).then(done);
        });
    });

    describe('_isScheduledUpdate', () => {
        it('should resolve with true if task is scheduled', done => {
            expect.spyOn(storage, '_someClient').andCall((...args) => {
                return Promise.resolve(true);
            });

            let taskId = 'myTaskId';
            let data = { myData: true };
            let interval = 5;
            storage._isScheduledUpdate(taskId, data, interval).then(scheduled => {
                expect(scheduled).toBe(true);
                expect(storage._someClient).toHaveBeenCalledWith('isScheduledUpdate', taskId, data, interval);
            }).then(done);
        });

        it('should resolve with false if task is not scheduled in any client', done => {
            expect.spyOn(storage, '_someClient').andCall((...args) => {
                return Promise.resolve(false);
            });

            let taskId = 'myTaskId';
            let data = { myData: true };
            let interval = 5;
            storage._isScheduledUpdate(taskId, data, interval).then(scheduled => {
                expect(storage._someClient).toHaveBeenCalledWith('isScheduledUpdate', taskId, data, interval);
            }).then(done);
        });
    });

    describe('assignTask', () => {
        it('should assign an interval task', done => {
            expect.spyOn(storage, '_lockSchedule').andCall(taskId => {
                return Promise.resolve(`lockOf:${taskId}`);
            });
            expect.spyOn(storage, '_isScheduledUpdate').andCall((taskId, data, interval) => {
                return Promise.resolve(false);
            });
            expect.spyOn(storage, '_schedule').andCall((id, data, startOn, interval) => {
                return Promise.resolve(true);
            });
            expect.spyOn(storage, '_unlockSchedule').andCall(lock => {
                return Promise.resolve(true);
            });

            let taskData = {
                id : 'myTask',
                data : { myData: true },
                startOn : Date.now()
            };
            let interval = 5 * 60;
            storage.assignTask(taskData, interval).then(didSchedule => {
                expect(didSchedule).toBe(true);
                expect(storage._lockSchedule).toHaveBeenCalledWith(taskData['id']);
                expect(storage._isScheduledUpdate).toHaveBeenCalledWith(taskData['id'], taskData['data'], interval);
                expect(storage._schedule).toHaveBeenCalledWith(taskData['id'], taskData['data'], taskData['startOn'], interval);
                expect(storage._unlockSchedule).toHaveBeenCalledWith(`lockOf:${taskData['id']}`);
            }).then(done);
        });

        it('should not assign task if task is assignment is locked', done => {
            expect.spyOn(storage, '_lockSchedule').andCall(taskId => {
                return Promise.reject(new Error('locked already'));
            });
            expect.spyOn(storage, '_isScheduledUpdate').andCall(taskId => {
                return Promise.resolve(false);
            });
            expect.spyOn(storage, '_schedule').andCall((id, data, startOn, interval) => {
                return Promise.resolve(true);
            });
            expect.spyOn(storage, '_unlockSchedule').andCall(lock => {
                return Promise.resolve(true);
            });

            let taskData = {
                id : 'myTask',
                data : { myData: true },
                startOn : Date.now()
            };
            let interval = 5 * 60 * 1000;
            storage.assignTask(taskData, interval).then(didSchedule => {
                expect(false).toBe(true, 'assignTask should have rejected');
            }).catch(err => {
                done();
            });
        });

        it('should assign an one time task', done => {
            expect.spyOn(storage, '_lockSchedule').andCall(taskId => {
                return Promise.resolve(`lockOf:${taskId}`);
            });
            expect.spyOn(storage, '_isScheduledUpdate').andCall(taskId => {
                return Promise.resolve(false);
            });
            expect.spyOn(storage, '_schedule').andCall((id, data, startOn, interval) => {
                return Promise.resolve(true);
            });
            expect.spyOn(storage, '_unlockSchedule').andCall(lock => {
                return Promise.resolve(true);
            });

            let taskData = {
                id : 'myTask',
                data : { myData: true },
                startOn : Date.now()
            };
            storage.assignTask(taskData).then(didSchedule => {
                expect(didSchedule).toBe(true);
                expect(storage._lockSchedule).toHaveBeenCalledWith(taskData['id']);
                let isScheduledArgs = storage._isScheduledUpdate.calls[0].arguments;
                expect(isScheduledArgs).toExist();
                expect(isScheduledArgs[0]).toBe(taskData['id']);
                expect(isScheduledArgs[1]).toEqual(taskData['data']);
                expect(storage._schedule).toHaveBeenCalledWith(taskData['id'], taskData['data'], taskData['startOn'], undefined);
                expect(storage._unlockSchedule).toHaveBeenCalledWith(`lockOf:${taskData['id']}`);
            }).then(done);
        });
    });

    describe('removeTask', () => {
        it('should remove task', done => {
            expect.spyOn(storage, '_isScheduledCancel').andCall(taskId => {
                return Promise.resolve(false);
            });

            let taskId = 'myTask';
            storage.removeTask(taskId).then(() => {
                expect(storage._isScheduledCancel).toHaveBeenCalledWith(taskId);
            }).then(done);
        });
    });

    describe('_someClient', () => {
        it('should return 5 since one client returned 5', done => {
            storage._onlineClients.forEach((client, i) => {
                expect.spyOn(client, 'isScheduled').andCall((...args) => {
                    return Promise.resolve(2 === i ? 5 : false);
                });
            });

            let myArgs = [1,2,3];
            storage._someClient('isScheduled', ...myArgs).then(res => {
                expect(res).toBe(5);
                storage._onlineClients.forEach(client => {
                    expect(client.isScheduled).toHaveBeenCalledWith(...myArgs);
                });
            }).then(done);
        });

        it('should return undefined since all clients returned false', done => {
            storage._onlineClients.forEach((client, i) => {
                expect.spyOn(client, 'isScheduled').andCall((...args) => {
                    return Promise.resolve(null);
                });
            });

            let myArgs = [1,2,3];
            storage._someClient('isScheduled', ...myArgs).then(res => {
                expect(res).toNotExist();
                storage._onlineClients.forEach(client => {
                    expect(client.isScheduled).toHaveBeenCalledWith(...myArgs);
                });
            }).then(done);
        });
    });

    describe('tryLock', () => {
        it('should lock', done => {
            expect.spyOn(storage._mainRedisClient, 'tryLock').andCall(lockBy => {
                return Promise.resolve(`lockOf:${lockBy}`);
            });

            storage.tryLock('myLockId').then(lock => {
                expect(lock).toBe(`lockOf:myLockId`);
                expect(storage._mainRedisClient.tryLock).toHaveBeenCalledWith('myLockId');
            }).then(done);
        });

        it('should not lock a locked lock', done => {
            expect.spyOn(storage._mainRedisClient, 'tryLock').andCall(lockBy => {
                return Promise.resolve(false);
            });

            storage.tryLock('myLockId').then(lock => {
                expect(lock).toBe(false);
                expect(storage._mainRedisClient.tryLock).toHaveBeenCalledWith('myLockId');
            }).then(done);
        });
    });

    describe('tryAutoLock', () => {
        it('should lock', done => {
            expect.spyOn(storage._mainRedisClient, 'tryAutoLock').andReturn(Promise.resolve(true));
            storage.tryAutoLock('myLockId', 5).then(locked => {
                expect(locked).toBe(true);
                expect(storage._mainRedisClient.tryAutoLock).toHaveBeenCalledWith('myLockId', 5);
            }).then(done);
        });

        it('should not lock a locked lock', done => {
            expect.spyOn(storage._mainRedisClient, 'tryAutoLock').andReturn(Promise.resolve(false));
            storage.tryAutoLock('myLockId', 5).then(locked => {
                expect(locked).toBe(false);
                expect(storage._mainRedisClient.tryAutoLock).toHaveBeenCalledWith('myLockId', 5);
            }).then(done);
        });
    });

    describe('unlock', () => {
        it('should unlock', done => {
            expect.spyOn(storage._mainRedisClient, 'unlock').andCall(lock => {
                return Promise.resolve(true);
            });

            storage.unlock('myLockId').then(unlocked => {
                expect(unlocked).toBe(true);
                expect(storage._mainRedisClient.unlock).toHaveBeenCalledWith('myLockId');
            }).then(done);
        });

        it('should not unlock a lock that was locked by another client', done => {
            expect.spyOn(storage._mainRedisClient, 'unlock').andCall(lock => {
                return Promise.resolve(false);
            });

            storage.unlock('myLockId').then(unlocked => {
                expect(unlocked).toBe(false);
                expect(storage._mainRedisClient.unlock).toHaveBeenCalledWith('myLockId');
            }).then(done);
        });
    });

    describe('_someClient', () => {
        it('should resolve with clients value if one online client resolves', done => {
            mockClients.forEach((client, i) => {
                expect.spyOn(client, 'isScheduled').andReturn(Promise.resolve(2 === i ? 'yes' : false));
            });
            let args = [1,2,3];
            storage._someClient('isScheduled', ...args).then(response => {
                expect(response).toBe('yes');
                mockClients.forEach(client => {
                    expect(client.isScheduled).toHaveBeenCalledWith(...args);
                });
            }).then(done);
        });

        it('should resolve with undefined if all online clients dont resolve with a truthy value', done => {
            mockClients.forEach((client, i) => {
                expect.spyOn(client, 'isScheduled').andReturn(Promise.resolve(false));
            });
            let args = [1,2,3];
            storage._someClient('isScheduled', ...args).then(response => {
                expect(response).toBe(undefined);
                mockClients.forEach(client => {
                    expect(client.isScheduled).toHaveBeenCalledWith(...args);
                });
            }).then(done);
        });
    });

});