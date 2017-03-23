const expect = require('expect');
const Redis = require('../redisStorage/redisClient').Redis;
const Logger = require('./logger');
const bluebird = require('bluebird');
const fs = bluebird.promisifyAll(require("fs"));

describe('lua', () => {

    let redis;

    before(done => {
        Logger.init({ isMaster : true });
        redis = new Redis({ host: 'localhost', port: '6379', db: 0 });
        redis.connect().then(done).catch(() => {
            redis = undefined;
            done();
        });
    });

    beforeEach(function(done) {
        if(!redis) {
            this.skip();
        } else {
            done();
        }
    });

    describe('existsupdate', () => {
        it('should return 0 for non existing key', done => {
            redis._lua('EXISTSUPDATE', 'nonExistentKey').then(exists => {
                expect(exists).toBe(0);
                done();
            });
        });

        it('should return 1 for existing key (with cancel true) un cancel it, update its data and interval', done => {
            redis._client.hmsetAsync('myKey', 'canceled', true, 'data', 'someValue', 'interval', 456).then(() => {
                return redis._lua('EXISTSUPDATE', 'myKey', 'updatedData', 789);
            }).then(exists => {
                expect(exists).toBe(1);
            }).then(() => {
                return redis._client.hmgetAsync('myKey', 'canceled', 'data', 'interval');
            }).then(memberValues => {
                return Promise.all([memberValues, redis._client.delAsync('myKey')]);
            }).then(results => {
                let memberValues = results[0];
                expect(memberValues[0]).toBe('false');
                expect(memberValues[1]).toBe('updatedData');
                expect(memberValues[2]).toBe('789');
            }).then(done);
        });

        it('should return 1 for existing key (with cancel true) un cancel it and update its data but not its interval', done => {
            redis._client.hmsetAsync('myKey', 'canceled', true, 'data', 'someValue', 'interval', 456).then(() => {
                return redis._lua('EXISTSUPDATE', 'myKey', 'updatedData');
            }).then(exists => {
                expect(exists).toBe(1);
            }).then(() => {
                return redis._client.hmgetAsync('myKey', 'canceled', 'data', 'interval');
            }).then(memberValues => {
                return Promise.all([memberValues, redis._client.delAsync('myKey')]);
            }).then(results => {
                let memberValues = results[0];
                expect(memberValues[0]).toBe('false');
                expect(memberValues[1]).toBe('updatedData');
                expect(memberValues[2]).toBe('456');
            }).then(done);
        });

        it('should return 1 for existing key (with cancel false) and update its data and interval', done => {
            redis._client.hmsetAsync('myKey', 'canceled', false, 'data', 'someValue', 'interval', 456).then(() => {
                return redis._lua('EXISTSUPDATE', 'myKey', 'updatedData', 789);
            }).then(exists => {
                expect(exists).toBe(1);
            }).then(() => {
                return redis._client.hmgetAsync('myKey', 'canceled', 'data', 'interval');
            }).then(memberValues => {
                return Promise.all([memberValues, redis._client.delAsync('myKey')]);
            }).then(results => {
                let memberValues = results[0];
                expect(memberValues[0]).toBe('false');
                expect(memberValues[1]).toBe('updatedData');
                expect(memberValues[2]).toBe('789');
            }).then(done);
        });

        it('should return 1 for existing key (with cancel false) and update its data but not its interval', done => {
            redis._client.hmsetAsync('myKey', 'canceled', false, 'data', 'someValue', 'interval', 456).then(() => {
                return redis._lua('EXISTSUPDATE', 'myKey', 'updatedData');
            }).then(exists => {
                expect(exists).toBe(1);
            }).then(() => {
                return redis._client.hmgetAsync('myKey', 'canceled', 'data', 'interval');
            }).then(memberValues => {
                return Promise.all([memberValues, redis._client.delAsync('myKey')]);
            }).then(results => {
                let memberValues = results[0];
                expect(memberValues[0]).toBe('false');
                expect(memberValues[1]).toBe('updatedData');
                expect(memberValues[2]).toBe('456');
            }).then(done);
        });
    });

    describe('existsdisable', () => {
        it('should return 0 for non existing key', done => {
            redis._lua('EXISTSDISABLE', 'nonExistentKey').then(exists => {
                expect(exists).toBe(0);
                done();
            });
        });

        it('should return 0 for existing key (with cancel false) and cancel it', done => {
            redis._client.hmsetAsync('myKey', 'canceled', false, 'otherMember', 'someValue').then(() => {
                return redis._lua('EXISTSDISABLE', 'myKey');
            }).then(enabled => {
                expect(enabled).toBe(0);
            }).then(() => {
                return redis._client.hgetAsync('myKey', 'canceled');
            }).then(memberValue => {
                return Promise.all([memberValue, redis._client.delAsync('myKey')]);
            }).then(results => {
                expect(results[0]).toBe('true');
            }).then(done);
        });

        it('should return 0 for existing key (with cancel true) and then key should still be canceled', done => {
            redis._client.hmsetAsync('myKey', 'canceled', true, 'otherMember', 'someValue').then(() => {
                return redis._lua('EXISTSDISABLE', 'myKey');
            }).then(enabled => {
                expect(enabled).toBe(0);
            }).then(() => {
                return redis._client.hgetAsync('myKey', 'canceled');
            }).then(memberValue => {
                return Promise.all([memberValue, redis._client.delAsync('myKey')]);
            }).then(results => {
                expect(results[0]).toBe('true');
            }).then(done);
        });
    });

    describe('unlock', () => {
        it('should return 0 for non existing key', done => {
            redis._lua('UNLOCK', 'nonExistentKey').then(unlocked => {
                expect(unlocked).toBe(0);
                done();
            });
        });

        it('should return 0 for existing key with wrong key value', done => {
            let nonExistingValue = 123456;
            redis._client.setAsync('myLockKey', `${nonExistingValue}789`).then(() => {
                return redis._lua('UNLOCK', 'myLockKey', nonExistingValue);
            }).then(unlocked => {
                return Promise.all([unlocked, redis._client.delAsync('myLockKey')]);
            }).then(results => {
                expect(results[0]).toBe(0);
            }).then(done);
        });

        it('should return 1 for existing key with correct key value', done => {
            let keyValue = 987654;
            redis._client.setAsync('myLockKey', keyValue).then(() => {
                return redis._lua('UNLOCK', 'myLockKey', keyValue);
            }).then(unlocked => {
                return Promise.all([unlocked, redis._client.delAsync('myLockKey')]);
            }).then(results => {
                expect(results[0]).toBe(1);
            }).then(done);
        });
    });

    describe('pulltask', () => {
        it('should return 0 for empty index queue', done => {
            redis._lua('PULLTASK', 'indexQueueName', Date.now() - (24 * 60 * 60 * 1000)).then(res => {
                expect(res).toBe(0);
            }).then(done);
        });

        it('should return 0 for index queue with only future tasks', done => {
            let executeOn = (Date.now() + (24 * 60 * 60 * 1000));
            redis._client.zaddAsync('indexQueueName', executeOn, executeOn).then(() => {
                return redis._lua('PULLTASK', 'indexQueueName', Date.now());
            }).then(task => {
                return Promise.all([task, redis._client.delAsync('indexQueueName')]);
            }).then(results => {
                expect(results[0]).toBe(0);
            }).then(done);
        });

        it('should remove a canceled task and delete its metadata', done => {
            let executeOn = Date.now();
            let taskId = 'myTaskId';
            redis._client.multi()
                .lpush(executeOn, taskId)
                .zadd('indexQueueName', executeOn, executeOn)
                .hmset(taskId, 'data', JSON.stringify({myData: true}), 'canceled', true)
                .execAsync()
                .then(() => {
                return redis._lua('PULLTASK', 'indexQueueName', Date.now());
            }).then(task => {
                return Promise.all([
                    task,
                    redis._client.llenAsync(executeOn),
                    redis._client.zrangeAsync('indexQueueName', 0 , 0),
                    redis._client.hlenAsync(taskId)
                ]);
            }).then(results => {
                return Promise.all([...results,
                    redis._client.delAsync(executeOn),
                    redis._client.delAsync('indexQueueName'),
                    redis._client.delAsync(taskId)
                ]);
            }).then(results => {
                let [task, queueSize, indexQueueNextTask, taskInfoExists] = results;
                expect(task).toBe(0);
                expect(queueSize).toBe(0);
                expect(indexQueueNextTask && indexQueueNextTask.length).toBe(0);
                expect(taskInfoExists).toBe(0);
            }).then(done);
        });

        it('should remove a canceled task and not delete its metadata', done => {
            let executeOn = Date.now();
            let taskId = 'myTaskId';
            let taskId2 = 'myTaskId2';
            redis._client.multi()
                .lpush(executeOn, taskId)
                .lpush(executeOn, taskId2)
                .zadd('indexQueueName', executeOn, executeOn)
                .hmset(taskId, 'data', JSON.stringify({myData: true}), 'canceled', true)
                .hmset(taskId2, 'data', JSON.stringify({myData: true}), 'canceled', true)
                .execAsync()
                .then(() => {
                    return redis._lua('PULLTASK', 'indexQueueName', Date.now());
                }).then(task => {
                return Promise.all([
                    task,
                    redis._client.llenAsync(executeOn),
                    redis._client.zrangeAsync('indexQueueName', 0 , 0),
                    redis._client.hlenAsync(taskId),
                    redis._client.hlenAsync(taskId2)
                ]);
            }).then(results => {
                return Promise.all([...results,
                    redis._client.delAsync(executeOn),
                    redis._client.delAsync('indexQueueName'),
                    redis._client.delAsync(taskId),
                    redis._client.delAsync(taskId2)
                ]);
            }).then(results => {
                let [task, queueSize, indexQueueNextTask, taskInfoExists, taskInfo2Exists] = results;
                expect(task).toBe(0);
                expect(queueSize).toBe(1);
                expect(indexQueueNextTask).toExist();
                expect(indexQueueNextTask[0]).toEqual(executeOn);
                expect(taskInfoExists).toBe(0);
                expect(taskInfo2Exists).toBeGreaterThan(0);
            }).then(done);
        });

        it('should return task data and reschedule it (and clear index queue)', done => {
            let executeOn = Date.now() - 2000;
            let now = Date.now();
            let taskId = 'myTaskId';
            redis._client.multi()
                .lpush(executeOn, taskId)
                .zadd('indexQueueName', executeOn, executeOn)
                .hmset(taskId, 'data', JSON.stringify({myData: true}), 'canceled', false, 'interval', 5000)
                .execAsync()
                .then(() => {
                    return redis._lua('PULLTASK', 'indexQueueName', now);
                }).then(task => {
                return Promise.all([
                    task,
                    redis._client.llenAsync(executeOn),
                    redis._client.zrangeAsync('indexQueueName', 0 , 0),
                    redis._client.hlenAsync(taskId),
                    redis._client.rpopAsync(now + 5000),
                ]);
            }).then(results => {
                return Promise.all([...results,
                    redis._client.delAsync(executeOn),
                    redis._client.delAsync('indexQueueName'),
                    redis._client.delAsync(taskId),
                    redis._client.delAsync(now + 5000)
                ]);
            }).then(results => {
                let [task, queueSize, indexQueueNextTask, taskInfoExists, nextScheduledTask] = results;
                expect(task).toBe(JSON.stringify({myData: true}));
                expect(queueSize).toBe(0);
                expect(indexQueueNextTask).toExist();
                expect(indexQueueNextTask.length).toBe(1);
                expect(indexQueueNextTask[0]).toEqual(now + 5000);
                expect(nextScheduledTask).toBe(taskId);
                expect(taskInfoExists).toBeGreaterThan(0);
            }).then(done);
        });

        it('should return task data and not reschedule it', done => {
            let executeOn = Date.now() - 2000;
            let now = Date.now();
            let taskId = 'myTaskId';
            redis._client.multi()
                .lpush(executeOn, taskId)
                .zadd('indexQueueName', executeOn, executeOn)
                .hmset(taskId, 'data', JSON.stringify({myData: true}), 'canceled', false)
                .execAsync()
                .then(() => {
                    return redis._lua('PULLTASK', 'indexQueueName', now);
                }).then(task => {
                return Promise.all([
                    task,
                    redis._client.llenAsync(executeOn),
                    redis._client.zrangeAsync('indexQueueName', 0 , 0),
                    redis._client.hlenAsync(taskId),
                ]);
            }).then(results => {
                return Promise.all([...results,
                    redis._client.delAsync(executeOn),
                    redis._client.delAsync('indexQueueName'),
                    redis._client.delAsync(taskId)
                ]);
            }).then(results => {
                let [task, queueSize, indexQueueNextTask, taskInfoExists] = results;
                expect(task).toBe(JSON.stringify({myData: true}));
                expect(queueSize).toBe(0);
                expect(indexQueueNextTask).toExist();
                expect(indexQueueNextTask.length).toBe(0);
                expect(taskInfoExists).toBe(0);
            }).then(done);
        });

        it('should return task data and reschedule it (and not clear index queue)', done => {
            let executeOn = Date.now() - 2000;
            let now = Date.now();
            let taskId = 'myTaskId';
            let taskId2 = 'myTaskId2';
            redis._client.multi()
                .lpush(executeOn, taskId)
                .lpush(executeOn, taskId2)
                .zadd('indexQueueName', executeOn, executeOn)
                .hmset(taskId, 'data', JSON.stringify({myData: true}), 'canceled', false, 'interval', 5000)
                .hmset(taskId2, 'data', JSON.stringify({myData2: true}), 'canceled', false, 'interval', 5000)
                .execAsync()
                .then(() => {
                    return redis._lua('PULLTASK', 'indexQueueName', now);
                }).then(task => {
                return Promise.all([
                    task,
                    redis._client.llenAsync(executeOn),
                    redis._client.zrangeAsync('indexQueueName', 0 , 1),
                    redis._client.hlenAsync(taskId),
                    redis._client.hlenAsync(taskId2),
                    redis._client.rpopAsync(now + 5000),
                ]);
            }).then(results => {
                return Promise.all([...results,
                    redis._client.delAsync(executeOn),
                    redis._client.delAsync('indexQueueName'),
                    redis._client.delAsync(taskId),
                    redis._client.delAsync(now + 5000)
                ]);
            }).then(results => {
                let [task, queueSize, indexQueueNextTask, taskInfoExists, taskInfo2Exists, nextScheduledTask] = results;
                expect(task).toBe(JSON.stringify({myData: true}));
                expect(queueSize).toBe(1);
                expect(indexQueueNextTask).toExist();
                expect(indexQueueNextTask.length).toBe(2);
                expect(indexQueueNextTask[0]).toEqual(executeOn);
                expect(indexQueueNextTask[1]).toEqual(now + 5000);
                expect(nextScheduledTask).toBe(taskId);
                expect(taskInfoExists).toBeGreaterThan(0);
                expect(taskInfo2Exists).toBeGreaterThan(1);
            }).then(done);
        });
    });
});

describe('redisClient', () => {

    let redis;
    let redisClientMock = {
        setAsync : () => {},
        evalshaAsync : () => {},
        scriptAsync : () => {},
        _multiMock : {
            lpush : () => { return 5 },
            zadd : () => {},
            hmset : () => {},
            execAsync : () => {}
        },
        multi : () => {
            return redisClientMock._multiMock;
        }
    };

    before(done => {
        Logger.init({ isMaster : true });
        redis = new Redis({ host: 'localhost', port: '1234', db: 0 });
        expect.spyOn(redis, 'connect').andCall(() => {
            redis._client = redisClientMock;
            return Promise.resolve();
        });
        redis.connect().then(done);
    });

    afterEach(() => {
        expect.restoreSpies();
    });

    describe('tryLock', () => {
        it('should lock task', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis._client, 'setAsync').andReturn(Promise.resolve('OK'));
            redis.tryLock(taskId).then(lock => {
                expect(lock).toExist();
                expect(lock['key']).toBe(redis._lockName(taskId));
                expect(redis._client.setAsync).toHaveBeenCalled();
            }).then(done);
        });

        it('should not lock task', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis._client, 'setAsync').andReturn(Promise.resolve());
            redis.tryLock(taskId).then(lock => {
                expect(lock).toBe(false);
                expect(redis._client.setAsync).toHaveBeenCalled();
            }).then(done);
        });

        it('should lock task with ttl of autoReleaseSeconds', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis._client, 'setAsync').andReturn(Promise.resolve('OK'));
            redis.tryLock(taskId).then(lock => {
                expect(lock).toExist();
                expect(lock['key']).toBe(redis._lockName(taskId));
                expect(redis._client.setAsync).toHaveBeenCalled();
                let calledWithArgs = redis._client.setAsync.calls[0].arguments;
                let autoReleaseSeconds = redis.settings('scheduleLock/autoReleaseSeconds');
                expect(calledWithArgs[calledWithArgs.length - 1]).toBe(autoReleaseSeconds);
                expect(calledWithArgs[0]).toBe(redis._lockName(taskId));
            }).then(done);
        });
    });

    describe('tryAutoLock', () => {
        it('should lock task', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis._client, 'setAsync').andReturn(Promise.resolve('OK'));
            redis.tryAutoLock(taskId, 5).then(locked => {
                expect(locked).toBe(true);
                expect(redis._client.setAsync).toHaveBeenCalled();
                expect(redis._client.setAsync.calls[0].arguments).toInclude(5);
                expect(redis._client.setAsync.calls[0].arguments).toInclude('EX');
            }).then(done);
        });

        it('should not lock task', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis._client, 'setAsync').andReturn(Promise.resolve());
            redis.tryAutoLock(taskId, 5).then(locked => {
                expect(locked).toBe(false);
                expect(redis._client.setAsync.calls[0].arguments).toInclude(5);
                expect(redis._client.setAsync.calls[0].arguments).toInclude('EX');
            }).then(done);
        });
    });

    describe('unlock', () => {
        it('should not un lock a lock that does not exist or was locked by another', done => {
            let lockId = 'myLockId';
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(0));
            redis.unlock(lockId).then(unlcoked => {
                expect(unlcoked).toBe(false);
                expect(redis._lua).toHaveBeenCalled();
            }).then(done);
        });

        it('should un lock a lock that exists and was locked by the client', done => {
            let lockId = 'myLockId';
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(1));
            redis.unlock(lockId).then(unlcoked => {
                expect(unlcoked).toBe(true);
                expect(redis._lua).toHaveBeenCalled();
            }).then(done);
        });
    });

    describe('isScheduledCancel', () => {
        it('should return false for a non existing task or a canceled task', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(0));
            redis.isScheduledCancel(taskId).then(scheduled => {
                expect(scheduled).toBe(false);
                let luaArgs = redis._lua.calls[0].arguments;
                expect(luaArgs).toExist();
                expect(luaArgs[0]).toBe('EXISTSDISABLE');
            }).then(done);
        });

        it('should return true for an existing task', done => {
            let taskId = 'myTaskId';
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(1));
            redis.isScheduledCancel(taskId).then(scheduled => {
                expect(scheduled).toBe(true);
                let luaArgs = redis._lua.calls[0].arguments;
                expect(luaArgs).toExist();
                expect(luaArgs[0]).toBe('EXISTSDISABLE');
            }).then(done);
        });
    });

    describe('isScheduledUpdate', () => {
        it('should return false for a non existing task or a canceled task', done => {
            let taskId = 'myTaskId';
            let data = 'myData';
            let interval = 5;
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(0));
            expect.spyOn(redis, '_roundToTaskInterval').andReturn('intervalRounded');
            redis.isScheduledUpdate(taskId, data, interval).then(scheduled => {
                expect(scheduled).toBe(false);
                expect(redis._lua).toHaveBeenCalledWith('EXISTSUPDATE', redis._taskName(taskId), data, 'intervalRounded');
                expect(redis._roundToTaskInterval).toHaveBeenCalledWith(interval * 1000);
            }).then(done);
        });

        it('should return true for an existing task', done => {
            let taskId = 'myTaskId';
            let data = 'myData';
            let interval = 5;
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(1));
            expect.spyOn(redis, '_roundToTaskInterval').andReturn('intervalRounded');
            redis.isScheduledUpdate(taskId, data, interval).then(scheduled => {
                expect(scheduled).toBe(true);
                expect(redis._lua).toHaveBeenCalledWith('EXISTSUPDATE', redis._taskName(taskId), data, 'intervalRounded');
                expect(redis._roundToTaskInterval).toHaveBeenCalledWith(interval * 1000);
            }).then(done);
        });
    });

    describe('schedule', () => {
        it('should schedule a recurring task and return the queue name', done => {
            let taskId = 'myTaskId';
            let data = { 'myData' : true };
            let startOn = Date.now();
            let interval = 5 * 60;

            let roundedToMinuteDate = new Date(startOn);
            roundedToMinuteDate.setMilliseconds(0);
            roundedToMinuteDate.setSeconds(0);
            roundedToMinuteDate = roundedToMinuteDate.getTime();
            expect.spyOn(redis._client.multi(), 'lpush').andReturn(redis._client.multi());
            expect.spyOn(redis._client.multi(), 'zadd').andReturn(redis._client.multi());
            expect.spyOn(redis._client.multi(), 'hmset').andReturn(redis._client.multi());
            expect.spyOn(redis._client.multi(), 'execAsync').andReturn(Promise.resolve());
            redis.schedule(taskId, data, startOn, interval).then(queueName => {
                expect(queueName).toBe(roundedToMinuteDate);
                expect(redis._client.multi().lpush).toHaveBeenCalledWith(roundedToMinuteDate, redis._taskName(taskId));

                expect(redis._client.multi().zadd).toHaveBeenCalled();
                let zaddArgs = redis._client.multi().zadd.calls[0].arguments;
                expect(zaddArgs[1]).toBe(roundedToMinuteDate);
                expect(zaddArgs[2]).toBe(roundedToMinuteDate);

                expect(redis._client.multi().hmset).toHaveBeenCalled();
                let hmsetArgs = redis._client.multi().hmset.calls[0].arguments;
                expect(hmsetArgs).toInclude(redis._taskName(taskId));
                expect(hmsetArgs).toInclude(false);
                expect(hmsetArgs).toInclude('interval');
                expect(hmsetArgs).toInclude(interval * 1000);
                expect(hmsetArgs.indexOf('data')).toBeGreaterThan(-1);
                expect(hmsetArgs[hmsetArgs.indexOf('data') + 1]).toExist();
                expect(hmsetArgs[hmsetArgs.indexOf('data') + 1]['myData']).toBe(true);

                expect(redis._client.multi().execAsync).toHaveBeenCalled();
            }).then(done);
        });

        it('should schedule a one time task and return the queue name', done => {
            let taskId = 'myTaskId';
            let data = { 'myData' : true };
            let startOn = Date.now();
            let interval = 5 * 60 * 1000;

            let roundedToMinuteDate = new Date(startOn);
            roundedToMinuteDate.setMilliseconds(0);
            roundedToMinuteDate.setSeconds(0);
            roundedToMinuteDate = roundedToMinuteDate.getTime();
            expect.spyOn(redis._client.multi(), 'lpush').andReturn(redis._client.multi());
            expect.spyOn(redis._client.multi(), 'zadd').andReturn(redis._client.multi());
            expect.spyOn(redis._client.multi(), 'hmset').andReturn(redis._client.multi());
            expect.spyOn(redis._client.multi(), 'execAsync').andReturn(Promise.resolve());
            redis.schedule(taskId, data, startOn).then(queueName => {
                expect(queueName).toBe(roundedToMinuteDate);
                expect(redis._client.multi().lpush).toHaveBeenCalledWith(roundedToMinuteDate, redis._taskName(taskId));

                expect(redis._client.multi().zadd).toHaveBeenCalled();
                let zaddArgs = redis._client.multi().zadd.calls[0].arguments;
                expect(zaddArgs[1]).toBe(roundedToMinuteDate);
                expect(zaddArgs[2]).toBe(roundedToMinuteDate);

                expect(redis._client.multi().hmset).toHaveBeenCalled();
                let hmsetArgs = redis._client.multi().hmset.calls[0].arguments;
                expect(hmsetArgs).toInclude(redis._taskName(taskId));
                expect(hmsetArgs).toInclude(false);
                expect(hmsetArgs).toNotInclude(interval);
                expect(hmsetArgs).toNotInclude('interval');
                expect(hmsetArgs.indexOf('data')).toBeGreaterThan(-1);
                expect(hmsetArgs[hmsetArgs.indexOf('data') + 1]).toExist();
                expect(hmsetArgs[hmsetArgs.indexOf('data') + 1]['myData']).toBe(true);

                expect(redis._client.multi().execAsync).toHaveBeenCalled();
            }).then(done);
        });
    });

    describe('_eval', () => {
        it('evalsha should be called with the correct script and arguments', done => {
            redis._scripts = {
                'ASCRIPT' : {sha: 'aaa', keys: 1},
                'BSCRIPT' : {sha: 'bbb', keys: 2}
            };
            Promise.all(['ASCRIPT', 'BSCRIPT'].map(scriptName => {
                expect.spyOn(redis._client, 'evalshaAsync').andReturn(Promise.resolve());
                let myArgs = [11,22,33];
                let checkScript = redis._eval(scriptName, ...myArgs).then(res => {
                    let s = redis._scripts[scriptName];
                    expect(redis._client.evalshaAsync).toHaveBeenCalledWith(s['sha'], s['keys'], ...myArgs);
                });
                return checkScript;
            })).then(() => {
                done();
            });
        });
    });

    describe('_lua', () => {
        it('_eval should be called with the correct script and arguments', done => {
            redis._scripts = { 'ASCRIPT' : { sha: 'aaa', keys: 1 }};
            expect.spyOn(redis, '_eval').andCall((name, ...args) => {
                return Promise.resolve([9,8,7]);
            });
            let myArgs = [11,22,33];
            redis._lua('ASCRIPT', ...myArgs).then(res => {
                expect(res).toEqual([9,8,7]);
                expect(redis._eval).toHaveBeenCalledWith('ASCRIPT', ...myArgs);
            }).then(done);
        });

        it('should reload scripts when failing to load them and then try again', done => {
            redis._scripts = { 'ASCRIPT' : { sha: 'aaa', keys: 1 }};
            let scriptLoaded = false;
            expect.spyOn(redis, '_eval').andCall((name, ...args) => {
                let err = 'NOSCRIPT No matching script. Please use EVAL.';
                return scriptLoaded ? Promise.resolve([9,8,7]) : Promise.reject({ message: err });
            });
            expect.spyOn(redis, '_loadScripts').andCall(() => {
                scriptLoaded = true;
                return Promise.resolve();
            });
            let myArgs = [11,22,33];
            redis._lua('ASCRIPT', ...myArgs).then(res => {
                expect(res).toEqual([9,8,7]);
                expect(redis._loadScripts).toHaveBeenCalled();
                expect(redis._eval).toHaveBeenCalledWith('ASCRIPT', ...myArgs);
                expect(redis._eval.calls).toExist();
                expect(redis._eval.calls[1]).toExist();
                expect(redis._eval.calls[1].arguments).toEqual(['ASCRIPT', ...myArgs]);
            }).then(done);
        });

        it('should not reload script when failing', done => {
            redis._scripts = { 'ASCRIPT' : { sha: 'aaa', keys: 1 }};
            let scriptLoaded = false;
            expect.spyOn(redis, '_eval').andCall((name, ...args) => {
                return scriptLoaded ? Promise.resolve([9,8,7]) : Promise.reject({ message: 'general err' });
            });
            expect.spyOn(redis, '_loadScripts');
            let myArgs = [11,22,33];
            redis._lua('ASCRIPT', ...myArgs).catch(err => {
                expect(err).toEqual({ message: 'general err' });
                expect(redis._eval).toHaveBeenCalledWith('ASCRIPT', ...myArgs);
                expect(redis._loadScripts).toNotHaveBeenCalled();
            }).then(done);
        });
    });

    describe('_loadScripts', () => {
        it('should load scripts correctly', done => {
            let mockScripts = {
              'ascript' : { content: '-- KEYS[0] aa -- KEYS[1] aaa', keys: 2 },
              'bscript' : { content: '-- KEYS[0] bb -- KEYS[1] -- KEYS[2] bbb', keys: 3 }
            };
            redis._settings['scriptNames'] = ['ascript', 'bscript'];
            expect.spyOn(fs, 'readFileAsync').andCall(path => {
                let scriptName = path.match(/([^\/]+)\.lua/)[1];
                return Promise.resolve(mockScripts[scriptName]['content']);
            });
            expect.spyOn(redis._client, 'scriptAsync').andCall((load, content) => {
                return Promise.resolve(`hashOf:${content}`);
            });
            redis._loadScripts().then(() => {
                Object.keys(mockScripts).forEach(scriptName => {
                    let script = redis._scripts[scriptName.toUpperCase()];
                    expect(script).toExist();
                    expect(script['content']).toBe(mockScripts[scriptName]['content']);
                    expect(script['keys']).toBe(mockScripts[scriptName]['keys']);
                    expect(script['sha']).toBe(`hashOf:${mockScripts[scriptName]['content']}`);
                })
            }).then(() => {
                done();
            });
        });
    });

    describe('pullTask', () => {
        it('should scheduled task data', done => {
            let now = Date.now();
            expect.spyOn(redis, '_roundToTaskInterval').andReturn(now);
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve('taskData'));
            redis.pullTask().then(task => {
                expect(task).toBe('taskData');
                expect(redis._lua).toHaveBeenCalledWith('PULLTASK', redis.settings('indexQueueName'), now);
            }).then(done);
        });

        it('should return null since no tasks scheduled', done => {
            let now = Date.now();
            expect.spyOn(redis, '_roundToTaskInterval').andReturn(now);
            expect.spyOn(redis, '_lua').andReturn(Promise.resolve(false));
            redis.pullTask().then(task => {
                expect(task).toBe(null);
                expect(redis._lua).toHaveBeenCalledWith('PULLTASK', redis.settings('indexQueueName'), now);
            }).then(done);
        });
    });

});