const expect = require('expect');
const Redis = require('../lib/redisStorage/redisClient');

/*
 * 'lua' is connecting to actual local redis if one exists
 */
describe('lua', () => {

    let redis;

    before(done => {
        redis = new Redis({ host: 'localhost', port: '6379', db: 10 });
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

        it('should return 2 for existing key (with cancel true) un cancel it, update its data and interval', done => {
            redis._client.hmsetAsync('myKey', 'canceled', true, 'data', 'someValue', 'interval', 456).then(() => {
                return redis._lua('EXISTSUPDATE', 'myKey', 'updatedData', 789);
            }).then(exists => {
                expect(exists).toBe(2);
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

        it('should return 2 for existing key (with cancel true) un cancel it and update its data but not its interval', done => {
            redis._client.hmsetAsync('myKey', 'canceled', true, 'data', 'someValue', 'interval', 456).then(() => {
                return redis._lua('EXISTSUPDATE', 'myKey', 'updatedData');
            }).then(exists => {
                expect(exists).toBe(2);
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

        it('should return 1 for existing key (with cancel false) and cancel it', done => {
            redis._client.hmsetAsync('myKey', 'canceled', false, 'otherMember', 'someValue').then(() => {
                return redis._lua('EXISTSDISABLE', 'myKey');
            }).then(enabled => {
                expect(enabled).toBe(1);
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
            }).then(([task, queueSize, indexQueueNextTask, taskInfoExists]) => {
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
            }).then(([task, queueSize, indexQueueNextTask, taskInfoExists, taskInfo2Exists]) => {
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
            }).then(([task, queueSize, indexQueueNextTask, taskInfoExists, nextScheduledTask]) => {
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
            }).then(([task, queueSize, indexQueueNextTask, taskInfoExists]) => {
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
            }).then(([task, queueSize, indexQueueNextTask, taskInfoExists, taskInfo2Exists, nextScheduledTask]) => {
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