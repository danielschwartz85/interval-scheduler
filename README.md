# interval-scheduler
Dynamically schedule interval tasks
 - Support for Redis partitioning (can use multiple Redis DBs)
 - Stateless (If scheduler process crashes and then brought back to life 
   then task status remains valid in Redis, i.e. tasks are guaranteed to be either set or unset).
 - Multiple schedulers can operate at the same time and assign the same tasks (same task can be scheduled twice, the second task is ignored). 

## Install with
```bash
> npm install interval-scheduler
```

## Usage - Implement A Task object
``` 
const Task = require('interval-scheduler').Task;

class MyTaskObject extends Task {

    constructor(userId) {
        super();
        this.userId = userId;
    }

    // Serialize task to string
    // return whatever, scheduler will call your handler with this string.
    serialize() {
        return JSON.stringify({ userId: this.userId, taskType: 'MyTask' });
    }

    // Return task interval, on task execution the task would reschedule to 
    // this interval. If not defined the task would execute once.
    get onExecuteRescheduleTo() {
        return 60; // run this task every minute.
    }

    // First execution time (unix epoch).
    get executeOn() {
        return Date.now(); // first execution is as fast as possible.
    }

    // Task id to globaly identify the task
    get id() {
        `user:${userId}`
    }
}
```

## Usage - Assigning tasks
```
const Scheduler = require('interval-scheduler').Scheduler;
let scheduler = new Scheduler();

scheduler.startTaskAccept().then(() => {
    scheduler.assignTask(new MyTaskObject(1));
    scheduler.assignTask(new MyTaskObject(2));
});
```

## Removing tasks
```
scheduler.startTaskAccept().then(() => {
    scheduler.removeTask(new MyTaskObject(1));
    scheduler.removeTask(new MyTaskObject(2));
});
```

## Usage - Start Pulling tasks
```
scheduler.startTaskExecute(myTaskExecutor);

let myTaskExecutor = (serializedTask) => {
    // 'serializedTask' is what task.serialize() returns.
    let myTask = JSON.parse(serializedTask);
    console.log('performing task..');
    console.log(`user: ${myTask.userId}`);
    console.log(`type: ${myTask.taskType}`);
};
```


## Locking task execution (auto released)
```
scheduler.startTaskExecute(myExecutor);

let myExecutor = (serializedTask) => {
    let task = JSON.parse(serializedTask);
    scheduler.tryAutoLock(task.id, 60).then(lockAquired => {
    if (lockAquired) {
        // perform task ..
        // lock for this id would be automatically released in 60 seconds
    } else {
        // ignore this task ..
    }
};
```

## Locking task execution (manually released)
```
scheduler.startTaskExecute(myTaskExecutor);

let myTaskExecutor = (serializedTask) => {
    let task = JSON.parse(serializedTask);
    scheduler.tryLock(task.id).then(lock => {
    if (lock) {
        // perform task ..
        // unlock in 90 seconds
        setTimeout(() => {
            scheduler.unlock(lock);
            // lock would be released in 24 hours if not manullay released
        }, 90);
    } else {
        // ignore this task ..
    }
}
```

## Stopping task execution
```
scheduler.startTaskExecute(myExecutorFunction);
scheduler.stopTaskExecute().then(() => {
     // myExecutorFunction will no longer be called
     scheduler.executingTasks; // false
     scheduler.assignTask(new MyTaskObject(1)); // OK - tasks are still accepted
 });
```

## Stopping task accept
```
scheduler.startTaskExecute(myExecutorFunction);
scheduler.stopTaskAccept().then(() => {
    // myExecutorFunction is still called  
    scheduler.acceptingTasks; // false
    scheduler.assignTask(new MyTaskObject(1)) // Thorws - 'not accepting tasks'
});
```

## Clearing All tasks
Implemented with Redis [scan](https://redis.io/commands/scan) command,
Note this limitation from Redis documentation
>The SCAN algorithm is guaranteed to terminate only if the size of the iterated collection remains bounded to a given maximum size, otherwise iterating a collection that always grows may result into SCAN to never terminate a full iteration.
```
scheduler.clearAllTasks();
```

## Configuring scheduler
```
let options = {
    storage : {
        instances: [
            { host: 'localhost', port: '6379', db: 0 },
            { host: 'localhost', port: '8888', db: 0 }
        ],
        masterIndex : 0,
        reconnectIntervalSeconds : 5,
        taskIntervalSeconds : 60
    },
    checkTasksEverySeconds : 60
};
let myLogger = new MyLogger();
let scheduler = new Scheduler(options, myLogger);
```
###### options:
Name | option | Default
------------ | ------------- | -------
checkTasksEverySeconds | In task execution mode scheduler would wakeup every 'checkTasksEverySeconds' to check tasks. | 10 seconds
storage | Storage options object | 
storage.taskIntervalSeconds | Tasks execution time is rounded to this interval, for example if this value is set to 60 seconds then tasks would be scheduled to 1 minute interval. | 60 seconds
storage.reconnectIntervalSeconds | On redis disconnect event try reconnecting every 'reconnectIntervalSeconds'. | 1 second
storage.instances | Redis instances option array |
storage.instances[].host | Redis host url | 'localhost'
storage.instances[].port | Redis host port | '6379'
storage.masterIndex | Keep scheduling internal and external locks and metadata on this Redis instance, this Redis would take up most memory for the scheduling process. | 0 (first instance)

## Limitations
- When setting the same task (by task id) twice the task is simply updated, the task interval and meta data are updated but **only after the task executes**. For example if the task interval is updated to 10 minutes when it was 1 minute then the task would execute in 1 minute and then executed again every 10 minutes.

## Performance
Method | Time | info
---------| --------| ------
assignTask | O(Log(N)) | N is task bucket size (if M tasks should perform at K times then N = K)
removeTask | O(1) | 
when executing tasks each task peek | O(1) | This is the operation that occurs every 'checkTasksEverySeconds' seconds 
clearAllTasks | O(N) | while N is the number of tasks (but done in iterations to protect redis CPU) 

