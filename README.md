<div align="center"><img src="https://capriza.github.io/images/logos/logos-scheduler.svg" height="128" /></div>
<div align="center"><div>Dynamically schedule interval tasks</div></div>

# interval-scheduler
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

    /*
     * Serialize task to string
     * return whatever, scheduler will call your handler with this string.
     */
    serialize() {
        return JSON.stringify({ userId: this.userId, taskType: 'MyTask' });
    }

    /*
     * Return task interval, on task execution the task would reschedule to
     * this interval. If not defined the task would execute once.
     */
    get onExecuteRescheduleTo() {
        return 60; // run this task every minute.
    }

    /*
     * First execution time (unix epoch).
     */
    get executeOn() {
        return Date.now(); // first execution is as fast as possible.
    }

    /*
     * Task id to globaly identify the task
     */
    get id() {
        `user:${userId}`
    }
}
```
## Usage - connecting
```
scheduler.connect().then(() => {
    // can now assign, remove and execute tasks
});
```

## Assigning tasks
```
scheduler.assignTask(new MyTaskObject(1));
```
This assigns the task and returns true or updates an existing task and returns false
## Start Pulling tasks
```
scheduler.startTaskExecute(myTaskExecutor); // scheduler should be connectd here

let myTaskExecutor = (serializedTask) => {
    // 'serializedTask' is what task.serialize() returns.
    let myTask = JSON.parse(serializedTask);
    console.log('performing task..');
    console.log(`user: ${myTask.userId}`);
    console.log(`type: ${myTask.taskType}`);
};
```

## Removing tasks
```
let taskId = 1;
scheduler.removeTask(taskId);
```
Return true if task was removed
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
 });
```

## Clearing All tasks
Implemented with Redis [scan](https://redis.io/commands/scan) command,
Note this limitation from Redis documentation
>The SCAN algorithm is guaranteed to terminate only if the size of the iterated collection remains bounded to a given maximum size, otherwise iterating a collection that always grows may result into SCAN to never terminate a full iteration.
```
scheduler.clearAllTasks().then(removedTasksCount => {
    console.log(`removed ${removedTasksCount} tasks`);
});
```
*Under current implementation tasks are really cleared from Redis when the task is pulled 
from the queue (execution time), this means that if a task is rescheduled after being removed 
then the task's first execution would be the old execution time.*

## Checking if task is scheduled
```
scheduler.isScheduled(taskId).then(taskData => {
    if (taskData) {
        console.log(`task ${JSON.stringify(taskData)} is scheduled`); // task {"userId":1,"taskType":"MyTask"} is scheduled
    } else {
        console.log(`task ${taskId} is not scheduled`); 
    }
});
```
*Not atomic, returns task if it is scheduled at the time of execution*


## Events
Event | info | args
---------| --------| ------
online | Scheduler is online and can accept tasks, If one of the non-master redises is down then his tasks could not be performed or removed but the scheduler would still accept tasks and perform them)|  
offline | Scheduler is offline, the scheduler cannot accept remove or preform tasks |
db-reconnect | A redis client has been re-connected | String of redis host:port
db-disconnect | A redis client has been disconnected | String of redis host:port


## Configuring scheduler
```
let options = {
    storage : {
        instances: [
            { host: 'localhost', port: '6379', db: 0 },
            { host: 'localhost', port: '8888', db: 0 }
        ],
        masterIndex : 0,
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
storage.instances | Redis instances option array |
storage.instances[].host | Redis host url | 'localhost'
storage.instances[].port | Redis host port | '6379'
storage.masterIndex | Keep scheduling internal and external locks and metadata on this Redis instance, this Redis would take up most memory for the scheduling process. | 0 (first instance)


## Important notes and limitations
- When setting the same task (by task id) twice the task is simply updated, the task interval and meta data are updated but **only after the task executes**. For example if the task interval is updated to 10 minutes when it was 1 minute then the task would execute in 1 minute and then executed again every 10 minutes.
- The scheduler task execution can be switched on and off by startTaskExecution() and stopTaskExecution().
    - This means that the scheduler can have be assigned with tasks while it is not executing them, and so when the scheduler returns to execution mode the task's first execution time 'executeOn' may have already passed.
    - In this case the scheduler would start executing the tasks in the order of their execution time (i.e. past tasks first), this would got on until there are no more tasks to perform (but future tasks). The scheduler would sleep and wait until more tasks should be performed.
    - This scenario can also occur when the task load is too big / slow and so the scheduler **lags** behind and would preform past tasks first (untill it reaches a balanced state in which it has preformed all past and present tasks).


## Performance
Method | Time | info
---------| --------| ------
assignTask | O(Log(N)) | N is task bucket size (if M tasks should perform at K times then N = K)
removeTask | O(1) | 
when executing tasks each task peek | O(1) | This is the operation that occurs every 'checkTasksEverySeconds' seconds 
clearAllTasks | O(N) | while N is the number of tasks 
isScheduled | O(1) |
taskSize | O(N) | where N is the number of redis keys
queueSize | O(Log(N)) | where N is task bucket size

