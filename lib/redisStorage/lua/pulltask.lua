-- KEYS[1] - main index queue name
-- ARGV[1] - pull tasks only up to this priority (now epoch)

local nexttasktime = redis.call("ZRANGE", KEYS[1], 0, 0)[1]
if nexttasktime == nil then
    return 0
else
    local now = tonumber(ARGV[1])
    -- all time queus are in the future, nothing to perform
    if tonumber(nexttasktime) > now then
        return 0
    else
        local taskid = redis.call("RPOP", nexttasktime)
        -- this time queue is empty, remove it from the main index queue
        if redis.call("LLEN", nexttasktime) == 0 then
            redis.call("ZREM", KEYS[1], nexttasktime)
        end

        local taskinfo = redis.call("HMGET", taskid, 'data', 'canceled', 'interval')
        -- canceled task, delete it's info.
        if taskinfo[2] == 'true' then
            redis.call("DEL", taskid)
            return 0
        else
            -- intervaled task, reschedule.
            if taskinfo[3] ~= false then
                local nextrun = tonumber(taskinfo[3]) + now
                redis.call("LPUSH", nextrun, taskid)
                redis.call("ZADD", KEYS[1], nextrun, nextrun)
            else
                redis.call("DEL", taskid)
            end
            return taskinfo[1]
        end
    end
end

