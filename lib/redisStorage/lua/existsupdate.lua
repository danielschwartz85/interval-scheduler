-- Check if task exists, if it exists then update it and un cancel.
-- Return 1 if task existed and was updated 2 if task was canceld and updated 0 o.w.
-- KEYS[1] - task info hash key name
-- ARGV[1] - task data
-- ARGV[2] - optional task interval

local canceled = redis.call("HGET", KEYS[1], 'canceled')
-- task doesn not exist
if canceled == false then
    return 0
-- task is canceled
elseif canceled == 'true' then
    if ARGV[2] then
        redis.call("HMSET", KEYS[1], 'canceled', 'false', 'data', ARGV[1], 'interval', ARGV[2])
    else
        redis.call("HMSET", KEYS[1], 'canceled', 'false', 'data', ARGV[1])
    end
    return 2
-- task exist and is not canceled
else
    if ARGV[2] then
        redis.call("HMSET", KEYS[1], 'data', ARGV[1], 'interval', ARGV[2])
    else
        redis.call("HMSET", KEYS[1], 'data', ARGV[1])
    end
    return 1
end
