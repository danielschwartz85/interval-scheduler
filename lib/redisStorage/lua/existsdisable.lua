-- Check if task exists, if it exists then cancel it.
-- Return 1 if task existed and was canceld 0 o.w.
-- KEYS[1] - task info hash key name

local canceled = redis.call("HGET", KEYS[1], 'canceled')
if canceled == false or canceled == 'true' then
    return 0
elseif canceled == 'false' then
    redis.call("HSET", KEYS[1], 'canceled', 'true')
    return 1
end
