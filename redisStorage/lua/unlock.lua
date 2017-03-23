-- Delete the key only if the key value matches ARGV[1]
-- KEYS[1] - the schedule lock key
-- ARGV[1] - the key's value, i.e. locker of key (so we don't allow non locker to unlock)

if redis.call("GET",KEYS[1]) == ARGV[1] then
    return redis.call("DEL",KEYS[1])
else
    return 0
end