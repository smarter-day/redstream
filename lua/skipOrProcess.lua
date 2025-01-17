-- PURPOSE:
--   1) Check if "already processed" in ZSET => if found => XACK + XDEL => return "skip"
--   2) If not found => XACK + XDEL + ZADD => return "processed"
--
-- KEYS[1] => processedZsetKey, e.g. "redstream:processed:myGroup:myStream"
-- KEYS[2] => streamName
-- KEYS[3] => groupName
-- ARGV[1] => uniqueID used in ZSET, e.g. "myGroup|myStream|1234-0"
-- ARGV[2] => Score (string float of current time)
-- ARGV[3] => The actual Redis message ID, e.g. "1234-0"

local found = redis.call("ZSCORE", KEYS[1], ARGV[1])
if found then
    -- already processed => skip => XACK + XDEL
    redis.call("XACK", KEYS[2], KEYS[3], ARGV[3])
    redis.call("XDEL", KEYS[2], ARGV[3])
    return "skip"
else
    -- not found => first success => XACK+XDEL + ZADD
    redis.call("XACK", KEYS[2], KEYS[3], ARGV[3])
    redis.call("XDEL", KEYS[2], ARGV[3])
    redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])
    return "processed"
end
