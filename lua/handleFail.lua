-- PURPOSE:
--   1) HINCRBY 'attemptsKey' <msgID + ":count"> => newCount
--   2) If newCount > MaxReclaimAttempts => do DLQ (optional), ack+del, remove attempts fields => return "exceeded"
--   3) Else => remain in PEL => set <msgID + ":ts"> = now, and <msgID + ":nextBackoffSec"> => some calc => return "failed"
--
-- KEYS[1] => attemptsKey (hash) e.g. "redstream:reclaim_attempts:myGroup:myStream"
-- KEYS[2] => streamName
-- KEYS[3] => groupName
-- ARGV[1] => msgID, e.g. "1234-0"
-- ARGV[2] => maxReclaimAttempts (integer)
-- ARGV[3] => nowSec (string integer)
-- ARGV[4] => lockExpirySec or reclaimInterval? Used for backoff calculation if you want. Up to you.
-- ARGV[5] => newCount was not known beforehand, we compute inside script. (But we can do partial if we want.)
--   Actually, we can store it after computing. Or pass ReclaimIntervalSec. We'll assume a simpler approach.

-- We'll do partial approach: we do HINCRBY here, then check newCount vs ARGV[2].
-- If exceed => XACK+XDEL => HDEL => return "exceeded"
-- else => remain => HSET => return "failed"

local countKey = ARGV[1] .. ":count"
local tsKey = ARGV[1] .. ":ts"
local backoffKey = ARGV[1] .. ":nextBackoffSec"

local newCount = redis.call("HINCRBY", KEYS[1], countKey, 1)

if newCount > tonumber(ARGV[2]) then
  -- exceed => ack+del => remove
  redis.call("XACK", KEYS[2], KEYS[3], ARGV[1])
  redis.call("XDEL", KEYS[2], ARGV[1])
  redis.call("HDEL", KEYS[1],
    ARGV[1],
    countKey,
    tsKey,
    backoffKey
  )
  return "exceeded"
else
  -- remain => set ts => ARGV[3], compute backoff if you want (2^(newCount))
  local nowSec = tonumber(ARGV[3])
  redis.call("HSET", KEYS[1], tsKey, nowSec)

  -- Just do an exponential factor. We'll do 2^newCount, but clamp it manually if we want.
  -- Suppose we do no clamp here, or we can pass a ReclaimMaxExp=ARGV[4], etc.
  local factor = 1
  for i=2,newCount,1 do
    factor = factor * 2
  end
  -- if factor> ReclaimMax => factor= ReclaimMax, etc...
  -- let's keep it simpler:
  local nextBackoffSec = nowSec + factor * 5  -- e.g. base=5
  redis.call("HSET", KEYS[1], backoffKey, nextBackoffSec)

  return tostring(newCount)
end
