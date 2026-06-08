-- nack.lua — handle a failed delivery.
--
-- Always removes the job from the inflight set, then decides its fate from the
-- attempt count on the job hash (claim bumps it): retries left -> requeue to the
-- delayed set at a caller-computed ready-at (the backoff), so the retry waits;
-- budget spent -> move to the dead-letter queue and INCR the dead counter so
-- the dashboard can show a cluster-wide total without scanning the DLQ list.
-- Reading the counts from the hash here keeps the decision atomic with the move.
--
-- KEYS[1] = inflight set q:{name}:inflight
-- KEYS[2] = delayed set  q:{name}:delayed
-- KEYS[3] = dead-letter  q:{name}:dlq
-- KEYS[4] = dead counter q:{name}:dead (cluster-wide; INCR only when dead-lettered)
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = retry ready-at in unix milliseconds (precomputed backoff)
--
-- Returns 'retry' or 'dead'.

local id = ARGV[1]
local job_key = ARGV[2] .. id
local ready_at = tonumber(ARGV[3])

redis.call('ZREM', KEYS[1], id)

local attempts = tonumber(redis.call('HGET', job_key, 'attempts')) or 0
local max_retries = tonumber(redis.call('HGET', job_key, 'max_retries')) or 0

if attempts < max_retries then
  redis.call('HSET', job_key, 'state', 'delayed')
  redis.call('ZADD', KEYS[2], ready_at, id)
  return 'retry'
end

redis.call('HSET', job_key, 'state', 'dead')
redis.call('RPUSH', KEYS[3], id)
redis.call('INCR', KEYS[4])
return 'dead'
