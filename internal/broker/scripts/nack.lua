-- nack.lua — handle a failed delivery.
--
-- Always removes the job from the inflight set, then decides its fate from the
-- attempt count already recorded on the job hash (claim bumps it): if attempts
-- are still under the retry budget the job is requeued to ready for another try;
-- otherwise it is moved to the dead-letter queue. Reading the counts from the
-- hash here (rather than trusting values passed in) keeps the decision atomic
-- with the move.
--
-- KEYS[1] = inflight set q:{name}:inflight
-- KEYS[2] = ready set    q:{name}:ready
-- KEYS[3] = dead-letter  q:{name}:dlq
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
--
-- Returns 'retry' or 'dead' to describe what happened.

local id = ARGV[1]
local job_key = ARGV[2] .. id

redis.call('ZREM', KEYS[1], id)

local attempts = tonumber(redis.call('HGET', job_key, 'attempts')) or 0
local max_retries = tonumber(redis.call('HGET', job_key, 'max_retries')) or 0

if attempts < max_retries then
  redis.call('HSET', job_key, 'state', 'ready')
  redis.call('ZADD', KEYS[2], 0, id)
  return 'retry'
end

redis.call('HSET', job_key, 'state', 'dead')
redis.call('RPUSH', KEYS[3], id)
return 'dead'
