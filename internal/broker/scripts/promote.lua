-- promote.lua — release delayed jobs whose ready-at time has arrived.
--
-- The mirror image of reaper.lua: instead of recovering past-due inflight jobs,
-- it moves due delayed jobs (scheduled jobs and backoff retries) into ready so a
-- worker can claim them. Bounded per call so a large backlog cannot block Redis.
--
-- Attempts are intentionally NOT bumped here: promotion is not a delivery
-- attempt. The next Claim counts the redelivery, so a job that keeps failing
-- still marches toward the DLQ.
--
-- KEYS[1] = delayed set q:{name}:delayed (ZSET scored by ready-at)
-- KEYS[2] = ready set    q:{name}:ready
-- ARGV[1] = now in unix milliseconds
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = max jobs to promote in this pass
-- ARGV[4] = priority scale (weights priority above time in the ready score)
--
-- Returns the number of jobs promoted.

local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local limit = tonumber(ARGV[3])
local scale = tonumber(ARGV[4])

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
for _, id in ipairs(due) do
  local job_key = prefix .. id
  redis.call('ZREM', KEYS[1], id)
  redis.call('HSET', job_key, 'state', 'ready')
  local priority = tonumber(redis.call('HGET', job_key, 'priority')) or 0
  -- 'now' is the promotion time: FIFO within a priority is by readiness time.
  redis.call('ZADD', KEYS[2], priority * scale - now, id)
end

return #due
