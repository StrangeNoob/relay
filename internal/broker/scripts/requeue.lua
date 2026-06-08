-- requeue.lua — move a dead-lettered job back into ready for another run.
--
-- An operator action: a job that exhausted its retry budget is given a fresh
-- start. The remove-from-dlq and add-to-ready must be one atomic step so the job
-- can never be in both or neither. attempts is reset to 0 so the job gets a full
-- retry budget again; the ready score is rebuilt from the job's priority exactly
-- like promote.lua/reaper.lua do, so priority ordering is preserved.
--
-- KEYS[1] = dlq list   q:{name}:dlq
-- KEYS[2] = ready set   q:{name}:ready (ZSET scored by priority)
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = now in unix milliseconds
-- ARGV[4] = priority scale (composite ready-score multiplier)
--
-- Returns 1 if the job was requeued, 0 if it was not present in the DLQ.

local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
  return 0
end

local job_key = ARGV[2] .. ARGV[1]
local priority = tonumber(redis.call('HGET', job_key, 'priority')) or 0
redis.call('HSET', job_key, 'state', 'ready', 'attempts', 0)

local score = priority * tonumber(ARGV[4]) - tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], score, ARGV[1])
return 1
