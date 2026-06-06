-- reaper.lua — recover jobs abandoned by crashed or stalled workers.
--
-- A claimed job lives in the inflight set scored by its visibility deadline. If
-- the worker holding it dies, nothing acks or nacks it; the only thing that
-- rescues it is this scan. Every inflight entry whose deadline is at or before
-- now is moved back to ready for another worker to claim — the mechanism behind
-- Relay's at-least-once guarantee on crash.
--
-- Attempts are intentionally NOT bumped here: the next Claim counts the
-- redelivery, so a job that keeps timing out still marches toward the DLQ.
--
-- KEYS[1] = inflight set q:{name}:inflight (ZSET scored by deadline)
-- KEYS[2] = ready set    q:{name}:ready
-- ARGV[1] = now in unix milliseconds
-- ARGV[2] = job hash key prefix ("job:")
-- ARGV[3] = max jobs to requeue in this pass (bounds the work per call)
--
-- Returns the number of jobs requeued.

local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local limit = tonumber(ARGV[3])

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('HSET', prefix .. id, 'state', 'ready')
  redis.call('ZADD', KEYS[2], 0, id)
end

return #expired
