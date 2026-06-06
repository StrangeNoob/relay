-- heartbeat.lua — extend the visibility deadline of an in-flight job.
--
-- A long-running handler calls this periodically so the reaper does not reclaim
-- work that is still making progress. It only extends a job that is currently in
-- flight: if the job has already been acked or reaped, ZADD with the XX flag is
-- a no-op, so we never resurrect it into the inflight set.
--
-- KEYS[1] = inflight set q:{name}:inflight
-- ARGV[1] = job id
-- ARGV[2] = now in unix milliseconds
-- ARGV[3] = visibility timeout in milliseconds
--
-- Returns 1 if the deadline was extended, 0 if the job was not in flight.

local id = ARGV[1]
local deadline = tonumber(ARGV[2]) + tonumber(ARGV[3])
-- XX: only update an existing member; CH: return the count actually changed.
return redis.call('ZADD', KEYS[1], 'XX', 'CH', deadline, id)
