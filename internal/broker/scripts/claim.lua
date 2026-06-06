-- claim.lua — the atomic claim, the heart of Relay.
--
-- In one indivisible step it pops the most important ready job, marks it
-- in-flight under a visibility deadline, and bumps its attempt count. Because
-- the whole thing is one Redis script, two workers can never claim the same job:
-- ZPOPMAX removes the id before any other command can observe it. Splitting this
-- into separate round-trips would reintroduce the race this project exists to
-- solve, so all of it must stay here.
--
-- KEYS[1] = ready set    q:{name}:ready    (ZSET scored by priority)
-- KEYS[2] = inflight set q:{name}:inflight (ZSET scored by visibility deadline)
-- ARGV[1] = now in unix milliseconds (passed in so the script is deterministic)
-- ARGV[2] = visibility timeout in milliseconds
-- ARGV[3] = job hash key prefix ("job:") so we can find the popped job's hash
--
-- Returns the claimed job's hash as a flat HGETALL array, or a Redis nil reply
-- when the ready set is empty (nothing to claim).

-- Pop the highest-scored (highest-priority) member. ZPOPMAX returns
-- {member, score} or {} when the set is empty.
local popped = redis.call('ZPOPMAX', KEYS[1])
if #popped == 0 then
  return nil
end

local id = popped[1]
local deadline = tonumber(ARGV[1]) + tonumber(ARGV[2])

-- Place it in the inflight set scored by its deadline; the reaper later scans
-- this set for entries whose deadline has passed and requeues them.
redis.call('ZADD', KEYS[2], deadline, id)

-- Update the job hash: count the delivery attempt and record the new state.
local job_key = ARGV[3] .. id
redis.call('HINCRBY', job_key, 'attempts', 1)
redis.call('HSET', job_key, 'state', 'inflight')

return redis.call('HGETALL', job_key)
