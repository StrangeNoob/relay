-- claim.lua — the atomic claim, the heart of Relay.
--
-- In one indivisible step it (optionally) checks a per-queue rate-limit token
-- bucket, pops the most important ready job, marks it in-flight under a
-- visibility deadline, and bumps its attempt count. Because the whole thing is
-- one Redis script, two workers can never claim the same job or over-issue
-- rate-limit tokens. Splitting any of it into separate round-trips would
-- reintroduce the races this project exists to solve, so all of it stays here.
--
-- KEYS[1] = ready set     q:{name}:ready    (ZSET scored by priority)
-- KEYS[2] = inflight set  q:{name}:inflight (ZSET scored by visibility deadline)
-- KEYS[3] = ratelimit     q:{name}:ratelimit (hash tokens/ts; unused when ARGV[6]='0')
-- ARGV[1] = now in unix milliseconds (passed in so the script is deterministic)
-- ARGV[2] = visibility timeout in milliseconds
-- ARGV[3] = job hash key prefix ("job:") so we can find the popped job's hash
-- ARGV[4] = rate limit: tokens per second
-- ARGV[5] = rate limit: burst (bucket capacity)
-- ARGV[6] = rate limiting enabled, '1' or '0'
--
-- Returns the claimed job's hash as a flat HGETALL array, or a Redis nil reply
-- when the ready set is empty OR the queue is rate-limited.

local now = tonumber(ARGV[1])

-- Optional token-bucket gate. A token is consumed only when a job is actually
-- popped (below), so empty-queue polls and denied claims never drain the bucket
-- and cannot starve real work.
local tokens
if ARGV[6] == '1' then
  local rate = tonumber(ARGV[4])
  local burst = tonumber(ARGV[5])
  local data = redis.call('HMGET', KEYS[3], 'tokens', 'ts')
  tokens = tonumber(data[1]) or burst
  local ts = tonumber(data[2]) or now
  tokens = math.min(burst, tokens + (now - ts) / 1000 * rate)
  if tokens < 1 then
    return nil -- rate-limited: do not pop; leave the bucket so time keeps accruing
  end
end

-- Pop the highest-scored (highest-priority) member. ZPOPMAX returns
-- {member, score} or {} when the set is empty.
local popped = redis.call('ZPOPMAX', KEYS[1])
if #popped == 0 then
  return nil -- empty queue: bucket left untouched (no token wasted)
end

local id = popped[1]

-- A job is being delivered: consume one token now (only here).
if ARGV[6] == '1' then
  redis.call('HSET', KEYS[3], 'tokens', tokens - 1, 'ts', now)
end

local deadline = now + tonumber(ARGV[2])

-- Place it in the inflight set scored by its deadline; the reaper later scans
-- this set for entries whose deadline has passed and requeues them.
redis.call('ZADD', KEYS[2], deadline, id)

-- Update the job hash: count the delivery attempt and record the new state.
local job_key = ARGV[3] .. id
redis.call('HINCRBY', job_key, 'attempts', 1)
redis.call('HSET', job_key, 'state', 'inflight')

return redis.call('HGETALL', job_key)
