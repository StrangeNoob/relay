-- enqueue.lua — write a job's hash and place it in its target set, atomically,
-- with an optional idempotency-key dedup gate.
--
-- Go decides the target set (ready or delayed), the score, and whether to dedup,
-- then hands them in with the flattened job hash. The dedup claim and the write
-- are one script, so there is no crash window between claiming the key and
-- writing the job, and concurrent same-key enqueues are serialized by Redis.
--
-- KEYS[1] = job hash key  job:{id}
-- KEYS[2] = target set    q:{name}:ready OR q:{name}:delayed
-- KEYS[3] = dedup key     q:{name}:dedup:{key}   (unused when useDedup = '0')
-- ARGV[1] = job id (ZADD member + dedup marker value)
-- ARGV[2] = score (ready: priority composite; delayed: ready-at ms)
-- ARGV[3] = dedup TTL in seconds
-- ARGV[4] = useDedup, '1' to dedup or '0' to skip
-- ARGV[5..] = job hash field/value pairs (flattened ToHash)
--
-- Returns 'ok' if enqueued, 'dup' if dropped as a duplicate.

if ARGV[4] == '1' then
  if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'EX', tonumber(ARGV[3])) == false then
    return 'dup'
  end
end

for i = 5, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 'ok'
