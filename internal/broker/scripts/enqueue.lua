-- enqueue.lua — write a job's hash and place it in its target set, atomically.
--
-- Go decides the target set (ready or delayed) and the score, then hands them in
-- with the flattened job hash. Doing the write in one script keeps it atomic
-- (the dedup gate added next slots in ahead of the write without a round-trip).
--
-- KEYS[1] = job hash key  job:{id}
-- KEYS[2] = target set    q:{name}:ready OR q:{name}:delayed
-- ARGV[1] = job id (ZADD member)
-- ARGV[2] = score (ready: priority composite; delayed: ready-at ms)
-- ARGV[3..] = job hash field/value pairs (flattened ToHash)
--
-- Returns 'ok'.

for i = 3, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 'ok'
