-- ack.lua — acknowledge that a job was processed successfully.
--
-- KEYS[1] = inflight set      q:{name}:inflight
-- KEYS[2] = processed counter q:{name}:processed (cluster-wide throughput counter)
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")

local id = ARGV[1]
redis.call('ZREM', KEYS[1], id)
redis.call('DEL', ARGV[2] .. id)
redis.call('INCR', KEYS[2])
return 1
