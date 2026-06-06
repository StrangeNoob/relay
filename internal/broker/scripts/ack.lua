-- ack.lua — acknowledge that a job was processed successfully.
--
-- KEYS[1] = inflight set q:{name}:inflight
-- ARGV[1] = job id
-- ARGV[2] = job hash key prefix ("job:")

local id = ARGV[1]
redis.call('ZREM', KEYS[1], id)
redis.call('DEL', ARGV[2] .. id)
return 1
