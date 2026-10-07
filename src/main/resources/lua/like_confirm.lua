-- 按 streamId 摘除在途集合，SREM 幂等
-- KEYS[1]=在途集合
-- ARGV[1]=streamId
return redis.call('SREM', KEYS[1], ARGV[1])
