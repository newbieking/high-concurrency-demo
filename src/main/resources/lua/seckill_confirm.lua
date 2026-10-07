-- 记终态 + 摘除在途台账。ZREM 幂等，重复调用不会把计数搞乱
-- KEYS[1]=在途ZSet  KEYS[2]=请求状态Hash
-- ARGV[1]=requestId（ZREM 的成员，必须是请求号） ARGV[2]=nowMillis ARGV[3]=终态 ARGV[4]=请求TTL秒
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'state', ARGV[3], 'doneAt', ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return 1
