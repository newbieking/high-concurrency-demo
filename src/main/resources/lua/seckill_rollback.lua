-- 补偿回滚：把预扣的库存和购买资格还回去，用于「消息重投到上限仍未落库」的场景
-- 用 SREM 的返回值做幂等开关，重复执行不会把库存越加越多
-- KEYS[1]=库存  KEYS[2]=已购用户集合  KEYS[3]=在途ZSet  KEYS[4]=请求状态Hash
-- ARGV[1]=userId ARGV[2]=requestId ARGV[3]=nowMillis ARGV[4]=终态 ARGV[5]=请求TTL秒
local removed = redis.call('SREM', KEYS[2], ARGV[1])
if removed == 1 then
    redis.call('INCR', KEYS[1])
end
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('HSET', KEYS[4], 'state', ARGV[4], 'doneAt', ARGV[3])
redis.call('EXPIRE', KEYS[4], ARGV[5])
return removed
