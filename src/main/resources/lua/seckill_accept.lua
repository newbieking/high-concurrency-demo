-- 秒杀受理：库存校验、限购去重、在途限流、预扣库存、写缓冲、记在途台账，全部一次原子完成
-- KEYS[1]=库存  KEYS[2]=已购用户集合  KEYS[3]=缓冲Stream  KEYS[4]=在途ZSet  KEYS[5]=请求状态Hash
-- ARGV[1]=userId ARGV[2]=requestId ARGV[3]=nowMillis ARGV[4]=在途上限 ARGV[5]=请求TTL秒 ARGV[6]=activityId
-- 返回：1=受理成功 -1=未预热 -2=已售罄 -3=重复购买 -4=在途过多
local stock = redis.call('GET', KEYS[1])
if not stock then
    return -1
end

-- 请求号级幂等：同一个 requestId 无论被提交多少次都只有第一次能受理
if redis.call('EXISTS', KEYS[5]) == 1 then
    return -3
end

if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return -3
end

if tonumber(stock) <= 0 then
    return -2
end

-- 在途超过上限就快速失败，宁可拒绝也不要让队列无限堆积
if redis.call('ZCARD', KEYS[4]) >= tonumber(ARGV[4]) then
    return -4
end

redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])

local streamId = redis.call('XADD', KEYS[3], '*',
        'activityId', ARGV[6],
        'userId', ARGV[1],
        'requestId', ARGV[2],
        'ts', ARGV[3])

redis.call('ZADD', KEYS[4], ARGV[3], ARGV[2])
redis.call('HSET', KEYS[5],
        'state', 'PENDING',
        'activityId', ARGV[6],
        'userId', ARGV[1],
        'streamId', streamId,
        'ts', ARGV[3],
        'retry', '0')
redis.call('EXPIRE', KEYS[5], ARGV[5])

return 1
