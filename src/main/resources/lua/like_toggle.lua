-- 点赞/取消：状态去重、计数、写缓冲、排行榜，一次原子完成
-- 状态没变化时不产生消息，天然幂等（重复点赞不会重复计数）
-- KEYS[1]=已点赞用户集合  KEYS[2]=计数  KEYS[3]=缓冲Stream  KEYS[4]=排行榜ZSet  KEYS[5]=在途集合
-- ARGV[1]=userId ARGV[2]=LIKE/UNLIKE ARGV[3]=nowMillis ARGV[4]=targetId
-- 返回全部为字符串，方便客户端直接反序列化成 List<String>：{changed, 最新计数, streamId}
local delta = 0
if ARGV[2] == 'LIKE' then
    if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
        delta = 1
    end
else
    if redis.call('SREM', KEYS[1], ARGV[1]) == 1 then
        delta = -1
    end
end

if delta == 0 then
    return {'0', '-1', ''}
end

local count = redis.call('INCRBY', KEYS[2], delta)
local streamId = redis.call('XADD', KEYS[3], '*',
        'targetId', ARGV[4],
        'userId', ARGV[1],
        'delta', tostring(delta),
        'ts', ARGV[3])
-- 在途集合记的是 streamId，消费者处理完按 id 摘除，重投不会重复摘
redis.call('SADD', KEYS[5], streamId)
-- 热度归零就从排行榜摘掉，否则取消点赞后榜单尾部会挂着一堆 0
-- 注意 ZINCRBY 返回的是 bulk string，必须先 tonumber 再比较，否则 Lua 直接报「string 与 number 无法比较」
local score = tonumber(redis.call('ZINCRBY', KEYS[4], delta, ARGV[4]))
if score <= 0 then
    redis.call('ZREM', KEYS[4], ARGV[4])
end

return {'1', tostring(count), streamId}
