package com.example.demo.seckill;

import com.example.demo.common.BizException;
import com.example.demo.common.ResultCode;
import com.example.demo.config.AppProps;
import com.example.demo.infra.RedisBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SeckillService {

    private static final Logger log = LoggerFactory.getLogger(SeckillService.class);

    private static final long ACCEPTED = 1L;
    private static final long NOT_WARMED = -1L;
    private static final long SOLD_OUT = -2L;
    private static final long DUPLICATE = -3L;
    private static final long TOO_BUSY = -4L;

    private final StringRedisTemplate redis;
    private final RedisScript<Long> acceptScript;
    private final RedisScript<Long> confirmScript;
    private final RedisScript<Long> rollbackScript;
    private final RedisBuffer buffer;
    private final JdbcTemplate jdbc;
    private final AppProps props;

    public SeckillService(StringRedisTemplate redis,
                          RedisScript<Long> seckillAcceptScript,
                          RedisScript<Long> seckillConfirmScript,
                          RedisScript<Long> seckillRollbackScript,
                          RedisBuffer buffer,
                          JdbcTemplate jdbc,
                          AppProps props) {
        this.redis = redis;
        this.acceptScript = seckillAcceptScript;
        this.confirmScript = seckillConfirmScript;
        this.rollbackScript = seckillRollbackScript;
        this.buffer = buffer;
        this.jdbc = jdbc;
        this.props = props;
    }

    public SeckillAcceptView accept(long activityId, long userId, String requestId) {
        Long result = doAccept(activityId, userId, requestId);
        if (result == NOT_WARMED) {
            // 首次访问自动把 DB 真值灌进 Redis，省掉一次手工预热
            ensureWarm(activityId);
            result = doAccept(activityId, userId, requestId);
        }
        if (result == null) {
            throw new BizException(ResultCode.INTERNAL_ERROR, "lua script returned null");
        }
        if (result == ACCEPTED) {
            Long remain = redisStock(activityId);
            return new SeckillAcceptView(requestId, "PENDING", remain == null ? 0 : remain);
        }
        if (result == SOLD_OUT) {
            throw new BizException(ResultCode.SECKILL_SOLD_OUT);
        }
        if (result == DUPLICATE) {
            throw new BizException(ResultCode.SECKILL_DUPLICATE);
        }
        if (result == TOO_BUSY) {
            throw new BizException(ResultCode.SECKILL_BUSY);
        }
        throw new BizException(ResultCode.SECKILL_NOT_READY);
    }

    private Long doAccept(long activityId, long userId, String requestId) {
        AppProps.Seckill cfg = props.getSeckill();
        List<String> keys = List.of(
                SeckillKeys.stock(activityId),
                SeckillKeys.buyers(activityId),
                cfg.getStreamKey(),
                SeckillKeys.pending(activityId),
                SeckillKeys.request(activityId, requestId));
        return redis.execute(acceptScript, keys,
                String.valueOf(userId),
                requestId,
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(cfg.getMaxInFlight()),
                String.valueOf(cfg.getRequestTtlSeconds()),
                String.valueOf(activityId));
    }

    /** 落库成功后记终态、摘在途台账；ZREM 幂等，重复消费重复调用都安全 */
    public void confirm(long activityId, String requestId) {
        AppProps.Seckill cfg = props.getSeckill();
        List<String> keys = List.of(
                SeckillKeys.pending(activityId),
                SeckillKeys.request(activityId, requestId));
        redis.execute(confirmScript, keys,
                requestId,
                String.valueOf(System.currentTimeMillis()),
                "CONFIRMED",
                String.valueOf(cfg.getRequestTtlSeconds()));
    }

    /** 最终失败时把预扣的库存和购买资格还回去 */
    public void rollback(long activityId, long userId, String requestId) {
        AppProps.Seckill cfg = props.getSeckill();
        List<String> keys = List.of(
                SeckillKeys.stock(activityId),
                SeckillKeys.buyers(activityId),
                SeckillKeys.pending(activityId),
                SeckillKeys.request(activityId, requestId));
        redis.execute(rollbackScript, keys,
                String.valueOf(userId),
                requestId,
                String.valueOf(System.currentTimeMillis()),
                "FAILED",
                String.valueOf(cfg.getRequestTtlSeconds()));
    }

    public SeckillResultView getResult(long activityId, String requestId) {
        Map<Object, Object> hash = redis.opsForHash().entries(SeckillKeys.request(activityId, requestId));
        if (hash.isEmpty()) {
            throw new BizException(ResultCode.SECKILL_NOT_FOUND);
        }
        return new SeckillResultView(
                requestId,
                str(hash.get("state"), "PENDING"),
                parseLong(hash.get("activityId")),
                parseLong(hash.get("userId")),
                parseLong(hash.get("ts")),
                parseLong(hash.get("doneAt")),
                (int) parseLong(hash.get("retry")));
    }

    public Long redisStock(long activityId) {
        String v = redis.opsForValue().get(SeckillKeys.stock(activityId));
        return v == null ? null : Long.parseLong(v);
    }

    public long inFlightCount(long activityId) {
        Long n = redis.opsForZSet().zCard(SeckillKeys.pending(activityId));
        return n == null ? 0 : n;
    }

    public long buyerCount(long activityId) {
        Long n = redis.opsForSet().size(SeckillKeys.buyers(activityId));
        return n == null ? 0 : n;
    }

    public long streamLength() {
        return buffer.streamLength(props.getSeckill().getStreamKey());
    }

    public long streamPending() {
        return buffer.pendingCount(props.getSeckill().getStreamKey(), props.getSeckill().getConsumerGroup());
    }

    /**
     * 强制用 DB 里的可用库存覆盖 Redis。只在活动空闲时调用：它会清掉已购集合和在途台账，
     * 活动正在跑的时候调用会放同一批用户重复下单。
     */
    public void warmup(long activityId) {
        int stock = dbStock(activityId);
        redis.opsForValue().set(SeckillKeys.stock(activityId), String.valueOf(stock));
        redis.delete(List.of(SeckillKeys.buyers(activityId), SeckillKeys.pending(activityId)));
        log.info("warmed up activity {} with stock {}", activityId, stock);
    }

    /**
     * 只补不覆盖：库存 key 不存在时才从 DB 灌一次，且绝不清理已购集合和在途台账。
     * 请求路径上的自动预热必须用它，否则并发首访会互相清掉去重状态，同一用户被重复受理。
     */
    public void ensureWarm(long activityId) {
        int stock = dbStock(activityId);
        redis.opsForValue().setIfAbsent(SeckillKeys.stock(activityId), String.valueOf(stock));
    }

    private int dbStock(long activityId) {
        Integer stock = jdbc.queryForObject(
                "SELECT stock_available FROM seckill_activity WHERE id = ?", Integer.class, activityId);
        if (stock == null) {
            throw new BizException(ResultCode.SECKILL_NOT_READY, "activity not found: " + activityId);
        }
        return stock;
    }

    /** 压测复位：清掉 Redis 侧全部痕迹和 DB 侧的订单/幂等/死信数据 */
    public void reset(long activityId) {
        AppProps.Seckill cfg = props.getSeckill();
        jdbc.update("UPDATE seckill_activity SET stock_available = stock_total WHERE id = ?", activityId);
        jdbc.update("DELETE FROM seckill_order WHERE activity_id = ?", activityId);
        jdbc.update("DELETE FROM idempotent_record WHERE biz_type = 'seckill'");
        jdbc.update("DELETE FROM dead_letter WHERE biz_type = 'seckill'");

        List<String> keys = new ArrayList<>();
        keys.add(SeckillKeys.stock(activityId));
        keys.add(SeckillKeys.buyers(activityId));
        keys.add(SeckillKeys.pending(activityId));
        keys.add(cfg.getStreamKey());
        redis.delete(keys);

        String prefix = "sk:{" + activityId + "}:req:";
        try (var cursor = redis.scan(org.springframework.data.redis.core.ScanOptions.scanOptions()
                .match(prefix + "*").count(500).build())) {
            while (cursor.hasNext()) {
                redis.delete(cursor.next());
            }
        }

        buffer.ensureGroup(cfg.getStreamKey(), cfg.getConsumerGroup());
        // 复位后立刻把库存灌好，避免首批并发请求各自走「未预热」分支
        warmup(activityId);
        log.info("activity {} reset: redis keys cleared, orders truncated", activityId);
    }

    private static String str(Object o, String defaultValue) {
        return o == null ? defaultValue : o.toString();
    }

    private static long parseLong(Object o) {
        if (o == null) {
            return 0L;
        }
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
