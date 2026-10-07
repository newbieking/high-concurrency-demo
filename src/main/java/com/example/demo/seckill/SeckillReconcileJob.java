package com.example.demo.seckill;

import com.example.demo.common.DeadLetterDao;
import com.example.demo.config.AppProps;
import com.example.demo.infra.MqSender;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 对账与补偿。三件事：
 * 1) 在途超时的请求重投 MQ（覆盖「MQ 丢消息」这条唯一靠缓冲兜不住的路径）；
 * 2) 重投到上限还没落库的，回滚 Redis 预扣并记死信；
 * 3) 比对 Redis 预扣数与 DB 订单数，不一致就告警留痕。
 */
@Component
public class SeckillReconcileJob {

    private static final Logger log = LoggerFactory.getLogger(SeckillReconcileJob.class);
    private static final int BATCH = 200;

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final SeckillService seckillService;
    private final SeckillWriter seckillWriter;
    private final MqSender mqSender;
    private final DeadLetterDao deadLetterDao;
    private final AppProps props;

    public SeckillReconcileJob(StringRedisTemplate redis, JdbcTemplate jdbc, SeckillService seckillService,
                               SeckillWriter seckillWriter, MqSender mqSender, DeadLetterDao deadLetterDao,
                               AppProps props) {
        this.redis = redis;
        this.jdbc = jdbc;
        this.seckillService = seckillService;
        this.seckillWriter = seckillWriter;
        this.mqSender = mqSender;
        this.deadLetterDao = deadLetterDao;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${demo.seckill.reconcile-interval-ms:5000}")
    public void scheduled() {
        List<Long> activityIds = jdbc.queryForList("SELECT id FROM seckill_activity", Long.class);
        for (Long activityId : activityIds) {
            try {
                redeliverStuck(activityId);
            } catch (Exception e) {
                log.error("redeliver failed for activity {}", activityId, e);
            }
            try {
                SeckillConsistency result = checkConsistency(activityId);
                if (!result.pass()) {
                    log.warn("activity {} consistency not satisfied: drained={} violations={}",
                            activityId, result.drained(), result.violations());
                }
            } catch (Exception e) {
                log.error("consistency check failed for activity {}", activityId, e);
            }
        }
    }

    void redeliverStuck(long activityId) throws Exception {
        AppProps.Seckill cfg = props.getSeckill();
        long deadline = System.currentTimeMillis() - cfg.getInFlightTimeoutMs();
        Set<String> stuck = redis.opsForZSet()
                .rangeByScore(SeckillKeys.pending(activityId), Double.NEGATIVE_INFINITY, deadline, 0, BATCH);
        if (stuck == null || stuck.isEmpty()) {
            return;
        }
        for (String requestId : stuck) {
            handleStuck(activityId, requestId, cfg);
        }
    }

    private void handleStuck(long activityId, String requestId, AppProps.Seckill cfg) throws Exception {
        String pendingKey = SeckillKeys.pending(activityId);
        String requestKey = SeckillKeys.request(activityId, requestId);
        Map<Object, Object> hash = redis.opsForHash().entries(requestKey);
        if (hash.isEmpty()) {
            // 状态记录已经过期，台账里的残留直接清掉
            redis.opsForZSet().remove(pendingKey, requestId);
            return;
        }
        String state = String.valueOf(hash.get("state"));
        if (!"PENDING".equals(state)) {
            redis.opsForZSet().remove(pendingKey, requestId);
            return;
        }
        // 已经落库了，只是确认那一步丢了，补一次确认就行，不用重投
        if (seckillWriter.alreadyPersisted(requestId)) {
            log.warn("request {} already persisted but still in flight, confirming", requestId);
            seckillService.confirm(activityId, requestId);
            return;
        }

        int retry = parseInt(hash.get("retry"));
        long userId = parseLong(hash.get("userId"));
        if (retry >= cfg.getMaxRedeliver()) {
            log.error("request {} exceeded max redeliver {}, rolling back", requestId, cfg.getMaxRedeliver());
            seckillService.rollback(activityId, userId, requestId);
            String body = new SeckillMessage(activityId, userId, requestId, parseLong(hash.get("ts"))).toBody();
            deadLetterDao.record("seckill", requestId, cfg.getTopic(), null,
                    "in-flight timeout after " + retry + " redeliveries", retry, body);
            recordDiff(activityId, "REDELIVER", 1, 0, 1, "request " + requestId + " rolled back after timeout");
            return;
        }

        redis.opsForHash().put(requestKey, "retry", String.valueOf(retry + 1));
        String body = new SeckillMessage(activityId, userId, requestId, parseLong(hash.get("ts"))).toBody();
        mqSender.sendOne(new Message(cfg.getTopic(), body.getBytes(StandardCharsets.UTF_8)));
        log.warn("request {} redelivered to MQ, attempt {}", requestId, retry + 1);
    }

    public SeckillConsistency checkConsistency(long activityId) {
        Map<String, Object> activity = jdbc.queryForMap(
                "SELECT stock_total, stock_available FROM seckill_activity WHERE id = ?", activityId);
        long stockTotal = ((Number) activity.get("stock_total")).longValue();
        long dbStock = ((Number) activity.get("stock_available")).longValue();
        Long dbOrders = jdbc.queryForObject("""
                SELECT COUNT(*) FROM seckill_order WHERE activity_id = ? AND status = 'SUCCESS'
                """, Long.class, activityId);
        long orders = dbOrders == null ? 0 : dbOrders;

        Long redisStock = seckillService.redisStock(activityId);
        long inFlight = seckillService.inFlightCount(activityId);
        long streamLength = seckillService.streamLength();
        long streamPending = seckillService.streamPending();
        long preDeducted = redisStock == null ? 0 : stockTotal - redisStock;

        List<String> violations = new ArrayList<>();
        boolean drained = inFlight == 0 && streamLength == 0 && streamPending == 0;
        if (redisStock == null) {
            violations.add("Redis 未预热：库存 key 不存在");
        } else if (drained) {
            if (redisStock != dbStock) {
                violations.add("库存不一致：redis=" + redisStock + " db=" + dbStock);
            }
            if (preDeducted != orders) {
                violations.add("预扣数与订单数不一致：preDeducted=" + preDeducted + " dbOrders=" + orders);
            }
        }
        if (dbStock < 0) {
            violations.add("DB 库存为负，出现超卖");
        }
        boolean pass = drained && violations.isEmpty();
        return new SeckillConsistency(drained, pass, stockTotal, redisStock == null ? -1 : redisStock,
                preDeducted, dbStock, orders, inFlight, streamLength, streamPending, violations);
    }

    private void recordDiff(long activityId, String action, long redisValue, long dbValue, long diff, String detail) {
        try {
            jdbc.update("""
                    INSERT INTO reconcile_diff(biz_type, biz_key, redis_value, db_value, diff, action, detail)
                    VALUES ('seckill', ?, ?, ?, ?, ?, ?)
                    """, String.valueOf(activityId), redisValue, dbValue, diff, action, detail);
        } catch (Exception e) {
            log.warn("record diff failed", e);
        }
    }

    private static int parseInt(Object o) {
        return (int) parseLong(o);
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
