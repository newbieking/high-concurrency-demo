package com.example.demo.like;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 点赞对账：比对 Redis 计数和 DB 计数，不一致且已排干在途消息时，以 Redis 为准修 DB。
 * 之所以以 Redis 为准，是因为 Redis 才是承接流量的准入点（SADD/INCR 原子完成），
 * DB 只是它的持久化投影，丢了可以用 Redis 重放出来。
 */
@Component
public class LikeReconcileJob {

    private static final Logger log = LoggerFactory.getLogger(LikeReconcileJob.class);

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final LikeService likeService;

    public LikeReconcileJob(StringRedisTemplate redis, JdbcTemplate jdbc, LikeService likeService) {
        this.redis = redis;
        this.jdbc = jdbc;
        this.likeService = likeService;
    }

    @Scheduled(fixedDelayString = "${demo.like.reconcile-interval-ms:10000}")
    public void scheduled() {
        try {
            rebuildRankIfMissing();
        } catch (Exception e) {
            log.error("rank rebuild failed", e);
        }
        try {
            LikeConsistency result = checkConsistency();
            if (!result.pass()) {
                log.warn("like consistency not satisfied: drained={} diffs={} violations={}",
                        result.drained(), result.diffs(), result.violations());
            }
        } catch (Exception e) {
            log.error("like consistency check failed", e);
        }
    }

    private void rebuildRankIfMissing() {
        String rankKey = likeService.rankKey();
        if (Boolean.FALSE.equals(redis.hasKey(rankKey))) {
            log.warn("rank key {} missing, rebuilding from db", rankKey);
            likeService.rebuildRank();
        }
    }

    public LikeConsistency checkConsistency() {
        boolean drained = likeService.drained();
        long inFlight = likeService.inFlightCount();
        long streamLength = likeService.streamLength();
        long streamPending = likeService.streamPending();

        List<LikeConsistency.TargetDiff> diffs = new ArrayList<>();
        List<Long> targets = jdbc.queryForList("SELECT target_id FROM like_counter", Long.class);
        for (Long targetId : targets) {
            Long redisCount = likeService.redisCount(targetId);
            if (redisCount == null) {
                // Redis 里根本没这个 target 的计数，说明没预热过，不能拿 0 去覆盖 DB
                continue;
            }
            Long dbCount = jdbc.queryForObject(
                    "SELECT like_count FROM like_counter WHERE target_id = ?", Long.class, targetId);
            if (dbCount != null && dbCount.equals(redisCount)) {
                continue;
            }
            diffs.add(new LikeConsistency.TargetDiff(targetId, redisCount, dbCount == null ? -1 : dbCount));
        }

        List<String> violations = new ArrayList<>();
        if (drained && !diffs.isEmpty()) {
            for (LikeConsistency.TargetDiff diff : diffs) {
                violations.add("计数不一致：target=" + diff.targetId()
                        + " redis=" + diff.redisCount() + " db=" + diff.dbCount());
                fixFromRedis(diff);
            }
        }
        return new LikeConsistency(drained, drained && violations.isEmpty(),
                inFlight, streamLength, streamPending, diffs, violations);
    }

    private void fixFromRedis(LikeConsistency.TargetDiff diff) {
        jdbc.update("""
                INSERT INTO like_counter(target_id, like_count) VALUES (?, ?) AS n
                ON DUPLICATE KEY UPDATE like_count = n.like_count
                """, diff.targetId(), diff.redisCount());
        jdbc.update("""
                INSERT INTO reconcile_diff(biz_type, biz_key, redis_value, db_value, diff, action, detail)
                VALUES ('like', ?, ?, ?, ?, 'FIXED_FROM_REDIS', ?)
                """, String.valueOf(diff.targetId()), diff.redisCount(), diff.dbCount(),
                diff.redisCount() - diff.dbCount(), "like counter repaired");
        log.warn("like counter repaired from redis: target={} {} -> {}",
                diff.targetId(), diff.dbCount(), diff.redisCount());
    }
}
