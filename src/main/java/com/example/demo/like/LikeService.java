package com.example.demo.like;

import com.example.demo.common.BizException;
import com.example.demo.common.ResultCode;
import com.example.demo.config.AppProps;
import com.example.demo.infra.RedisBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class LikeService {

    private static final Logger log = LoggerFactory.getLogger(LikeService.class);

    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> likeToggleScript;
    private final RedisScript<Long> likeConfirmScript;
    private final RedisBuffer buffer;
    private final JdbcTemplate jdbc;
    private final AppProps props;

    public LikeService(StringRedisTemplate redis,
                       @SuppressWarnings("rawtypes") RedisScript<List> likeToggleScript,
                       RedisScript<Long> likeConfirmScript,
                       RedisBuffer buffer,
                       JdbcTemplate jdbc,
                       AppProps props) {
        this.redis = redis;
        this.likeToggleScript = likeToggleScript;
        this.likeConfirmScript = likeConfirmScript;
        this.buffer = buffer;
        this.jdbc = jdbc;
        this.props = props;
    }

    public LikeToggleView toggle(long targetId, long userId, boolean like) {
        if (Boolean.FALSE.equals(redis.hasKey(LikeKeys.count(targetId)))) {
            // 请求路径上的自动预热必须是「只补不覆盖」的，否则并发首访会把刚加上的点赞数盖回去
            ensureWarm(targetId);
        }
        List<String> keys = List.of(
                LikeKeys.users(targetId),
                LikeKeys.count(targetId),
                props.getLike().getStreamKey(),
                props.getLike().getRankKey(),
                LikeKeys.INFLIGHT);
        @SuppressWarnings("unchecked")
        List<String> result = redis.execute(likeToggleScript, keys,
                String.valueOf(userId),
                like ? "LIKE" : "UNLIKE",
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(targetId));
        if (result == null || result.size() < 3) {
            throw new BizException(ResultCode.INTERNAL_ERROR, "like lua returned " + result);
        }
        boolean changed = "1".equals(result.get(0));
        long count = Long.parseLong(result.get(1));
        return new LikeToggleView(changed, count, like);
    }

    /** 按 streamId 摘除在途记录，SREM 幂等，消息重投也不会摘错 */
    public void confirm(String streamId) {
        if (streamId == null || streamId.isEmpty()) {
            return;
        }
        redis.execute(likeConfirmScript, List.of(LikeKeys.INFLIGHT), streamId);
    }

    public Long redisCount(long targetId) {
        String v = redis.opsForValue().get(LikeKeys.count(targetId));
        return v == null ? null : Long.parseLong(v);
    }

    public boolean isLiked(long targetId, long userId) {
        return Boolean.TRUE.equals(redis.opsForSet().isMember(LikeKeys.users(targetId), String.valueOf(userId)));
    }

    public List<ZSetOperations.TypedTuple<String>> topRanked(int top) {
        Set<ZSetOperations.TypedTuple<String>> tuples =
                redis.opsForZSet().reverseRangeWithScores(props.getLike().getRankKey(), 0, Math.max(0, top - 1));
        return tuples == null ? List.of() : new ArrayList<>(tuples);
    }

    public long inFlightCount() {
        Long n = redis.opsForSet().size(LikeKeys.INFLIGHT);
        return n == null ? 0 : n;
    }

    public long streamLength() {
        return buffer.streamLength(props.getLike().getStreamKey());
    }

    public long streamPending() {
        return buffer.pendingCount(props.getLike().getStreamKey(), props.getLike().getConsumerGroup());
    }

    public boolean drained() {
        return inFlightCount() == 0 && streamLength() == 0 && streamPending() == 0;
    }

    public String rankKey() {
        return props.getLike().getRankKey();
    }

    /**
     * 压测复位。注意 stream / 在途集合 / 排行榜是跨 target 共享的，
     * 所以只能在系统空闲时调用，否则会把别的 target 的在途消息一起丢掉。
     */
    public void reset(long targetId) {
        jdbc.update("UPDATE like_counter SET like_count = 0 WHERE target_id = ?", targetId);
        jdbc.update("DELETE FROM like_record WHERE target_id = ?", targetId);
        jdbc.update("DELETE FROM idempotent_record WHERE biz_type = 'like'");
        jdbc.update("DELETE FROM dead_letter WHERE biz_type = 'like'");

        AppProps.Like cfg = props.getLike();
        redis.delete(List.of(LikeKeys.count(targetId), LikeKeys.users(targetId), LikeKeys.INFLIGHT,
                cfg.getStreamKey(), cfg.getRankKey()));
        buffer.ensureGroup(cfg.getStreamKey(), cfg.getConsumerGroup());
        rebuildRank();
        warmup(targetId);
        log.info("like target {} reset", targetId);
    }

    /**
     * 把 DB 里的计数和已点赞用户灌进 Redis（覆盖式）。
     * 用户集合是全量加载，对象很大时首次预热会慢，生产上应该换结构（分片集合 / Bitmap / Roaring）。
     * 覆盖式的只能在没有流量时调用，请求路径上用 {@link #ensureWarm}。
     */
    public void warmup(long targetId) {
        long count = dbLikeCount(targetId);
        redis.opsForValue().set(LikeKeys.count(targetId), String.valueOf(count));
        if (count > 0) {
            redis.opsForZSet().add(props.getLike().getRankKey(), String.valueOf(targetId), count);
        }
        int liked = loadLikedUsers(targetId);
        log.info("warmed up target {}: count={} likedUsers={}", targetId, count, liked);
    }

    /** 只补不覆盖：计数 key 不存在时才从 DB 灌，已点赞用户用 SADD 补（幂等），并发首访也安全 */
    public void ensureWarm(long targetId) {
        redis.opsForValue().setIfAbsent(LikeKeys.count(targetId), String.valueOf(dbLikeCount(targetId)));
        loadLikedUsers(targetId);
    }

    private long dbLikeCount(long targetId) {
        Long dbCount = jdbc.query("SELECT like_count FROM like_counter WHERE target_id = ?",
                rs -> rs.next() ? rs.getLong(1) : null, targetId);
        return dbCount == null ? 0 : dbCount;
    }

    private int loadLikedUsers(long targetId) {
        List<Long> userIds = jdbc.queryForList(
                "SELECT user_id FROM like_record WHERE target_id = ? AND liked = 1", Long.class, targetId);
        if (!userIds.isEmpty()) {
            String[] members = userIds.stream().map(String::valueOf).toArray(String[]::new);
            redis.opsForSet().add(LikeKeys.users(targetId), members);
        }
        return userIds.size();
    }

    /** 排行榜整表重建，用于 Redis 被清空后恢复 */
    public int rebuildRank() {
        List<Object[]> rows = jdbc.query("SELECT target_id, like_count FROM like_counter",
                (rs, i) -> new Object[]{rs.getLong(1), rs.getLong(2)});
        String rankKey = props.getLike().getRankKey();
        redis.delete(rankKey);
        for (Object[] row : rows) {
            double count = ((Number) row[1]).doubleValue();
            if (count > 0) {
                redis.opsForZSet().add(rankKey, String.valueOf(row[0]), count);
            }
        }
        log.info("rank rebuilt with {} targets", rows.size());
        return rows.size();
    }
}
