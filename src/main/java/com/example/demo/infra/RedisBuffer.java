package com.example.demo.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Redis Stream 作为「本地可靠缓冲」的读写封装。
 * 关键点是 XACK 之前消息一直留在 PEL 里，进程崩溃后可以被别的消费者认领重投，
 * 这样「Redis 预扣成功但 MQ 投递失败」不会丢数据。
 */
@Component
public class RedisBuffer {

    private static final Logger log = LoggerFactory.getLogger(RedisBuffer.class);

    private final StringRedisTemplate redis;

    public RedisBuffer(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private StreamOperations<String, String, String> ops() {
        return redis.opsForStream();
    }

    public void ensureGroup(String streamKey, String group) {
        if (Boolean.FALSE.equals(redis.hasKey(streamKey))) {
            RecordId seed = ops().add(streamKey, Map.of("_init", "1"));
            ops().delete(streamKey, seed);
        }
        try {
            ops().createGroup(streamKey, group);
        } catch (Exception e) {
            if (!isBusyGroup(e)) {
                throw e;
            }
        }
    }

    private boolean isBusyGroup(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("BUSYGROUP")) {
                return true;
            }
        }
        return false;
    }

    /** 非阻塞读取新消息。用轮询而非 XREAD BLOCK，避免阻塞命令占用专用连接。 */
    public List<MapRecord<String, String, String>> readNew(String streamKey, String group, String consumer, int count) {
        List<MapRecord<String, String, String>> records = ops().read(
                Consumer.from(group, consumer),
                StreamReadOptions.empty().count(count),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
        return records == null ? List.of() : records;
    }

    /**
     * 认领「投递后长时间没被 ACK」的消息。覆盖两种情况：
     * 1) 派发进程发完 MQ 但没来得及 ACK 就挂了；
     * 2) 上一轮 MQ 发送失败，消息还挂在 PEL 里等着重试。
     * 重投会让 MQ 里出现重复消息，靠消费端幂等兜住。
     */
    public List<MapRecord<String, String, String>> claimStale(String streamKey, String group, String consumer,
                                                              Duration minIdle, int count) {
        PendingMessages pending = ops().pending(streamKey, group, Range.unbounded(), count);
        List<RecordId> ids = new ArrayList<>();
        for (PendingMessage pm : pending) {
            if (pm.getElapsedTimeSinceLastDelivery().compareTo(minIdle) >= 0) {
                ids.add(pm.getId());
            }
        }
        if (ids.isEmpty()) {
            return List.of();
        }
        List<MapRecord<String, String, String>> claimed =
                ops().claim(streamKey, group, consumer, minIdle, ids.toArray(new RecordId[0]));
        return claimed == null ? List.of() : claimed;
    }

    public void ackAndDelete(String streamKey, String group, List<RecordId> ids) {
        if (ids.isEmpty()) {
            return;
        }
        RecordId[] arr = ids.toArray(new RecordId[0]);
        ops().acknowledge(streamKey, group, arr);
        // 顺手把已确认的条目从 stream 里删掉，否则压测时 stream 会无限膨胀
        ops().delete(streamKey, arr);
    }

    public long streamLength(String streamKey) {
        Long size = ops().size(streamKey);
        return size == null ? 0 : size;
    }

    public long pendingCount(String streamKey, String group) {
        try {
            PendingMessagesSummary summary = ops().pending(streamKey, group);
            return summary == null ? 0 : summary.getTotalPendingMessages();
        } catch (Exception e) {
            log.debug("xpending {} {} failed: {}", streamKey, group, e.getMessage());
            return 0;
        }
    }
}
