package com.example.demo.like;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class LikeWriter {

    private final JdbcTemplate jdbc;

    public LikeWriter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 批量落库：明细按 seq 做 LWW upsert，计数按 target 聚合后一次更新。
     * 用 idempotent_record 按 streamId 去重，只有「第一次处理」的事件才计入增量，
     * 这样消息重投不会把计数算两遍。
     *
     * @return 真正生效的事件数
     */
    @Transactional
    public int persist(List<LikeMessage> messages) {
        Map<Long, Long> appliedDelta = new HashMap<>();
        int applied = 0;

        for (LikeMessage message : messages) {
            try {
                jdbc.update("INSERT INTO idempotent_record(biz_type, biz_id) VALUES ('like', ?)",
                        message.streamId());
            } catch (DuplicateKeyException e) {
                continue;
            }
            jdbc.update("""
                    INSERT INTO like_record(target_id, user_id, seq, liked) VALUES (?, ?, ?, ?) AS n
                    ON DUPLICATE KEY UPDATE
                        liked = IF(n.seq > like_record.seq, n.liked, like_record.liked),
                        seq = GREATEST(like_record.seq, n.seq)
                    """, message.targetId(), message.userId(), message.seq(), message.delta() > 0 ? 1 : 0);
            appliedDelta.merge(message.targetId(), (long) message.delta(), Long::sum);
            applied++;
        }

        for (Map.Entry<Long, Long> entry : appliedDelta.entrySet()) {
            jdbc.update("""
                    INSERT INTO like_counter(target_id, like_count) VALUES (?, ?) AS n
                    ON DUPLICATE KEY UPDATE like_count = like_counter.like_count + n.like_count
                    """, entry.getKey(), entry.getValue());
        }
        return applied;
    }
}
