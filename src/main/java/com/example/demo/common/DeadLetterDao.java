package com.example.demo.common;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class DeadLetterDao {

    private final JdbcTemplate jdbc;

    public DeadLetterDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(String bizType, String bizId, String topic, String msgId,
                       String reason, int retryCount, String body) {
        jdbc.update("""
                INSERT INTO dead_letter(biz_type, biz_id, topic, msg_id, reason, retry_count, body)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, bizType, bizId, topic, msgId,
                reason == null ? null : reason.substring(0, Math.min(reason.length(), 250)),
                retryCount, body);
    }

    public List<Map<String, Object>> recent(String bizType, int limit) {
        return jdbc.queryForList("""
                SELECT id, biz_id, topic, msg_id, reason, retry_count, created_at
                FROM dead_letter WHERE biz_type = ? ORDER BY id DESC LIMIT ?
                """, bizType, limit);
    }

    public int count(String bizType) {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM dead_letter WHERE biz_type = ?", Integer.class, bizType);
        return n == null ? 0 : n;
    }
}
