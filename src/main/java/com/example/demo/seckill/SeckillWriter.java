package com.example.demo.seckill;

import com.example.demo.common.BizException;
import com.example.demo.common.ResultCode;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SeckillWriter {

    private final JdbcTemplate jdbc;

    public SeckillWriter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 幂等落库：幂等表唯一索引是主闸，DB 库存条件更新是防超卖的兜底闸。
     * 任一步失败整个事务回滚，消息会被 MQ 重投。
     *
     * @return true 表示这次真的写入了；false 表示之前已经处理过
     */
    @Transactional
    public boolean persist(SeckillMessage message) {
        try {
            jdbc.update("INSERT INTO idempotent_record(biz_type, biz_id) VALUES ('seckill', ?)",
                    message.requestId());
        } catch (DuplicateKeyException e) {
            return false;
        }

        // 兜底防线：即使 Redis 判重/扣减出了问题，这里也不会把库存扣成负数
        int rows = jdbc.update("""
                UPDATE seckill_activity SET stock_available = stock_available - 1
                WHERE id = ? AND stock_available > 0
                """, message.activityId());
        if (rows == 0) {
            throw new BizException(ResultCode.SECKILL_SOLD_OUT, "db stock guard rejected");
        }

        jdbc.update("""
                INSERT INTO seckill_order(request_id, activity_id, user_id, status)
                VALUES (?, ?, ?, 'SUCCESS')
                """, message.requestId(), message.activityId(), message.userId());
        return true;
    }

    public boolean alreadyPersisted(String requestId) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM idempotent_record WHERE biz_type = 'seckill' AND biz_id = ?",
                Integer.class, requestId);
        return n != null && n > 0;
    }
}
