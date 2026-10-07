package com.example.demo.seckill;

import com.example.demo.common.DeadLetterDao;
import com.example.demo.config.AppProps;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
public class SeckillConsumer implements MessageListenerConcurrently {

    private static final Logger log = LoggerFactory.getLogger(SeckillConsumer.class);

    private final SeckillWriter writer;
    private final SeckillService seckillService;
    private final DeadLetterDao deadLetterDao;
    private final AppProps props;

    public SeckillConsumer(SeckillWriter writer, SeckillService seckillService,
                           DeadLetterDao deadLetterDao, AppProps props) {
        this.writer = writer;
        this.seckillService = seckillService;
        this.deadLetterDao = deadLetterDao;
        this.props = props;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        boolean needRetry = false;
        for (MessageExt raw : msgs) {
            String body = new String(raw.getBody(), StandardCharsets.UTF_8);
            try {
                SeckillMessage message = SeckillMessage.from(body);
                if (!message.valid()) {
                    deadLetter(raw, body, "malformed message", 0);
                    continue;
                }
                boolean inserted = writer.persist(message);
                // 即使这次是重复消息（inserted=false），也要补一次 confirm：
                // 上一次可能在「落库成功但确认丢失」之间崩溃了，confirm 本身幂等。
                seckillService.confirm(message.activityId(), message.requestId());
                if (!inserted) {
                    log.debug("duplicate message skipped: {}", message.requestId());
                }
            } catch (Exception e) {
                if (raw.getReconsumeTimes() >= props.getSeckill().getMaxRedeliver()) {
                    terminate(raw, body, e);
                } else {
                    needRetry = true;
                    log.warn("seckill consume failed (retry {}): {}", raw.getReconsumeTimes(), e.getMessage());
                }
            }
        }
        // 返回 RECONSUME_LATER 会让整批重投。因为落库是幂等的，整批重投是安全的
        return needRetry ? ConsumeConcurrentlyStatus.RECONSUME_LATER : ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /** 重投到上限仍然失败：记死信 + 把 Redis 预扣的库存和购买资格还回去，避免库存被永久占住 */
    private void terminate(MessageExt raw, String body, Exception cause) {
        log.error("seckill message exhausted retries, rolling back: {}", body, cause);
        SeckillMessage message = SeckillMessage.from(body);
        if (message.valid()) {
            try {
                seckillService.rollback(message.activityId(), message.userId(), message.requestId());
            } catch (Exception e) {
                log.error("rollback failed for {}", message.requestId(), e);
            }
        }
        deadLetter(raw, body, "max redeliver reached: " + cause.getMessage(), raw.getReconsumeTimes());
    }

    private void deadLetter(MessageExt raw, String body, String reason, int retryCount) {
        try {
            SeckillMessage message = SeckillMessage.from(body);
            deadLetterDao.record("seckill", message.requestId(), raw.getTopic(), raw.getMsgId(),
                    reason, retryCount, body);
        } catch (Exception e) {
            log.error("failed to record dead letter: {}", body, e);
        }
    }
}
