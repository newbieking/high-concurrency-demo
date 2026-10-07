package com.example.demo.like;

import com.example.demo.common.DeadLetterDao;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Service
public class LikeConsumer implements MessageListenerConcurrently {

    private static final Logger log = LoggerFactory.getLogger(LikeConsumer.class);
    private static final int MAX_RECONSUME = 8;

    private final LikeWriter writer;
    private final LikeService likeService;
    private final DeadLetterDao deadLetterDao;

    public LikeConsumer(LikeWriter writer, LikeService likeService, DeadLetterDao deadLetterDao) {
        this.writer = writer;
        this.likeService = likeService;
        this.deadLetterDao = deadLetterDao;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        List<LikeMessage> parsed = new ArrayList<>(msgs.size());
        List<MessageExt> parsedRaw = new ArrayList<>(msgs.size());
        for (MessageExt raw : msgs) {
            String body = new String(raw.getBody(), StandardCharsets.UTF_8);
            LikeMessage message = LikeMessage.from(body);
            if (!message.valid()) {
                deadLetterDao.record("like", null, raw.getTopic(), raw.getMsgId(),
                        "malformed message", 0, body);
                continue;
            }
            parsed.add(message);
            parsedRaw.add(raw);
        }
        if (parsed.isEmpty()) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        try {
            writer.persist(parsed);
            parsed.forEach(m -> likeService.confirm(m.streamId()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception batchError) {
            // 整批一个事务失败时退化成逐条处理，避免一条坏消息拖住整批一直重投
            log.warn("like batch persist failed, falling back to per-message: {}", batchError.getMessage());
        }

        boolean needRetry = false;
        for (int i = 0; i < parsed.size(); i++) {
            LikeMessage message = parsed.get(i);
            MessageExt raw = parsedRaw.get(i);
            try {
                writer.persist(List.of(message));
                likeService.confirm(message.streamId());
            } catch (Exception e) {
                if (raw.getReconsumeTimes() >= MAX_RECONSUME) {
                    // 放弃重试：把消息摘出在途集合，剩下的交给对账任务用 Redis 计数修 DB
                    log.error("like event {} dropped after max retries", message.streamId(), e);
                    likeService.confirm(message.streamId());
                    deadLetterDao.record("like", message.streamId(), raw.getTopic(), raw.getMsgId(),
                            "max reconsumes reached: " + e.getMessage(), raw.getReconsumeTimes(),
                            new String(raw.getBody(), StandardCharsets.UTF_8));
                } else {
                    needRetry = true;
                }
            }
        }
        return needRetry ? ConsumeConcurrentlyStatus.RECONSUME_LATER : ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
