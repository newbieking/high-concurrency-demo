package com.example.demo.infra;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MqSender {

    private static final Logger log = LoggerFactory.getLogger(MqSender.class);

    private final DefaultMQProducer producer;

    public MqSender(DefaultMQProducer producer) {
        this.producer = producer;
    }

    /**
     * 批量同步发送。整批成功才算成功，调用方据此决定要不要 XACK。
     * 失败时抛异常，缓冲区里的消息会留在 PEL 里，由重投任务兜底。
     */
    public void sendBatch(List<Message> messages) throws Exception {
        if (messages.isEmpty()) {
            return;
        }
        producer.send(messages);
    }

    public void sendOne(Message message) throws Exception {
        producer.send(message);
    }

    public void start() throws Exception {
        producer.start();
    }

    public void shutdown() {
        try {
            producer.shutdown();
        } catch (Exception e) {
            log.warn("producer shutdown failed", e);
        }
    }
}
