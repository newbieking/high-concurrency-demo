package com.example.demo.infra;

import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 把 Redis 缓冲里的消息搬到 MQ。
 * 顺序是「先发 MQ，成功了再 XACK」，失败就把消息留在缓冲里等重投，
 * 因此任何一步崩溃都不会丢消息，代价是可能出现重复投递。
 */
public class StreamDispatcher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StreamDispatcher.class);

    /** 超过这个空闲时间还没被 ACK 的消息，判定为卡住，重新投递 */
    private static final Duration RECLAIM_MIN_IDLE = Duration.ofSeconds(5);
    private static final int RECLAIM_EVERY_LOOPS = 25;

    private final String bizName;
    private final String streamKey;
    private final String consumerGroup;
    private final String topic;
    private final int threads;
    private final int batchSize;
    private final long pollMs;
    private final BufferMapper mapper;
    private final RedisBuffer buffer;
    private final MqSender sender;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService pool;

    public StreamDispatcher(String bizName, String streamKey, String consumerGroup, String topic,
                            int threads, int batchSize, long pollMs, BufferMapper mapper,
                            RedisBuffer buffer, MqSender sender) {
        this.bizName = bizName;
        this.streamKey = streamKey;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.threads = threads;
        this.batchSize = batchSize;
        this.pollMs = pollMs;
        this.mapper = mapper;
        this.buffer = buffer;
        this.sender = sender;
    }

    @Override
    public void start() {
        buffer.ensureGroup(streamKey, consumerGroup);
        running.set(true);
        pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, bizName + "-dispatcher");
            t.setDaemon(true);
            return t;
        });
        String instance = UUID.randomUUID().toString().substring(0, 8);
        for (int i = 0; i < threads; i++) {
            final String consumerName = bizName + "-" + instance + "-" + i;
            pool.submit(() -> loop(consumerName));
        }
        log.info("[{}] dispatcher started, stream={} group={} topic={} threads={}",
                bizName, streamKey, consumerGroup, topic, threads);
    }

    private void loop(String consumerName) {
        int loops = 0;
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, String, String>> records =
                        buffer.readNew(streamKey, consumerGroup, consumerName, batchSize);
                if (records.isEmpty()) {
                    if (++loops % RECLAIM_EVERY_LOOPS == 0) {
                        reclaim(consumerName);
                    } else {
                        sleep();
                    }
                    continue;
                }
                dispatch(records);
            } catch (Exception e) {
                // MQ 或 Redis 抖动：消息留在 PEL，等下一轮重投，不丢
                log.warn("[{}] dispatch failed, will retry later: {}", bizName, e.getMessage());
                sleep();
            }
        }
    }

    private void dispatch(List<MapRecord<String, String, String>> records) throws Exception {
        List<Message> messages = new ArrayList<>(records.size());
        List<RecordId> ids = new ArrayList<>(records.size());
        for (MapRecord<String, String, String> record : records) {
            String streamId = record.getId().getValue();
            String body = mapper.toBody(streamId, record.getValue());
            Message message = new Message(topic, body.getBytes(StandardCharsets.UTF_8));
            message.setKeys(streamId);
            messages.add(message);
            ids.add(record.getId());
        }
        sender.sendBatch(messages);
        buffer.ackAndDelete(streamKey, consumerGroup, ids);
    }

    private void reclaim(String consumerName) {
        List<MapRecord<String, String, String>> stale =
                buffer.claimStale(streamKey, consumerGroup, consumerName, RECLAIM_MIN_IDLE, batchSize);
        if (stale.isEmpty()) {
            sleep();
            return;
        }
        log.warn("[{}] reclaim {} stale message(s) by {}", bizName, stale.size(), consumerName);
        try {
            dispatch(stale);
        } catch (Exception e) {
            log.warn("[{}] reclaim dispatch failed: {}", bizName, e.getMessage());
            sleep();
        }
    }

    private void sleep() {
        try {
            Thread.sleep(pollMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
        running.set(false);
        if (pool != null) {
            pool.shutdown();
            try {
                pool.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("[{}] dispatcher stopped", bizName);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}
