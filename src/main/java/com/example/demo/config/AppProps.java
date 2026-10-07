package com.example.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "demo")
public class AppProps {

    @NestedConfigurationProperty
    private Mq mq = new Mq();
    @NestedConfigurationProperty
    private Seckill seckill = new Seckill();
    @NestedConfigurationProperty
    private Like like = new Like();

    public Mq getMq() {
        return mq;
    }

    public void setMq(Mq mq) {
        this.mq = mq;
    }

    public Seckill getSeckill() {
        return seckill;
    }

    public void setSeckill(Seckill seckill) {
        this.seckill = seckill;
    }

    public Like getLike() {
        return like;
    }

    public void setLike(Like like) {
        this.like = like;
    }

    public static class Mq {
        private String namesrvAddr = "127.0.0.1:9876";
        private String producerGroup = "demo1-producer";
        private int sendTimeoutMs = 3000;
        private int retryTimesWhenSendFailed = 2;
        private int consumerThreads = 32;

        public String getNamesrvAddr() {
            return namesrvAddr;
        }

        public void setNamesrvAddr(String namesrvAddr) {
            this.namesrvAddr = namesrvAddr;
        }

        public String getProducerGroup() {
            return producerGroup;
        }

        public void setProducerGroup(String producerGroup) {
            this.producerGroup = producerGroup;
        }

        public int getSendTimeoutMs() {
            return sendTimeoutMs;
        }

        public void setSendTimeoutMs(int sendTimeoutMs) {
            this.sendTimeoutMs = sendTimeoutMs;
        }

        public int getRetryTimesWhenSendFailed() {
            return retryTimesWhenSendFailed;
        }

        public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
            this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
        }

        public int getConsumerThreads() {
            return consumerThreads;
        }

        public void setConsumerThreads(int consumerThreads) {
            this.consumerThreads = consumerThreads;
        }
    }

    public static class Seckill {
        private String topic = "topic-seckill-order";
        private String consumerGroup = "cg-seckill-order";
        private String streamKey = "sk:stream";
        private int dispatchThreads = 2;
        private int dispatchBatchSize = 256;
        private long dispatchPollMs = 20;
        private int maxInFlight = 20000;
        private long inFlightTimeoutMs = 20000;
        private int maxRedeliver = 5;
        private int requestTtlSeconds = 7200;
        private long reconcileIntervalMs = 5000;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public String getStreamKey() {
            return streamKey;
        }

        public void setStreamKey(String streamKey) {
            this.streamKey = streamKey;
        }

        public int getDispatchThreads() {
            return dispatchThreads;
        }

        public void setDispatchThreads(int dispatchThreads) {
            this.dispatchThreads = dispatchThreads;
        }

        public int getDispatchBatchSize() {
            return dispatchBatchSize;
        }

        public void setDispatchBatchSize(int dispatchBatchSize) {
            this.dispatchBatchSize = dispatchBatchSize;
        }

        public long getDispatchPollMs() {
            return dispatchPollMs;
        }

        public void setDispatchPollMs(long dispatchPollMs) {
            this.dispatchPollMs = dispatchPollMs;
        }

        public int getMaxInFlight() {
            return maxInFlight;
        }

        public void setMaxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
        }

        public long getInFlightTimeoutMs() {
            return inFlightTimeoutMs;
        }

        public void setInFlightTimeoutMs(long inFlightTimeoutMs) {
            this.inFlightTimeoutMs = inFlightTimeoutMs;
        }

        public int getMaxRedeliver() {
            return maxRedeliver;
        }

        public void setMaxRedeliver(int maxRedeliver) {
            this.maxRedeliver = maxRedeliver;
        }

        public int getRequestTtlSeconds() {
            return requestTtlSeconds;
        }

        public void setRequestTtlSeconds(int requestTtlSeconds) {
            this.requestTtlSeconds = requestTtlSeconds;
        }

        public long getReconcileIntervalMs() {
            return reconcileIntervalMs;
        }

        public void setReconcileIntervalMs(long reconcileIntervalMs) {
            this.reconcileIntervalMs = reconcileIntervalMs;
        }
    }

    public static class Like {
        private String topic = "topic-like-event";
        private String consumerGroup = "cg-like-event";
        private String streamKey = "lk:stream";
        private String rankKey = "lk:rank";
        private int dispatchThreads = 2;
        private int dispatchBatchSize = 512;
        private long dispatchPollMs = 20;
        private long reconcileIntervalMs = 10000;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public String getStreamKey() {
            return streamKey;
        }

        public void setStreamKey(String streamKey) {
            this.streamKey = streamKey;
        }

        public String getRankKey() {
            return rankKey;
        }

        public void setRankKey(String rankKey) {
            this.rankKey = rankKey;
        }

        public int getDispatchThreads() {
            return dispatchThreads;
        }

        public void setDispatchThreads(int dispatchThreads) {
            this.dispatchThreads = dispatchThreads;
        }

        public int getDispatchBatchSize() {
            return dispatchBatchSize;
        }

        public void setDispatchBatchSize(int dispatchBatchSize) {
            this.dispatchBatchSize = dispatchBatchSize;
        }

        public long getDispatchPollMs() {
            return dispatchPollMs;
        }

        public void setDispatchPollMs(long dispatchPollMs) {
            this.dispatchPollMs = dispatchPollMs;
        }

        public long getReconcileIntervalMs() {
            return reconcileIntervalMs;
        }

        public void setReconcileIntervalMs(long reconcileIntervalMs) {
            this.reconcileIntervalMs = reconcileIntervalMs;
        }
    }
}
