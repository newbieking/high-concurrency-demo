package com.example.demo.config;

import com.example.demo.like.LikeConsumer;
import com.example.demo.seckill.SeckillConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class ConsumerConfig {

    private static final int CONSUME_BATCH_SIZE = 32;

    @Bean(destroyMethod = "shutdown")
    public DefaultMQPushConsumer seckillPushConsumer(AppProps props, SeckillConsumer listener) throws Exception {
        AppProps.Seckill cfg = props.getSeckill();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(cfg.getConsumerGroup());
        consumer.setNamesrvAddr(props.getMq().getNamesrvAddr());
        // 只在第一次启动（没有位点时）从最新消息开始，重启后按已提交位点续读
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(cfg.getTopic(), "*");
        consumer.setConsumeMessageBatchMaxSize(CONSUME_BATCH_SIZE);
        consumer.setConsumeThreadMin(props.getMq().getConsumerThreads());
        consumer.setConsumeThreadMax(props.getMq().getConsumerThreads());
        // 留出余量：正常路径在 maxRedeliver 次就人工终止了，这里是终止逻辑本身失败时的兜底
        consumer.setMaxReconsumeTimes(cfg.getMaxRedeliver() + 2);
        consumer.setInstanceName("seckill-" + UUID.randomUUID().toString().substring(0, 8));
        consumer.registerMessageListener(listener);
        consumer.start();
        return consumer;
    }

    @Bean(destroyMethod = "shutdown")
    public DefaultMQPushConsumer likePushConsumer(AppProps props, LikeConsumer listener) throws Exception {
        AppProps.Like cfg = props.getLike();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(cfg.getConsumerGroup());
        consumer.setNamesrvAddr(props.getMq().getNamesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(cfg.getTopic(), "*");
        consumer.setConsumeMessageBatchMaxSize(CONSUME_BATCH_SIZE);
        consumer.setConsumeThreadMin(props.getMq().getConsumerThreads());
        consumer.setConsumeThreadMax(props.getMq().getConsumerThreads());
        consumer.setMaxReconsumeTimes(8);
        consumer.setInstanceName("like-" + UUID.randomUUID().toString().substring(0, 8));
        consumer.registerMessageListener(listener);
        consumer.start();
        return consumer;
    }
}
