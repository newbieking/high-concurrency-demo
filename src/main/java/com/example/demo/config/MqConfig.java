package com.example.demo.config;

import com.example.demo.infra.MqSender;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class MqConfig {

    @Bean(destroyMethod = "")
    public DefaultMQProducer rocketMqProducer(AppProps props) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(props.getMq().getProducerGroup());
        producer.setNamesrvAddr(props.getMq().getNamesrvAddr());
        producer.setSendMsgTimeout(props.getMq().getSendTimeoutMs());
        producer.setRetryTimesWhenSendFailed(props.getMq().getRetryTimesWhenSendFailed());
        // 多个实例同组时 instanceName 不能重名，否则会被 Broker 当成同一个客户端
        producer.setInstanceName("producer-" + UUID.randomUUID().toString().substring(0, 8));
        producer.setCompressMsgBodyOverHowmuch(4096);
        producer.start();
        return producer;
    }

    @Bean
    public MqSender mqSender(DefaultMQProducer producer) {
        return new MqSender(producer);
    }
}
