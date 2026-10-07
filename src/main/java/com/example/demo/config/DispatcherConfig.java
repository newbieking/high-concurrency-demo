package com.example.demo.config;

import com.example.demo.infra.MqSender;
import com.example.demo.infra.MsgCodec;
import com.example.demo.infra.RedisBuffer;
import com.example.demo.infra.StreamDispatcher;
import com.example.demo.infra.StreamIds;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class DispatcherConfig {

    @Bean
    public StreamDispatcher seckillDispatcher(AppProps props, RedisBuffer buffer, MqSender sender) {
        AppProps.Seckill s = props.getSeckill();
        return new StreamDispatcher("seckill", s.getStreamKey(), s.getConsumerGroup(), s.getTopic(),
                s.getDispatchThreads(), s.getDispatchBatchSize(), s.getDispatchPollMs(),
                (streamId, fields) -> MsgCodec.encode(fields), buffer, sender);
    }

    @Bean
    public StreamDispatcher likeDispatcher(AppProps props, RedisBuffer buffer, MqSender sender) {
        AppProps.Like l = props.getLike();
        return new StreamDispatcher("like", l.getStreamKey(), l.getConsumerGroup(), l.getTopic(),
                l.getDispatchThreads(), l.getDispatchBatchSize(), l.getDispatchPollMs(),
                (streamId, fields) -> {
                    Map<String, String> body = new LinkedHashMap<>(fields);
                    body.put("streamId", streamId);
                    body.put("seq", String.valueOf(StreamIds.toSeq(streamId)));
                    return MsgCodec.encode(body);
                }, buffer, sender);
    }
}
