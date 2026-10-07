package com.example.demo.seckill;

import com.example.demo.infra.MsgCodec;

import java.util.Map;

public record SeckillMessage(long activityId, long userId, String requestId, long ts) {

    public static SeckillMessage from(String body) {
        Map<String, String> f = MsgCodec.decode(body);
        return new SeckillMessage(
                MsgCodec.asLong(f, "activityId", -1),
                MsgCodec.asLong(f, "userId", -1),
                f.getOrDefault("requestId", ""),
                MsgCodec.asLong(f, "ts", 0));
    }

    public boolean valid() {
        return activityId > 0 && userId > 0 && !requestId.isEmpty();
    }

    public String toBody() {
        return MsgCodec.encode(Map.of(
                "activityId", String.valueOf(activityId),
                "userId", String.valueOf(userId),
                "requestId", requestId,
                "ts", String.valueOf(ts)));
    }
}
