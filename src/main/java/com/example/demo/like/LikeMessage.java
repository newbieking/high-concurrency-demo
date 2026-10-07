package com.example.demo.like;

import com.example.demo.infra.MsgCodec;

import java.util.Map;

/**
 * 点赞事件用「增量」而不是「全量状态」，好处是消息乱序也不会算错——
 * 计数只是累加，谁先谁后结果一样。明细表用 seq 做 LWW，同样不依赖到达顺序。
 */
public record LikeMessage(long targetId, long userId, int delta, String streamId, long seq, long ts) {

    public static LikeMessage from(String body) {
        Map<String, String> f = MsgCodec.decode(body);
        return new LikeMessage(
                MsgCodec.asLong(f, "targetId", -1),
                MsgCodec.asLong(f, "userId", -1),
                MsgCodec.asInt(f, "delta", 0),
                f.getOrDefault("streamId", ""),
                MsgCodec.asLong(f, "seq", 0),
                MsgCodec.asLong(f, "ts", 0));
    }

    public boolean valid() {
        return targetId > 0 && userId > 0 && delta != 0 && !streamId.isEmpty();
    }
}
