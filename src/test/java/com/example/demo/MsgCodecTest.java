package com.example.demo;

import com.example.demo.infra.MsgCodec;
import com.example.demo.infra.StreamIds;
import com.example.demo.like.LikeMessage;
import com.example.demo.seckill.SeckillMessage;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MsgCodecTest {

    @Test
    void roundTripKeepsSpecialCharacters() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("requestId", "req-1&userId=999");
        fields.put("userId", "1001");
        fields.put("note", "a=b c");

        Map<String, String> decoded = MsgCodec.decode(MsgCodec.encode(fields));

        assertEquals(fields, decoded);
    }

    @Test
    void seckillMessageSurvivesRoundTrip() {
        SeckillMessage origin = new SeckillMessage(1L, 42L, "req-42", 1730000000000L);

        SeckillMessage restored = SeckillMessage.from(origin.toBody());

        assertEquals(origin, restored);
        assertTrue(restored.valid());
    }

    @Test
    void malformedSeckillMessageIsRejected() {
        assertFalse(SeckillMessage.from("userId=1").valid());
    }

    @Test
    void likeMessageUsesStreamIdAsOrderingToken() {
        long seq = StreamIds.toSeq("1730000000000-7");
        assertEquals(1730000000000L * 1_000_000L + 7, seq);

        // 同一个 stream 内 ID 单调递增，换算出来的 seq 也必须单调
        assertTrue(StreamIds.toSeq("1730000000000-8") > seq);
        assertTrue(StreamIds.toSeq("1730000000001-0") > StreamIds.toSeq("1730000000000-9"));
    }

    @Test
    void likeMessageParsesDeltaAndSeq() {
        String body = MsgCodec.encode(Map.of(
                "targetId", "101",
                "userId", "7",
                "delta", "-1",
                "streamId", "1730000000000-3",
                "seq", String.valueOf(StreamIds.toSeq("1730000000000-3")),
                "ts", "1730000000000"));

        LikeMessage message = LikeMessage.from(body);

        assertEquals(101L, message.targetId());
        assertEquals(7L, message.userId());
        assertEquals(-1, message.delta());
        assertTrue(message.valid());
    }

    @Test
    void likeMessageWithoutDeltaIsInvalid() {
        assertFalse(LikeMessage.from("targetId=101&userId=7&streamId=1-0").valid());
    }
}
