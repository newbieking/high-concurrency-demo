package com.example.demo.seckill;

public record SeckillResultView(
        String requestId,
        String state,
        long activityId,
        long userId,
        long acceptedAt,
        long doneAt,
        int retry) {
}
