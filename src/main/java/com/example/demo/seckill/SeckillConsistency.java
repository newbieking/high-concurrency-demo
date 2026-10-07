package com.example.demo.seckill;

import java.util.List;

public record SeckillConsistency(
        boolean drained,
        boolean pass,
        long stockTotal,
        long redisStock,
        long redisPreDeducted,
        long dbStock,
        long dbOrders,
        long inFlight,
        long streamLength,
        long streamPending,
        List<String> violations) {
}
