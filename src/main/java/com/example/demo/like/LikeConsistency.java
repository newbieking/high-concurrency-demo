package com.example.demo.like;

import java.util.List;

public record LikeConsistency(
        boolean drained,
        boolean pass,
        long inFlight,
        long streamLength,
        long streamPending,
        List<TargetDiff> diffs,
        List<String> violations) {

    public record TargetDiff(long targetId, long redisCount, long dbCount) {
    }
}
