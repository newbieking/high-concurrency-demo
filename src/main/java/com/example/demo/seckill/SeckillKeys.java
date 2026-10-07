package com.example.demo.seckill;

public final class SeckillKeys {

    private SeckillKeys() {
    }

    public static String stock(long activityId) {
        return "sk:{" + activityId + "}:stock";
    }

    public static String buyers(long activityId) {
        return "sk:{" + activityId + "}:buyers";
    }

    public static String pending(long activityId) {
        return "sk:{" + activityId + "}:pending";
    }

    public static String request(long activityId, String requestId) {
        return "sk:{" + activityId + "}:req:" + requestId;
    }
}
