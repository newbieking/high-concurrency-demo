package com.example.demo.like;

public final class LikeKeys {

    public static final String INFLIGHT = "lk:inflight";

    private LikeKeys() {
    }

    public static String users(long targetId) {
        return "lk:{" + targetId + "}:users";
    }

    public static String count(long targetId) {
        return "lk:{" + targetId + "}:count";
    }
}
