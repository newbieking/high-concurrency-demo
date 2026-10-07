package com.example.demo.infra;

/**
 * Redis Stream ID 形如 1730000000000-5，同一 stream 内单调递增，
 * 直接换算成一个可比较的数值就能当 LWW 版本号用，
 * 省掉了给每个 (target,user) 单独维护自增 key 的开销。
 */
public final class StreamIds {

    private static final long SEQUENCE_SCALE = 1_000_000L;

    private StreamIds() {
    }

    public static long toSeq(String streamId) {
        int dash = streamId.indexOf('-');
        if (dash <= 0) {
            return 0L;
        }
        try {
            long ms = Long.parseLong(streamId.substring(0, dash));
            long seq = Long.parseLong(streamId.substring(dash + 1));
            return ms * SEQUENCE_SCALE + seq;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
