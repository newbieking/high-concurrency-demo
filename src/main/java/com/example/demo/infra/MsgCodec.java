package com.example.demo.infra;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消息体编解码：URL 编码的扁平键值对，形如 requestId=abc&userId=1&ts=1730000000000。
 * 用 JDK 自带能力，不引入 JSON 库；好处是消息在 MQ 控制台里直接可读，排障时不用解码。
 * 生产环境字段变复杂后应换成 JSON / Protobuf 并配 schema 管理。
 */
public final class MsgCodec {

    private MsgCodec() {
    }

    public static String encode(Map<String, String> fields) {
        StringBuilder sb = new StringBuilder(fields.size() * 32);
        for (Map.Entry<String, String> e : fields.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    public static Map<String, String> decode(String body) {
        Map<String, String> fields = new LinkedHashMap<>();
        if (body == null || body.isEmpty()) {
            return fields;
        }
        for (String pair : body.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int idx = pair.indexOf('=');
            if (idx < 0) {
                fields.put(URLDecoder.decode(pair, StandardCharsets.UTF_8), "");
            } else {
                String k = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                fields.put(k, v);
            }
        }
        return fields;
    }

    public static long asLong(Map<String, String> fields, String key, long defaultValue) {
        String v = fields.get(key);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static int asInt(Map<String, String> fields, String key, int defaultValue) {
        return (int) asLong(fields, key, defaultValue);
    }
}
