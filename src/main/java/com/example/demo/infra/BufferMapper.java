package com.example.demo.infra;

import java.util.Map;

/**
 * 把 Redis Stream 里的一个条目翻译成 MQ 消息体。
 * streamId 单独传进来，因为点赞场景要用它当作 LWW 版本号（Stream ID 单调递增）。
 */
public interface BufferMapper {

    String toBody(String streamId, Map<String, String> fields);
}
