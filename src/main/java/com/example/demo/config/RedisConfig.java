package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

@Configuration
public class RedisConfig {

    @Bean
    public RedisScript<Long> seckillAcceptScript() {
        return script("lua/seckill_accept.lua");
    }

    @Bean
    public RedisScript<Long> seckillConfirmScript() {
        return script("lua/seckill_confirm.lua");
    }

    @Bean
    public RedisScript<Long> seckillRollbackScript() {
        return script("lua/seckill_rollback.lua");
    }

    @Bean
    public RedisScript<Long> likeConfirmScript() {
        return script("lua/like_confirm.lua");
    }

    @Bean
    @SuppressWarnings("rawtypes")
    public RedisScript<java.util.List> likeToggleScript() {
        DefaultRedisScript<java.util.List> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/like_toggle.lua")));
        script.setResultType(java.util.List.class);
        return script;
    }

    private DefaultRedisScript<Long> script(String path) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource(path)));
        script.setResultType(Long.class);
        return script;
    }
}
