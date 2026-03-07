package com.estudo.redis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "redis.service", value = "enable", matchIfMissing = true)
public class RedisConfig {

    @Value("${redis.service.host}")
    private String host;

    @Value("${redis.service.port}")
    private int port;

    @Bean
    public LettuceConnectionFactory connectionFactory() {
        RedisStandaloneConfiguration redisConf = new RedisStandaloneConfiguration();
        redisConf.setHostName(host);
        redisConf.setPort(port);
        return new LettuceConnectionFactory(redisConf);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }
}
