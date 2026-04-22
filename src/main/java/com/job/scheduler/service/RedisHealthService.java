package com.job.scheduler.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RedisHealthService {
    private final RedisConnectionFactory redisConnectionFactory;

    public boolean isRedisAvailable() {
        try (RedisConnection connection = redisConnectionFactory.getConnection()) {
            String response = connection.ping();
            return "PONG".equalsIgnoreCase(response);
        } catch (RuntimeException e) {
            log.debug("Redis is unavailable: {}", e.getMessage());
            return false;
        }
    }
}
