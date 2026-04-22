package com.job.scheduler.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RedisLockService {
    private static final RedisScript<Long> RELEASE_LOCK_SCRIPT = RedisScript.of(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            Long.class
    );

    private static final RedisScript<Long> RENEW_LOCK_SCRIPT = RedisScript.of(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('pexpire', KEYS[1], ARGV[2])
            end
            return 0
            """,
            Long.class
    );

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${scheduler.worker-id}")
    private String workerId;

    public String acquireLock(String lockKey, Duration ttl) {
        String lockToken = workerId + ":" + UUID.randomUUID();
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, lockToken, ttl);
        return Boolean.TRUE.equals(acquired) ? lockToken : null;
    }


    public boolean releaseLock(String lockKey, String lockToken) {
        Long released = redisTemplate.execute(
                RELEASE_LOCK_SCRIPT,
                Collections.singletonList(lockKey),
                lockToken
        );
        return Long.valueOf(1).equals(released);
    }

    public boolean renewLock(String lockKey, String lockToken, Duration ttl) {
        Long renewed = redisTemplate.execute(
                RENEW_LOCK_SCRIPT,
                Collections.singletonList(lockKey),
                lockToken,
                String.valueOf(ttl.toMillis())
        );
        return Long.valueOf(1).equals(renewed);
    }
}
