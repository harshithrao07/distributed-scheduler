package com.job.scheduler.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.test.autoconfigure.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@DataRedisTest
@Import(RedisLockService.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = "scheduler.worker-id=worker-one")
class RedisLockServiceIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private RedisLockService redisLockService;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @BeforeEach
    void clearRedis() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    void acquireLockAllowsOnlyOneWorkerForSameKey() {
        RedisLockService competingWorker = competingWorker("worker-two");

        String firstToken = redisLockService.acquireLock("job-lock:shared-job", Duration.ofSeconds(30));
        String secondToken = competingWorker.acquireLock("job-lock:shared-job", Duration.ofSeconds(30));

        assertThat(firstToken).isNotNull();
        assertThat(firstToken).startsWith("worker-one:");
        assertThat(secondToken).isNull();
    }

    @Test
    void duplicateProcessingIsSkippedWhileLockExists() {
        RedisLockService firstWorker = competingWorker("worker-a");
        RedisLockService duplicateWorker = competingWorker("worker-b");

        String lockToken = firstWorker.acquireLock("job-lock:duplicate-job", Duration.ofSeconds(30));
        String duplicateAttempt = duplicateWorker.acquireLock("job-lock:duplicate-job", Duration.ofSeconds(30));

        assertThat(lockToken).isNotNull();
        assertThat(duplicateAttempt).isNull();

        assertThat(firstWorker.releaseLock("job-lock:duplicate-job", lockToken)).isTrue();

        String retryAfterRelease = duplicateWorker.acquireLock("job-lock:duplicate-job", Duration.ofSeconds(30));
        assertThat(retryAfterRelease).isNotNull();
        assertThat(retryAfterRelease).startsWith("worker-b:");
    }

    private RedisLockService competingWorker(String workerId) {
        RedisLockService service = new RedisLockService(redisTemplate);
        ReflectionTestUtils.setField(service, "workerId", workerId);
        return service;
    }
}
