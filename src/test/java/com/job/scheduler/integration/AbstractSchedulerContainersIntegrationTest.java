package com.job.scheduler.integration;

import com.job.scheduler.SchedulerApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(classes = SchedulerApplication.class)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=create",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.UUIDSerializer",
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer",
        "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.UUIDDeserializer",
        "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
        "spring.kafka.consumer.properties.spring.json.value.default.type=com.job.scheduler.dto.JobDispatchEvent",
        "spring.kafka.consumer.properties.spring.json.trusted.packages=com.job.scheduler.dto,com.job.scheduler.enums",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "scheduler.worker-id=integration-worker",
        "scheduler.due-job.poll-delay-ms=3600000",
        "scheduler.queued-watchdog.poll-delay-ms=3600000",
        "scheduler.running-watchdog.poll-delay-ms=3600000",
        "scheduler.dead-letter.poll-delay-ms=3600000",
        "scheduler.redis-health.poll-delay-ms=3600000",
        "scheduler.kafka.redis-retry-backoff-ms=1000",
        "logging.level.com.zaxxer.hikari.pool.PoolBase=ERROR"
})
public abstract class AbstractSchedulerContainersIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("jobscheduler")
            .withUsername("postgres")
            .withPassword("postgres");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.2"));

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
