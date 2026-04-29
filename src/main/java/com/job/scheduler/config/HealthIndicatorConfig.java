package com.job.scheduler.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class HealthIndicatorConfig {

    private static final Duration KAFKA_HEALTH_TIMEOUT = Duration.ofSeconds(2);

    @Bean
    public HealthIndicator redisHealthIndicator(RedisConnectionFactory redisConnectionFactory) {
        return () -> {
            try (RedisConnection connection = redisConnectionFactory.getConnection()) {
                String response = connection.ping();
                if ("PONG".equalsIgnoreCase(response)) {
                    return Health.up().build();
                }
                return Health.down().withDetail("response", response).build();
            } catch (RuntimeException exception) {
                return Health.down(exception).build();
            }
        };
    }

    @Bean
    public HealthIndicator kafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        return () -> {
            Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
            config.putIfAbsent("request.timeout.ms", (int) KAFKA_HEALTH_TIMEOUT.toMillis());
            config.putIfAbsent("default.api.timeout.ms", (int) KAFKA_HEALTH_TIMEOUT.toMillis());

            try (AdminClient adminClient = AdminClient.create(config)) {
                adminClient.describeCluster().clusterId().get(
                        KAFKA_HEALTH_TIMEOUT.toMillis(),
                        java.util.concurrent.TimeUnit.MILLISECONDS
                );
                return Health.up().build();
            } catch (Exception exception) {
                return Health.down(exception).build();
            }
        };
    }
}
