package com.job.scheduler.config;

import com.job.scheduler.exception.RedisUnavailableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Slf4j
public class KafkaErrorHandlerConfig {
    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            @Value("${scheduler.kafka.redis-retry-backoff-ms:5000}") long redisRetryBackoffMs
    ) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (consumerRecord, thrownException) -> log.error(
                        "Kafka record was not processed after retries. topic={}, partition={}, offset={}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        thrownException
                ),
                new FixedBackOff(redisRetryBackoffMs, FixedBackOff.UNLIMITED_ATTEMPTS)
        );

        errorHandler.defaultFalse();
        errorHandler.addRetryableExceptions(RedisUnavailableException.class);
        errorHandler.setAckAfterHandle(false);

        return errorHandler;
    }
}
