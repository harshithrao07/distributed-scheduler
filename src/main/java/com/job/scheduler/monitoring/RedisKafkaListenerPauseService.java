package com.job.scheduler.monitoring;

import com.job.scheduler.service.RedisHealthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RedisKafkaListenerPauseService {
    private final RedisHealthService redisHealthService;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Scheduled(fixedDelayString = "${scheduler.redis-health.poll-delay-ms:5000}")
    public void pauseWorkersWhenRedisIsDown() {
        boolean redisAvailable = redisHealthService.isRedisAvailable();

        for (MessageListenerContainer listenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            if (redisAvailable && listenerContainer.isContainerPaused()) {
                listenerContainer.resume();
                log.info("Redis is available again. Resumed Kafka listener container: {}", listenerContainer.getListenerId());
            } else if (!redisAvailable && listenerContainer.isRunning() && !listenerContainer.isContainerPaused()) {
                listenerContainer.pause();
                log.warn("Redis is unavailable. Paused Kafka listener container: {}", listenerContainer.getListenerId());
            }
        }
    }
}
