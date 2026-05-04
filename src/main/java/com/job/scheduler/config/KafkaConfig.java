package com.job.scheduler.config;

import com.job.scheduler.constants.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafka
public class KafkaConfig {
    @Bean
    public NewTopic jobQueueTopic() {
        return TopicBuilder.name(Topics.TOPIC_JOB_QUEUE)
                .partitions(36)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic jobQueueHighTopic() {
        return TopicBuilder.name(Topics.TOPIC_JOB_QUEUE_HIGH)
                .partitions(36)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic jobDLQTopic() {
        return TopicBuilder.name(Topics.TOPIC_JOB_DLQ)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
