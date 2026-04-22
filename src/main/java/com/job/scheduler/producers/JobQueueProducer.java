package com.job.scheduler.producers;

import com.job.scheduler.constants.Topics;
import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.enums.JobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobQueueProducer {
    private final KafkaTemplate<UUID, JobDispatchEvent> kafkaTemplate;

    public CompletableFuture<Void> sendToMainQueue(JobDispatchEvent job) {
        log.info("Attempting to send message to topic: {}, recordId: {}", Topics.TOPIC_JOB_QUEUE, job.jobId());
        return kafkaTemplate.send(Topics.TOPIC_JOB_QUEUE, job.jobId(), job)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent successfully to topic: {}, recordId: {}", Topics.TOPIC_JOB_QUEUE, job.jobId());
                    } else {
                        log.error("Failed to send message to topic: {}, recordId: {}", Topics.TOPIC_JOB_QUEUE, job.jobId(), ex);
                    }
                })
                .thenApply(result -> null);
    }

    public CompletableFuture<Void> sendToHighPriorityQueue(JobDispatchEvent job) {
        log.info("Attempting to send message to topic: {}, recordId: {}", Topics.TOPIC_JOB_QUEUE_HIGH, job.jobId());
        return kafkaTemplate.send(Topics.TOPIC_JOB_QUEUE_HIGH, job.jobId(), job)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent successfully to topic: {}, recordId: {}", Topics.TOPIC_JOB_QUEUE_HIGH, job.jobId());
                    } else {
                        log.error("Failed to send message to topic: {}, recordId: {}", Topics.TOPIC_JOB_QUEUE_HIGH, job.jobId(), ex);
                    }
                })
                .thenApply(result -> null);
    }

    public CompletableFuture<Void> sendToDlq(JobDispatchEvent job) {
        log.info("Attempting to send message to topic: {}, recordId: {}", Topics.TOPIC_JOB_DLQ, job.jobId());
        return kafkaTemplate.send(Topics.TOPIC_JOB_DLQ, job.jobId(), job)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent successfully to topic: {}, recordId: {}", Topics.TOPIC_JOB_DLQ, job.jobId());
                    } else {
                        log.error("Failed to send message to topic: {}, recordId: {}", Topics.TOPIC_JOB_DLQ, job.jobId(), ex);
                    }
                })
                .thenApply(result -> null);
    }
}
