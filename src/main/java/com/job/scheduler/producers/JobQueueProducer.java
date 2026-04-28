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
    private static final String SEND_ATTEMPT_LOG = "Attempting to send message to topic: {}, recordId: {}";
    private static final String SEND_SUCCESS_LOG = "Message sent successfully to topic: {}, recordId: {}";
    private static final String SEND_FAILURE_LOG = "Failed to send message to topic: {}, recordId: {}";

    private final KafkaTemplate<UUID, JobDispatchEvent> kafkaTemplate;

    public CompletableFuture<Void> sendToMainQueue(JobDispatchEvent job) {
        return send(Topics.TOPIC_JOB_QUEUE, job);
    }

    public CompletableFuture<Void> sendToHighPriorityQueue(JobDispatchEvent job) {
        return send(Topics.TOPIC_JOB_QUEUE_HIGH, job);
    }

    public CompletableFuture<Void> sendToDlq(JobDispatchEvent job) {
        return send(Topics.TOPIC_JOB_DLQ, job);
    }

    private CompletableFuture<Void> send(String topic, JobDispatchEvent job) {
        log.info(SEND_ATTEMPT_LOG, topic, job.jobId());
        return kafkaTemplate.send(topic, job.jobId(), job)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info(SEND_SUCCESS_LOG, topic, job.jobId());
                    } else {
                        log.error(SEND_FAILURE_LOG, topic, job.jobId(), ex);
                    }
                })
                .thenApply(result -> null);
    }
}
