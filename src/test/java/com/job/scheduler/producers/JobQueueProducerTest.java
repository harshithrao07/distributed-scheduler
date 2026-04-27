package com.job.scheduler.producers;

import com.job.scheduler.constants.Topics;
import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.enums.JobType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobQueueProducerTest {

    @Mock
    private KafkaTemplate<UUID, JobDispatchEvent> kafkaTemplate;

    private JobQueueProducer jobQueueProducer;

    @BeforeEach
    void setUp() {
        jobQueueProducer = new JobQueueProducer(kafkaTemplate);
    }

    @Test
    void sendToMainQueueUsesMainTopic() throws Exception {
        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), JobType.WEBHOOK);
        when(kafkaTemplate.send(Topics.TOPIC_JOB_QUEUE, event.jobId(), event))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> result = jobQueueProducer.sendToMainQueue(event);

        verify(kafkaTemplate).send(Topics.TOPIC_JOB_QUEUE, event.jobId(), event);
        assertThat(result.get()).isNull();
    }

    @Test
    void sendToHighPriorityQueueUsesHighPriorityTopic() throws Exception {
        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), JobType.CLEANUP);
        when(kafkaTemplate.send(Topics.TOPIC_JOB_QUEUE_HIGH, event.jobId(), event))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> result = jobQueueProducer.sendToHighPriorityQueue(event);

        verify(kafkaTemplate).send(Topics.TOPIC_JOB_QUEUE_HIGH, event.jobId(), event);
        assertThat(result.get()).isNull();
    }

    @Test
    void sendToDlqUsesDlqTopic() throws Exception {
        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), JobType.SEND_EMAIL);
        when(kafkaTemplate.send(Topics.TOPIC_JOB_DLQ, event.jobId(), event))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> result = jobQueueProducer.sendToDlq(event);

        verify(kafkaTemplate).send(Topics.TOPIC_JOB_DLQ, event.jobId(), event);
        assertThat(result.get()).isNull();
    }

    @Test
    void sendToMainQueuePropagatesKafkaFailure() {
        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), JobType.WEBHOOK);
        CompletableFuture<?> failedFuture = CompletableFuture.failedFuture(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(Topics.TOPIC_JOB_QUEUE, event.jobId(), event))
                .thenReturn((CompletableFuture) failedFuture);

        CompletableFuture<Void> result = jobQueueProducer.sendToMainQueue(event);

        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("kafka down");
    }
}
