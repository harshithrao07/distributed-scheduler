package com.job.scheduler.integration;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.scheduler.DeadLetterSchedulerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

class SchedulerDeadLetterIntegrationTest extends AbstractSchedulerFlowIntegrationTest {

    @Autowired
    private DeadLetterSchedulerService deadLetterSchedulerService;

    @Test
    void deadLetterSchedulerPublishesPendingDeadLettersAndMarksSent() {
        var deadJob = saveCleanupJob(JobStatus.DEAD, null, "dlq-success-" + UUID.randomUUID());
        deadJob.setDeadLetterStatus(DeadLetterStatus.PENDING);
        deadJob.setDeadLetterQueuedAt(Instant.now().minusSeconds(30));
        deadJob.setNextDeadLetterAttemptAt(Instant.now().minusSeconds(1));
        jobRepository.saveAndFlush(deadJob);

        deadLetterSchedulerService.publishPendingDeadLetters();

        var updated = waitForJob(
                deadJob.getId(),
                job -> job.getDeadLetterStatus() == DeadLetterStatus.SENT,
                Duration.ofSeconds(10),
                "dead-letter job to be published and marked SENT"
        );

        assertThat(updated.getDeadLetterSentAt()).isNotNull();
        assertThat(updated.getDeadLetterLastAttemptAt()).isNotNull();
        assertThat(updated.getDeadLetterSentAt()).isAfterOrEqualTo(updated.getDeadLetterLastAttemptAt());
        assertThat(updated.getDeadLetterQueuedAt()).isNotNull();
        assertThat(updated.getDeadLetterQueuedAt()).isBeforeOrEqualTo(updated.getDeadLetterSentAt());
        assertThat(updated.getNextDeadLetterAttemptAt()).isNull();
        assertThat(updated.getDeadLetterErrorMessage()).isNull();
    }

    @Test
    void deadLetterSchedulerMarksRetryMetadataWhenPublishFails() {
        var deadJob = saveCleanupJob(JobStatus.DEAD, null, "dlq-retry-" + UUID.randomUUID());
        deadJob.setDeadLetterStatus(DeadLetterStatus.PENDING);
        deadJob.setDeadLetterQueuedAt(Instant.now().minusSeconds(30));
        deadJob.setNextDeadLetterAttemptAt(Instant.now().minusSeconds(1));
        jobRepository.saveAndFlush(deadJob);

        doReturn(CompletableFuture.failedFuture(new RuntimeException("dlq unavailable")))
                .when(jobQueueProducer)
                .sendToDlq(any(JobDispatchEvent.class));

        deadLetterSchedulerService.publishPendingDeadLetters();

        var updated = waitForJob(
                deadJob.getId(),
                job -> "dlq unavailable".equals(job.getDeadLetterErrorMessage()),
                Duration.ofSeconds(5),
                "dead-letter retry metadata to be written after publish failure"
        );

        assertThat(updated.getDeadLetterStatus()).isEqualTo(DeadLetterStatus.PENDING);
        assertThat(updated.getDeadLetterLastAttemptAt()).isNotNull();
        assertThat(updated.getDeadLetterQueuedAt()).isNotNull();
        assertThat(updated.getDeadLetterErrorMessage()).isEqualTo("dlq unavailable");
        assertThat(updated.getNextDeadLetterAttemptAt()).isAfter(Instant.now());
        assertThat(updated.getNextDeadLetterAttemptAt()).isAfterOrEqualTo(updated.getDeadLetterLastAttemptAt());
    }
}
