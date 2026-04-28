package com.job.scheduler.integration;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.scheduler.DueJobSchedulerService;
import com.job.scheduler.service.WorkerService;
import com.job.scheduler.utility.Utilities;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

class SchedulerDispatchIntegrationTest extends AbstractSchedulerFlowIntegrationTest {

    @Autowired
    private WorkerService workerService;

    @Autowired
    private DueJobSchedulerService dueJobSchedulerService;

    @Test
    void dispatchDueJobsProcessesJobEndToEndThroughKafka() {
        doNothing().when(jobHandlerRouter).route(any(JobDispatchEvent.class));

        UUID jobId = submitCleanupJob(null, "e2e-success-" + UUID.randomUUID());

        dueJobSchedulerService.dispatchDueJobs();

        var processed = waitForJob(
                jobId,
                job -> job.getJobStatus() == JobStatus.SUCCESS,
                Duration.ofSeconds(15),
                "job to be marked SUCCESS after Kafka dispatch"
        );
        var executionLog = waitForExecutionLog(
                jobId,
                log -> log.getExecutionStatus() == JobStatus.SUCCESS,
                Duration.ofSeconds(15),
                "execution log to be marked SUCCESS"
        );

        assertThat(processed.getJobType()).isEqualTo(JobType.CLEANUP);
        assertThat(processed.getJobPriority()).isEqualTo(JobPriority.MEDIUM);
        assertThat(processed.getQueuedAt()).isNull();
        assertThat(processed.getStartedAt()).isNotNull();
        assertThat(processed.getCompletedAt()).isNotNull();
        assertThat(executionLogRepository.countByJobId(jobId)).isEqualTo(1);
        assertThat(executionLog.getAttemptNumber()).isEqualTo(1);
        assertThat(executionLog.getStartedAt()).isNotNull();
        assertThat(executionLog.getCompletedAt()).isNotNull();
        assertThat(executionLog.getCompletedAt()).isAfterOrEqualTo(executionLog.getStartedAt());
        assertThat(executionLog.getDurationMs()).isNotNull();
        assertThat(executionLog.getDurationMs()).isGreaterThanOrEqualTo(0L);
        assertThat(executionLog.getErrorMessage()).isNull();
        assertThat(executionLog.getWorkerId()).isEqualTo("integration-worker");
        assertThat(redisTemplate.hasKey(Utilities.getDoneKey(processed.getIdempotencyKey()))).isTrue();
    }

    @Test
    void processJobSkipsWhenDoneMarkerAlreadyExistsInRedis() {
        UUID jobId = submitCleanupJob(null, "done-marker-" + UUID.randomUUID());
        jobService.markDispatchSucceeded(jobId);
        Job queued = jobRepository.findById(jobId).orElseThrow();
        redisTemplate.opsForValue().set(Utilities.getDoneKey(queued.getIdempotencyKey()), "true", Duration.ofHours(24));

        workerService.processJob(new JobDispatchEvent(jobId, JobType.CLEANUP));

        var untouched = jobRepository.findById(jobId).orElseThrow();
        assertThat(untouched.getJobStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(untouched.getQueuedAt()).isNotNull();
        assertThat(untouched.getStartedAt()).isNull();
        assertThat(untouched.getCompletedAt()).isNull();
        assertThat(executionLogRepository.countByJobId(jobId)).isZero();
    }

    @Test
    void processJobSchedulesRetryAfterHandlerFailure() {
        doThrow(new RuntimeException("temporary failure")).when(jobHandlerRouter).route(any(JobDispatchEvent.class));

        UUID jobId = submitCleanupJob(null, "retry-flow-" + UUID.randomUUID());
        jobService.markDispatchSucceeded(jobId);

        workerService.processJob(new JobDispatchEvent(jobId, JobType.CLEANUP));

        var retried = jobRepository.findById(jobId).orElseThrow();
        var executionLog = waitForExecutionLog(
                jobId,
                log -> log.getExecutionStatus() == JobStatus.FAILED,
                Duration.ofSeconds(5),
                "execution log to be marked FAILED for retryable error"
        );

        assertThat(retried.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(retried.getNextRunAt()).isAfter(Instant.now());
        assertThat(retried.getLastErrorMessage()).isEqualTo("temporary failure");
        assertThat(retried.getQueuedAt()).isNull();
        assertThat(retried.getStartedAt()).isNull();
        assertThat(retried.getCompletedAt()).isNull();
        assertThat(executionLogRepository.countByJobId(jobId)).isEqualTo(1);
        assertThat(executionLog.getAttemptNumber()).isEqualTo(1);
        assertThat(executionLog.getStartedAt()).isNotNull();
        assertThat(executionLog.getCompletedAt()).isNotNull();
        assertThat(executionLog.getDurationMs()).isNotNull();
        assertThat(executionLog.getErrorMessage()).isEqualTo("temporary failure");
        assertThat(executionLog.getWorkerId()).isEqualTo("integration-worker");
    }

    @Test
    void processJobReschedulesCronJobAfterSuccess() {
        doNothing().when(jobHandlerRouter).route(any(JobDispatchEvent.class));

        UUID jobId = submitCleanupJob("*/5 * * * * *", "cron-flow-" + UUID.randomUUID());
        jobService.markDispatchSucceeded(jobId);

        workerService.processJob(new JobDispatchEvent(jobId, JobType.CLEANUP));

        var rescheduled = jobRepository.findById(jobId).orElseThrow();
        var executionLog = waitForExecutionLog(
                jobId,
                log -> log.getExecutionStatus() == JobStatus.SUCCESS,
                Duration.ofSeconds(5),
                "execution log to be marked SUCCESS for cron reschedule"
        );

        assertThat(rescheduled.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(rescheduled.getNextRunAt()).isAfter(Instant.now());
        assertThat(rescheduled.getQueuedAt()).isNull();
        assertThat(rescheduled.getStartedAt()).isNull();
        assertThat(rescheduled.getCompletedAt()).isNull();
        assertThat(rescheduled.getLastErrorMessage()).isNull();
        assertThat(executionLog.getAttemptNumber()).isEqualTo(1);
        assertThat(executionLog.getStartedAt()).isNotNull();
        assertThat(executionLog.getCompletedAt()).isNotNull();
        assertThat(executionLog.getDurationMs()).isNotNull();
        assertThat(executionLog.getErrorMessage()).isNull();
        assertThat(executionLog.getWorkerId()).isEqualTo("integration-worker");
        assertThat(redisTemplate.hasKey(Utilities.getDoneKey(rescheduled.getIdempotencyKey()))).isFalse();
    }
}
