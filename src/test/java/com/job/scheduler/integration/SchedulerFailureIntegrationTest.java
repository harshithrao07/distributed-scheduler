package com.job.scheduler.integration;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.exception.RedisUnavailableException;
import com.job.scheduler.scheduler.DueJobSchedulerService;
import com.job.scheduler.service.RedisHealthService;
import com.job.scheduler.service.WorkerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

class SchedulerFailureIntegrationTest extends AbstractSchedulerFlowIntegrationTest {

    @Autowired
    private DueJobSchedulerService dueJobSchedulerService;

    @Autowired
    private WorkerService workerService;

    @MockitoSpyBean
    private RedisHealthService redisHealthService;

    @Test
    void dispatchDueJobsLeavesPendingJobUntouchedWhenRedisIsUnavailable() {
        doReturn(false).when(redisHealthService).isRedisAvailable();

        UUID jobId = submitCleanupJob(null, "redis-down-dispatch-" + UUID.randomUUID());
        Instant originalNextRunAt = jobRepository.findById(jobId).orElseThrow().getNextRunAt();

        dueJobSchedulerService.dispatchDueJobs();

        var job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getJobStatus()).isEqualTo(com.job.scheduler.enums.JobStatus.PENDING);
        assertThat(job.getQueuedAt()).isNull();
        assertThat(job.getStartedAt()).isNull();
        assertThat(job.getCompletedAt()).isNull();
        assertThat(job.getNextRunAt()).isEqualTo(originalNextRunAt);
        assertThat(executionLogRepository.countByJobId(jobId)).isZero();
    }

    @Test
    void dispatchDueJobsKeepsClaimedJobRecoverableWhenKafkaPublishFails() {
        doReturn(CompletableFuture.failedFuture(new RuntimeException("kafka unavailable")))
                .when(jobQueueProducer)
                .sendToMainQueue(any(JobDispatchEvent.class));

        UUID jobId = submitCleanupJob(null, "kafka-dispatch-failure-" + UUID.randomUUID());

        dueJobSchedulerService.dispatchDueJobs();

        var claimedButUndispatched = jobRepository.findById(jobId).orElseThrow();
        assertThat(claimedButUndispatched.getJobStatus()).isEqualTo(com.job.scheduler.enums.JobStatus.PENDING);
        assertThat(claimedButUndispatched.getQueuedAt()).isNull();
        assertThat(claimedButUndispatched.getStartedAt()).isNull();
        assertThat(claimedButUndispatched.getCompletedAt()).isNull();
        assertThat(claimedButUndispatched.getNextRunAt()).isAfter(Instant.now());
        assertThat(claimedButUndispatched.getLastErrorMessage()).isNull();
        assertThat(executionLogRepository.countByJobId(jobId)).isZero();
    }

    @Test
    void processJobFailsFastAndPreservesQueuedStateWhenRedisIsUnavailable() {
        doReturn(false).when(redisHealthService).isRedisAvailable();

        UUID jobId = submitCleanupJob(null, "redis-down-worker-" + UUID.randomUUID());
        jobService.markDispatchSucceeded(jobId);

        assertThatThrownBy(() -> workerService.processJob(new JobDispatchEvent(jobId, com.job.scheduler.enums.JobType.CLEANUP)))
                .isInstanceOf(RedisUnavailableException.class)
                .hasMessageContaining("Redis is unavailable");

        var job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getJobStatus()).isEqualTo(com.job.scheduler.enums.JobStatus.QUEUED);
        assertThat(job.getQueuedAt()).isNotNull();
        assertThat(job.getStartedAt()).isNull();
        assertThat(job.getCompletedAt()).isNull();
        assertThat(executionLogRepository.countByJobId(jobId)).isZero();
    }
}
