package com.job.scheduler.service;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.exception.RedisUnavailableException;
import com.job.scheduler.handlers.JobHandlerRouter;
import com.job.scheduler.utility.Utilities;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WorkerServiceTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private JobService jobService;

    @Mock
    private ExecutionLogService executionLogService;

    @Mock
    private JobHandlerRouter jobHandlerRouter;

    @Mock
    private RedisLockService redisLockService;

    @Mock
    private RedisHealthService redisHealthService;

    private WorkerService workerService;

    @BeforeEach
    void setUp() {
        workerService = new WorkerService(
                redisTemplate,
                jobService,
                executionLogService,
                jobHandlerRouter,
                redisLockService,
                redisHealthService
        );
        ReflectionTestUtils.setField(workerService, "workerId", "worker-test");
        ReflectionTestUtils.setField(workerService, "retryBaseDelayMs", 1000L);
        ReflectionTestUtils.setField(workerService, "retryMaxDelayMs", 30000L);

        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @AfterEach
    void tearDown() {
        workerService.shutdown();
    }

    @Test
    void processJobThrowsWhenRedisIsUnavailable() {
        JobDispatchEvent event = new JobDispatchEvent(UUID.randomUUID(), JobType.WEBHOOK);
        when(redisHealthService.isRedisAvailable()).thenReturn(false);

        assertThatThrownBy(() -> workerService.processJob(event))
                .isInstanceOf(RedisUnavailableException.class)
                .hasMessageContaining("Redis is unavailable");
    }

    @Test
    void processJobSkipsWhenDoneMarkerExists() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);
        Job job = activeJob(jobId, JobType.WEBHOOK);

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(jobService.findById(jobId)).thenReturn(job);
        when(redisTemplate.hasKey(Utilities.getDoneKey(job.getIdempotencyKey()))).thenReturn(true);

        workerService.processJob(event);

        verify(redisLockService, never()).acquireLock(anyString(), any(Duration.class));
        verify(jobService).findById(jobId);
    }

    @Test
    void processJobSkipsWhenLockIsNotAcquired() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);
        Job job = activeJob(jobId, JobType.WEBHOOK);

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(jobService.findById(jobId)).thenReturn(job);
        when(redisTemplate.hasKey(Utilities.getDoneKey(job.getIdempotencyKey()))).thenReturn(false);
        when(redisLockService.acquireLock(eq("job-lock:" + jobId), any(Duration.class))).thenReturn(null);

        workerService.processJob(event);

        verify(jobService).findById(jobId);
        verify(redisLockService, never()).releaseLock(anyString(), anyString());
    }

    @Test
    void processJobMarksOneTimeJobSuccess() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);
        Job job = activeJob(jobId, JobType.WEBHOOK);
        ExecutionLog executionLog = new ExecutionLog();
        String doneKey = Utilities.getDoneKey(job.getIdempotencyKey());

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(redisTemplate.hasKey(doneKey)).thenReturn(false);
        when(redisLockService.acquireLock(eq("job-lock:" + jobId), any(Duration.class))).thenReturn("token-1");
        when(jobService.findById(jobId)).thenReturn(job);
        when(jobService.maxAttemptsExceeded(jobId)).thenReturn(false);
        when(jobService.markJobStartingAtomic(jobId, "worker-test")).thenReturn(executionLog);

        workerService.processJob(event);

        verify(jobService).markJobStartingAtomic(jobId, "worker-test");
        verify(jobHandlerRouter).route(event);
        verify(jobService).markJobCompletedAtomic(jobId, executionLog, "worker-test");
        verify(valueOperations).set(doneKey, "true", Duration.ofHours(24));
        verify(redisLockService).releaseLock("job-lock:" + jobId, "token-1");
    }

    @Test
    void processJobSchedulesNextCronRunForCronJobs() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.CLEANUP);
        Job job = activeJob(jobId, JobType.CLEANUP);
        ExecutionLog executionLog = new ExecutionLog();

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(redisLockService.acquireLock(eq("job-lock:" + jobId), any(Duration.class))).thenReturn("token-2");
        when(jobService.findById(jobId)).thenReturn(job);
        when(jobService.maxAttemptsExceeded(jobId)).thenReturn(false);
        when(jobService.markJobStartingAtomic(jobId, "worker-test")).thenReturn(executionLog);
        when(jobService.hasCronExpression(job)).thenReturn(true);

        workerService.processJob(event);

        // markJobCompletedAtomic handles cron rescheduling internally based on the
        // job's cronExpression — the worker no longer branches on it.
        verify(jobService).markJobCompletedAtomic(jobId, executionLog, "worker-test");
        verify(valueOperations, never()).set(anyString(), eq("true"), any(Duration.class));
    }

    @Test
    void processJobMarksDeadForIllegalArgumentException() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);
        Job job = activeJob(jobId, JobType.WEBHOOK);
        ExecutionLog executionLog = new ExecutionLog();

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(redisTemplate.hasKey(Utilities.getDoneKey(job.getIdempotencyKey()))).thenReturn(false);
        when(redisLockService.acquireLock(eq("job-lock:" + jobId), any(Duration.class))).thenReturn("token-3");
        when(jobService.findById(jobId)).thenReturn(job);
        when(jobService.maxAttemptsExceeded(jobId)).thenReturn(false);
        when(jobService.markJobStartingAtomic(jobId, "worker-test")).thenReturn(executionLog);
        org.mockito.Mockito.doThrow(new IllegalArgumentException("bad payload")).when(jobHandlerRouter).route(event);

        workerService.processJob(event);

        verify(jobService).updateJobStatus(jobId, JobStatus.FAILED);
        verify(executionLogService).updateExecutionStatus(executionLog, JobStatus.FAILED, "bad payload", "worker-test");
        verify(jobService).markJobDead(jobId, "bad payload");
        verify(jobService, never()).scheduleRetry(any(), any(), anyString());
        verify(redisLockService).releaseLock("job-lock:" + jobId, "token-3");
    }

    @Test
    void processJobSchedulesRetryForRetryableFailure() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);
        Job job = activeJob(jobId, JobType.WEBHOOK);
        ExecutionLog executionLog = new ExecutionLog();

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(redisTemplate.hasKey(Utilities.getDoneKey(job.getIdempotencyKey()))).thenReturn(false);
        when(redisLockService.acquireLock(eq("job-lock:" + jobId), any(Duration.class))).thenReturn("token-4");
        when(jobService.findById(jobId)).thenReturn(job);
        when(jobService.maxAttemptsExceeded(jobId)).thenReturn(false);
        when(jobService.markJobStartingAtomic(jobId, "worker-test")).thenReturn(executionLog);
        when(jobService.getAttemptCount(jobId)).thenReturn(1L);
        org.mockito.Mockito.doThrow(new RuntimeException("temporary failure")).when(jobHandlerRouter).route(event);

        workerService.processJob(event);

        verify(jobService).updateJobStatus(jobId, JobStatus.FAILED);
        ArgumentCaptor<Instant> retryAtCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(jobService).scheduleRetry(eq(jobId), retryAtCaptor.capture(), eq("temporary failure"));
        assertThat(retryAtCaptor.getValue()).isAfter(Instant.now().minusSeconds(1));
        verify(jobService, never()).markJobDead(eq(jobId), anyString());
        verify(redisLockService).releaseLock("job-lock:" + jobId, "token-4");
    }

    @Test
    void processJobMarksDeadWhenAttemptsAlreadyExceeded() {
        UUID jobId = UUID.randomUUID();
        JobDispatchEvent event = new JobDispatchEvent(jobId, JobType.WEBHOOK);
        Job job = activeJob(jobId, JobType.WEBHOOK);

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(redisTemplate.hasKey(Utilities.getDoneKey(job.getIdempotencyKey()))).thenReturn(false);
        when(redisLockService.acquireLock(eq("job-lock:" + jobId), any(Duration.class))).thenReturn("token-5");
        when(jobService.findById(jobId)).thenReturn(job);
        when(jobService.maxAttemptsExceeded(jobId)).thenReturn(true);

        workerService.processJob(event);

        verify(jobService).markJobDead(jobId, "Max attempts exceeded");
        verify(jobService, never()).markJobStartingAtomic(any(), anyString());
        verify(redisLockService).releaseLock("job-lock:" + jobId, "token-5");
    }

    private Job activeJob(UUID id, JobType type) {
        Job job = new Job();
        job.setId(id);
        job.setJobType(type);
        job.setJobPriority(JobPriority.MEDIUM);
        job.setJobStatus(JobStatus.QUEUED);
        job.setIdempotencyKey(type.name().toLowerCase() + "-" + id);
        return job;
    }
}
