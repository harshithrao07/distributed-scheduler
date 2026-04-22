package com.job.scheduler.service;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.exception.RedisUnavailableException;
import com.job.scheduler.handlers.JobHandlerRouter;
import com.job.scheduler.utility.Utilities;
import jakarta.annotation.PreDestroy;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkerService {
    private final RedisTemplate<String, String> redisTemplate;
    private final JobService jobService;
    private final ExecutionLogService executionLogService;
    private final JobHandlerRouter jobHandlerRouter;
    private final RedisLockService redisLockService;
    private final RedisHealthService redisHealthService;
    private final ScheduledExecutorService renewExecutor = Executors.newSingleThreadScheduledExecutor();

    @Value("${scheduler.worker-id}")
    private String workerId;

    @Value("${scheduler.retry.base-delay-ms:1000}")
    private long retryBaseDelayMs;

    @Value("${scheduler.retry.max-delay-ms:30000}")
    private long retryMaxDelayMs;

    public void processJob(JobDispatchEvent jobDispatchEvent) {
        if (!redisHealthService.isRedisAvailable()) {
            throw new RedisUnavailableException("Redis is unavailable. Worker is paused from processing jobs.");
        }

        UUID jobId = jobDispatchEvent.jobId();

        String doneKey = Utilities.getDoneKey(jobId);
        String lockKey = Utilities.getLockKey(jobId);

        // Idempotency Check
        if (hasDoneMarker(doneKey, jobId)) return;

        // Acquire the lock
        String lockToken = acquireLock(lockKey, jobId);

        // If lock not acquired then job is already processed by some other worker
        if (lockToken == null) return;

        ScheduledFuture<?> renewTask = renewExecutor.scheduleAtFixedRate(
                () -> redisLockService.renewLock(lockKey, lockToken, Duration.ofSeconds(30)),
                10,
                10,
                TimeUnit.SECONDS
        );
        ExecutionLog executionLog = null;

        try {
            Job job = jobService.findById(jobId);
            if (shouldSkip(job)) {
                return;
            }

            // Check if the job has crossed the max retry value
            // If crossed then manual intervention is required
            if (jobService.maxRetriesExceeded(jobId)) {
                // Mark the job as DEAD
                jobService.markJobDead(jobId, "Max retry attempts exceeded");
                return;
            }

            // Update the job status from PENDING to RUNNING
            jobService.updateJobStatus(jobId, JobStatus.RUNNING);

            // Create a new ExecutionLog entry
            executionLog = executionLogService.createEntry(jobId);

            // Update execution status to RUNNING
            executionLogService.updateExecutionStatus(executionLog, JobStatus.RUNNING, null, workerId);

            // Route it to right per type handler
            jobHandlerRouter.route(jobDispatchEvent);

            // Update execution status and job status to SUCCESS
            executionLogService.updateExecutionStatus(executionLog, JobStatus.SUCCESS, null, workerId);
            if (jobService.hasCronExpression(job)) {
                jobService.scheduleNextCronRun(jobId);
            } else {
                jobService.updateJobStatus(jobId, JobStatus.SUCCESS);

                // Set done key on success
                markJobDoneBestEffort(doneKey, jobId);
            }
        } catch (Exception e) {
            jobService.updateJobStatus(jobId, JobStatus.FAILED);
            if (executionLog != null) {
                executionLogService.updateExecutionStatus(executionLog, JobStatus.FAILED, e.getMessage(), workerId);
            }

            if (e instanceof EntityNotFoundException || e instanceof IllegalArgumentException
            || jobService.maxRetriesExceeded(jobId)) {
                // Mark the job as DEAD
                jobService.markJobDead(jobId, e.getMessage());
            } else {
                jobService.scheduleRetry(jobId, Instant.now().plus(retryDelay(jobId)), e.getMessage());
            }
        } finally {
            renewTask.cancel(true);
            releaseLockBestEffort(lockKey, lockToken, jobId);
        }
    }

    private boolean shouldSkip(Job job) {
        if (job.getJobStatus() == JobStatus.SUCCESS
                || job.getJobStatus() == JobStatus.DEAD
                || job.getJobStatus() == JobStatus.CANCELED) {
            return true;
        }

        if (job.getJobStatus() == JobStatus.RUNNING) {
            return true;
        }

        return job.getJobStatus() == JobStatus.PENDING
                && job.getNextRunAt() != null
                && job.getNextRunAt().isAfter(Instant.now());
    }

    private Duration retryDelay(UUID jobId) {
        long attemptCount = jobService.getAttemptCount(jobId);
        long exponent = Math.max(0, attemptCount - 1);
        long multiplier = 1L << Math.min(exponent, 30);
        long delayMs = Math.min(retryBaseDelayMs * multiplier, retryMaxDelayMs);
        return Duration.ofMillis(delayMs);
    }

    private boolean hasDoneMarker(String doneKey, UUID jobId) {
        try {
            return Boolean.TRUE.equals(redisTemplate.hasKey(doneKey));
        } catch (RuntimeException e) {
            throw new RedisUnavailableException("Could not check Redis done marker for job " + jobId, e);
        }
    }

    private String acquireLock(String lockKey, UUID jobId) {
        try {
            return redisLockService.acquireLock(lockKey, Duration.ofSeconds(30));
        } catch (RuntimeException e) {
            throw new RedisUnavailableException("Could not acquire Redis lock for job " + jobId, e);
        }
    }

    private void markJobDoneBestEffort(String doneKey, UUID jobId) {
        try {
            redisTemplate.opsForValue().set(doneKey, "true", Duration.ofHours(24));
        } catch (RuntimeException e) {
            log.warn("Job {} succeeded, but Redis done marker could not be written. Duplicate Kafka messages may rely on DB status.", jobId, e);
        }
    }

    private void releaseLockBestEffort(String lockKey, String lockToken, UUID jobId) {
        try {
            redisLockService.releaseLock(lockKey, lockToken);
        } catch (RuntimeException e) {
            log.warn("Could not release Redis lock for job {}. Lock will expire by TTL.", jobId, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        renewExecutor.shutdownNow();
    }

}
