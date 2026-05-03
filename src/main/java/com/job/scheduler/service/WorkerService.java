package com.job.scheduler.service;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.exception.RedisUnavailableException;
import com.job.scheduler.handlers.JobHandlerRouter;
import com.job.scheduler.monitoring.events.JobExecutedEvent;
import com.job.scheduler.utility.Utilities;
import jakarta.annotation.PreDestroy;
import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
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
public class WorkerService {
    private final RedisTemplate<String, String> redisTemplate;
    private final JobService jobService;
    private final ExecutionLogService executionLogService;
    private final JobHandlerRouter jobHandlerRouter;
    private final RedisLockService redisLockService;
    private final RedisHealthService redisHealthService;
    private final ApplicationEventPublisher eventPublisher;
    private final ScheduledExecutorService renewExecutor = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    public WorkerService(
            RedisTemplate<String, String> redisTemplate,
            JobService jobService,
            ExecutionLogService executionLogService,
            JobHandlerRouter jobHandlerRouter,
            RedisLockService redisLockService,
            RedisHealthService redisHealthService,
            ApplicationEventPublisher eventPublisher
    ) {
        this.redisTemplate = redisTemplate;
        this.jobService = jobService;
        this.executionLogService = executionLogService;
        this.jobHandlerRouter = jobHandlerRouter;
        this.redisLockService = redisLockService;
        this.redisHealthService = redisHealthService;
        this.eventPublisher = eventPublisher;
    }

    public WorkerService(
            RedisTemplate<String, String> redisTemplate,
            JobService jobService,
            ExecutionLogService executionLogService,
            JobHandlerRouter jobHandlerRouter,
            RedisLockService redisLockService,
            RedisHealthService redisHealthService
    ) {
        this(
                redisTemplate,
                jobService,
                executionLogService,
                jobHandlerRouter,
                redisLockService,
                redisHealthService,
                event -> {
                }
        );
    }

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
        String lockKey = Utilities.getLockKey(jobId);
        Job job = jobService.findById(jobId);
        String doneKey = shouldUseDoneMarker(job) ? Utilities.getDoneKey(job.getIdempotencyKey()) : null;

        ScheduledFuture<?> renewTask = null;
        ExecutionLog executionLog = null;
        String lockToken = null;

        try {
            if (hasDoneMarker(doneKey, jobId)) {
                return;
            }

            lockToken = acquireLock(lockKey, jobId);
            if (lockToken == null) {
                return;
            }

            String acquiredLockToken = lockToken;
            renewTask = renewExecutor.scheduleAtFixedRate(
                    () -> redisLockService.renewLock(lockKey, acquiredLockToken, Duration.ofSeconds(30)),
                    10,
                    10,
                    TimeUnit.SECONDS
            );

            if (shouldSkip(job)) {
                return;
            }

            // Check if the job has crossed the max attempt value.
            if (jobService.maxAttemptsExceeded(jobId)) {
                // Mark the job as DEAD
                jobService.markJobDead(jobId, "Max attempts exceeded");
                return;
            }

            // Atomic: flip job to RUNNING + create execution log + log->RUNNING in one tx
            executionLog = jobService.markJobStartingAtomic(jobId, workerId);

            // Route it to right per type handler
            Instant executionStartedAt = Instant.now();
            jobHandlerRouter.route(jobDispatchEvent);
            publish(new JobExecutedEvent(
                    job.getJobType(),
                    job.getJobPriority(),
                    JobStatus.SUCCESS,
                    Duration.between(executionStartedAt, Instant.now())
            ));

            handleSuccessfulExecution(jobId, executionLog, doneKey);
        } catch (Exception e) {
            handleFailedExecution(job, jobId, executionLog, e);
        } finally {
            if (renewTask != null) {
                renewTask.cancel(true);
            }
            releaseLockBestEffort(lockKey, lockToken, jobId);
        }
    }

    private void handleSuccessfulExecution(UUID jobId, ExecutionLog executionLog, String doneKey) {
        // One transaction: execution log -> SUCCESS + (job -> SUCCESS or schedule next cron run)
        jobService.markJobCompletedAtomic(jobId, executionLog, workerId);
        markJobDoneBestEffort(doneKey, jobId);
    }

    private void handleFailedExecution(Job job, UUID jobId, ExecutionLog executionLog, Exception e) {
        jobService.updateJobStatus(jobId, JobStatus.FAILED);
        if (executionLog != null) {
            executionLogService.updateExecutionStatus(executionLog, JobStatus.FAILED, e.getMessage(), workerId);
            publish(new JobExecutedEvent(
                    job.getJobType(),
                    job.getJobPriority(),
                    JobStatus.FAILED,
                    durationSince(executionLog.getStartedAt())
            ));
        }

        if (shouldMarkDead(jobId, e)) {
            jobService.markJobDead(jobId, e.getMessage());
            return;
        }

        jobService.scheduleRetry(jobId, Instant.now().plus(retryDelay(jobId)), e.getMessage());
    }

    private boolean shouldMarkDead(UUID jobId, Exception e) {
        return e instanceof EntityNotFoundException
                || e instanceof IllegalArgumentException
                || jobService.maxAttemptsExceeded(jobId);
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

    private Duration durationSince(Instant startedAt) {
        if (startedAt == null) {
            return Duration.ZERO;
        }
        return Duration.between(startedAt, Instant.now());
    }

    private boolean hasDoneMarker(String doneKey, UUID jobId) {
        if (doneKey == null) {
            return false;
        }

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
        if (doneKey == null) {
            return;
        }

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

    private boolean shouldUseDoneMarker(Job job) {
        return !jobService.hasCronExpression(job);
    }

    private void publish(Object event) {
        eventPublisher.publishEvent(event);
    }

}
