package com.job.scheduler.integration;

import com.job.scheduler.dto.JobRequestDTO;
import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.producers.JobQueueProducer;
import com.job.scheduler.repository.ExecutionLogRepository;
import com.job.scheduler.repository.JobRepository;
import com.job.scheduler.service.JobService;
import com.job.scheduler.utility.Utilities;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Predicate;

abstract class AbstractSchedulerFlowIntegrationTest extends AbstractSchedulerContainersIntegrationTest {

    @Autowired
    protected JobService jobService;

    @Autowired
    protected JobRepository jobRepository;

    @Autowired
    protected ExecutionLogRepository executionLogRepository;

    @Autowired
    protected RedisTemplate<String, String> redisTemplate;

    @Autowired
    protected ObjectMapper objectMapper;

    @MockitoSpyBean
    protected com.job.scheduler.handlers.JobHandlerRouter jobHandlerRouter;

    @MockitoSpyBean
    protected JobQueueProducer jobQueueProducer;

    @BeforeEach
    void resetPersistentState() {
        executionLogRepository.deleteAll();
        jobRepository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        Mockito.reset(jobHandlerRouter, jobQueueProducer);
    }

    protected UUID submitCleanupJob(String cronExpression, String idempotencyKey) {
        return jobService.submitJob(new JobRequestDTO(
                JobType.CLEANUP,
                JobPriority.MEDIUM,
                objectMapper.createObjectNode().put("olderThanDays", 1),
                cronExpression,
                3,
                idempotencyKey
        ));
    }

    protected Job saveCleanupJob(JobStatus status, Instant nextRunAt, String idempotencyKey) {
        Job job = new Job();
        job.setJobType(JobType.CLEANUP);
        job.setJobStatus(status);
        job.setJobPriority(JobPriority.MEDIUM);
        job.setPayload(objectMapper.createObjectNode().put("olderThanDays", 1).toString());
        job.setIdempotencyKey(idempotencyKey);
        job.setNextRunAt(nextRunAt);
        return jobRepository.saveAndFlush(job);
    }

    protected Job waitForJob(UUID jobId, Predicate<Job> predicate, Duration timeout, String expectation) {
        Instant deadline = Instant.now().plus(timeout);
        Job latest = null;

        while (Instant.now().isBefore(deadline)) {
            latest = jobRepository.findById(jobId).orElseThrow();
            if (predicate.test(latest)) {
                return latest;
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for " + expectation, e);
            }
        }

        throw new AssertionError("Timed out waiting for " + expectation
                + " for job " + jobId
                + ". Last known status was " + (latest != null ? latest.getJobStatus() : "unknown")
                + ", done marker present=" + redisTemplate.hasKey(Utilities.getDoneKey(jobId)));
    }

    protected ExecutionLog waitForExecutionLog(UUID jobId, Predicate<ExecutionLog> predicate, Duration timeout, String expectation) {
        Instant deadline = Instant.now().plus(timeout);
        ExecutionLog latest = null;

        while (Instant.now().isBefore(deadline)) {
            var logs = executionLogRepository.findByJobIdOrderByAttemptNumberAsc(jobId);
            if (!logs.isEmpty()) {
                latest = logs.get(logs.size() - 1);
                if (predicate.test(latest)) {
                    return latest;
                }
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for " + expectation, e);
            }
        }

        throw new AssertionError("Timed out waiting for " + expectation
                + " for job " + jobId
                + ". Last known execution status was " + (latest != null ? latest.getExecutionStatus() : "unknown"));
    }
}
