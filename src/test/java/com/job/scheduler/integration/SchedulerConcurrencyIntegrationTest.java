package com.job.scheduler.integration;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.service.WorkerService;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SchedulerConcurrencyIntegrationTest extends AbstractSchedulerFlowIntegrationTest {

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private WorkerService workerService;

    @Test
    void concurrentClaimingDoesNotDoubleClaimSameJob() throws Exception {
        saveCleanupJob(JobStatus.PENDING, Instant.now().minusSeconds(20), "claim-race-1-" + UUID.randomUUID());
        saveCleanupJob(JobStatus.PENDING, Instant.now().minusSeconds(10), "claim-race-2-" + UUID.randomUUID());

        Instant now = Instant.now();
        Instant retryAt = now.plusSeconds(30);
        CyclicBarrier barrier = new CyclicBarrier(2);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<List<UUID>> claimTask = () -> {
                barrier.await(5, TimeUnit.SECONDS);
                return jobRepository.claimDueJobsForDispatch(now, retryAt, 1)
                        .stream()
                        .map(job -> job.getId())
                        .toList();
            };

            Future<List<UUID>> firstFuture = executor.submit(claimTask);
            Future<List<UUID>> secondFuture = executor.submit(claimTask);

            List<UUID> firstClaim = firstFuture.get(10, TimeUnit.SECONDS);
            List<UUID> secondClaim = secondFuture.get(10, TimeUnit.SECONDS);

            assertThat(firstClaim).hasSize(1);
            assertThat(secondClaim).hasSize(1);
            assertThat(Set.copyOf(firstClaim)).doesNotContainAnyElementsOf(secondClaim);

            entityManager.clear();
            var jobs = jobRepository.findAll();
            assertThat(jobs).hasSize(2);
            assertThat(jobs)
                    .allMatch(job -> Math.abs(job.getNextRunAt().toEpochMilli() - retryAt.toEpochMilli()) <= 1000);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentWorkersAllowOnlyOneExecutionForSameQueuedJob() throws Exception {
        UUID jobId = submitCleanupJob(null, "worker-race-" + UUID.randomUUID());
        jobService.markDispatchSucceeded(jobId);

        CountDownLatch firstInvocationEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> handlerThreads = new ConcurrentLinkedQueue<>();

        doAnswer(invocation -> {
            handlerThreads.add(Thread.currentThread().getName());
            firstInvocationEntered.countDown();
            if (!releaseHandler.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to release handler execution");
            }
            return null;
        }).when(jobHandlerRouter).route(any(JobDispatchEvent.class));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CyclicBarrier barrier = new CyclicBarrier(2);
            Runnable workerCall = () -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    workerService.processJob(new JobDispatchEvent(jobId, JobType.CLEANUP));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            Future<?> firstFuture = executor.submit(workerCall);
            Future<?> secondFuture = executor.submit(workerCall);

            assertThat(firstInvocationEntered.await(5, TimeUnit.SECONDS)).isTrue();
            releaseHandler.countDown();

            firstFuture.get(10, TimeUnit.SECONDS);
            secondFuture.get(10, TimeUnit.SECONDS);

            var processed = waitForJob(
                    jobId,
                    job -> job.getJobStatus() == JobStatus.SUCCESS,
                    Duration.ofSeconds(10),
                    "only one worker to finish processing the queued job"
            );

            assertThat(processed.getCompletedAt()).isNotNull();
            assertThat(executionLogRepository.countByJobId(jobId)).isEqualTo(1);
            assertThat(handlerThreads).hasSize(1);
            verify(jobHandlerRouter, times(1)).route(any(JobDispatchEvent.class));
        } finally {
            releaseHandler.countDown();
            executor.shutdownNow();
        }
    }
}
