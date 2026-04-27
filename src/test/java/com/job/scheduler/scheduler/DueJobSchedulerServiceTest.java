package com.job.scheduler.scheduler;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.producers.JobQueueProducer;
import com.job.scheduler.repository.JobRepository;
import com.job.scheduler.service.JobService;
import com.job.scheduler.service.RedisHealthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DueJobSchedulerServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobService jobService;

    @Mock
    private JobQueueProducer jobQueueProducer;

    @Mock
    private RedisHealthService redisHealthService;

    private DueJobSchedulerService dueJobSchedulerService;

    @BeforeEach
    void setUp() {
        dueJobSchedulerService = new DueJobSchedulerService(
                jobRepository,
                jobService,
                jobQueueProducer,
                redisHealthService
        );

        ReflectionTestUtils.setField(dueJobSchedulerService, "dispatchRetryDelayMs", 5000L);
        ReflectionTestUtils.setField(dueJobSchedulerService, "claimLimit", 25);
    }

    @Test
    void dispatchDueJobsDoesNothingWhenRedisIsUnavailable() {
        when(redisHealthService.isRedisAvailable()).thenReturn(false);

        dueJobSchedulerService.dispatchDueJobs();

        verify(jobRepository, never()).claimDueJobsForDispatch(any(), any(), any(Integer.class));
        verify(jobQueueProducer, never()).sendToMainQueue(any());
        verify(jobQueueProducer, never()).sendToHighPriorityQueue(any());
        verify(jobService, never()).markDispatchSucceeded(any());
    }

    @Test
    void dispatchDueJobsClaimsJobsUsingConfiguredRetryWindowAndLimit() {
        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(jobRepository.claimDueJobsForDispatch(any(), any(), any(Integer.class))).thenReturn(List.of());

        dueJobSchedulerService.dispatchDueJobs();

        ArgumentCaptor<Instant> nowCaptor = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> retryCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(jobRepository).claimDueJobsForDispatch(nowCaptor.capture(), retryCaptor.capture(), eq(25));

        long diffMs = retryCaptor.getValue().toEpochMilli() - nowCaptor.getValue().toEpochMilli();
        assertThat(diffMs).isBetween(5000L, 5100L);
    }

    @Test
    void dispatchDueJobsSendsHighPriorityJobsToHighPriorityQueue() {
        UUID jobId = UUID.randomUUID();
        Job job = job(jobId, JobPriority.HIGH, JobType.WEBHOOK);

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(jobRepository.claimDueJobsForDispatch(any(), any(), any(Integer.class))).thenReturn(List.of(job));
        when(jobQueueProducer.sendToHighPriorityQueue(any(JobDispatchEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        dueJobSchedulerService.dispatchDueJobs();

        ArgumentCaptor<JobDispatchEvent> eventCaptor = ArgumentCaptor.forClass(JobDispatchEvent.class);
        verify(jobQueueProducer).sendToHighPriorityQueue(eventCaptor.capture());
        verify(jobQueueProducer, never()).sendToMainQueue(any());
        verify(jobService).markDispatchSucceeded(jobId);

        JobDispatchEvent event = eventCaptor.getValue();
        assertThat(event.jobId()).isEqualTo(jobId);
        assertThat(event.jobType()).isEqualTo(JobType.WEBHOOK);
    }

    @Test
    void dispatchDueJobsSendsNonHighPriorityJobsToMainQueue() {
        UUID jobId = UUID.randomUUID();
        Job job = job(jobId, JobPriority.MEDIUM, JobType.CLEANUP);

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(jobRepository.claimDueJobsForDispatch(any(), any(), any(Integer.class))).thenReturn(List.of(job));
        when(jobQueueProducer.sendToMainQueue(any(JobDispatchEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        dueJobSchedulerService.dispatchDueJobs();

        ArgumentCaptor<JobDispatchEvent> eventCaptor = ArgumentCaptor.forClass(JobDispatchEvent.class);
        verify(jobQueueProducer).sendToMainQueue(eventCaptor.capture());
        verify(jobQueueProducer, never()).sendToHighPriorityQueue(any());
        verify(jobService).markDispatchSucceeded(jobId);

        JobDispatchEvent event = eventCaptor.getValue();
        assertThat(event.jobId()).isEqualTo(jobId);
        assertThat(event.jobType()).isEqualTo(JobType.CLEANUP);
    }

    @Test
    void dispatchDueJobsDoesNotMarkJobQueuedWhenKafkaSendFails() {
        UUID jobId = UUID.randomUUID();
        Job job = job(jobId, JobPriority.LOW, JobType.WEBHOOK);

        when(redisHealthService.isRedisAvailable()).thenReturn(true);
        when(jobRepository.claimDueJobsForDispatch(any(), any(), any(Integer.class))).thenReturn(List.of(job));
        when(jobQueueProducer.sendToMainQueue(any(JobDispatchEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka unavailable")));

        dueJobSchedulerService.dispatchDueJobs();

        verify(jobQueueProducer).sendToMainQueue(any(JobDispatchEvent.class));
        verify(jobService, never()).markDispatchSucceeded(jobId);
    }

    private Job job(UUID id, JobPriority priority, JobType type) {
        Job job = new Job();
        job.setId(id);
        job.setJobPriority(priority);
        job.setJobType(type);
        return job;
    }
}
