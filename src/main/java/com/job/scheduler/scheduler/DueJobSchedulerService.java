package com.job.scheduler.scheduler;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.producers.JobQueueProducer;
import com.job.scheduler.repository.JobRepository;
import com.job.scheduler.service.JobService;
import com.job.scheduler.service.RedisHealthService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class DueJobSchedulerService {
    private final JobRepository jobRepository;
    private final JobService jobService;
    private final JobQueueProducer jobQueueProducer;
    private final RedisHealthService redisHealthService;

    @Value("${scheduler.due-job.dispatch-retry-delay-ms:5000}")
    private long dispatchRetryDelayMs;

    // Pending Jobs

    @Scheduled(fixedDelayString = "${scheduler.due-job.poll-delay-ms:${scheduler.retry.poll-delay-ms:1000}}")
    public void dispatchDueJobs() {
        if (!redisHealthService.isRedisAvailable()) {
            return;
        }

        List<Job> dueJobs = jobRepository.findByJobStatusAndNextRunAtLessThanEqual(JobStatus.PENDING, Instant.now());

        for (Job job : dueJobs) {
            jobService.markDispatchAttempt(job.getId(), Instant.now().plusMillis(dispatchRetryDelayMs));
            JobDispatchEvent event = new JobDispatchEvent(job.getId(), job.getJobType());
            CompletableFuture<Void> sendFuture;

            if (job.getJobPriority() == JobPriority.HIGH) {
                sendFuture = jobQueueProducer.sendToHighPriorityQueue(event);
            } else {
                sendFuture = jobQueueProducer.sendToMainQueue(event);
            }

            sendFuture.whenComplete((ignored, ex) -> {
                if (ex == null) {
                    jobService.markDispatchSucceeded(job.getId());
                }
            });
        }
    }
}
