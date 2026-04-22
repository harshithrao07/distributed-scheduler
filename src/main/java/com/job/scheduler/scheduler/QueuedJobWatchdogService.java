package com.job.scheduler.scheduler;

import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.repository.JobRepository;
import com.job.scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class QueuedJobWatchdogService {
    private final JobRepository jobRepository;
    private final JobService jobService;

    @Value("${scheduler.queued-watchdog.timeout-ms:300000}")
    private long queuedTimeoutMs;

    // Jobs added to queue, but later queue went down so in DB state is QUEUED but its not really in queue

    @Scheduled(fixedDelayString = "${scheduler.queued-watchdog.poll-delay-ms:60000}")
    public void recoverStaleQueuedJobs() {
        Instant cutoff = Instant.now().minusMillis(queuedTimeoutMs);
        List<Job> staleQueuedJobs = jobRepository.findByJobStatusAndQueuedAtLessThanEqual(JobStatus.QUEUED, cutoff);

        for (Job job : staleQueuedJobs) {
            jobService.recoverQueuedJob(job.getId());
        }
    }
}
