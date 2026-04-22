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
public class RunningJobWatchdogService {
    private final JobRepository jobRepository;
    private final JobService jobService;

    @Value("${scheduler.running-watchdog.timeout-ms:600000}")
    private long runningTimeoutMs;

    // Jobs stuck in running for a while

    @Scheduled(fixedDelayString = "${scheduler.running-watchdog.poll-delay-ms:60000}")
    public void recoverStaleRunningJobs() {
        Instant cutoff = Instant.now().minusMillis(runningTimeoutMs);
        List<Job> staleRunningJobs = jobRepository.findByJobStatusAndStartedAtLessThanEqual(JobStatus.RUNNING, cutoff);

        for (Job job : staleRunningJobs) {
            jobService.recoverRunningJob(job.getId());
        }
    }
}
