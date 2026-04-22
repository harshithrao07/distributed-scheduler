package com.job.scheduler.scheduler;

import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.producers.JobQueueProducer;
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
public class DeadLetterSchedulerService {
    private final JobRepository jobRepository;
    private final JobService jobService;
    private final JobQueueProducer jobQueueProducer;

    @Value("${scheduler.dead-letter.dispatch-retry-delay-ms:30000}")
    private long deadLetterDispatchRetryDelayMs;

    @Scheduled(fixedDelayString = "${scheduler.dead-letter.poll-delay-ms:30000}")
    public void publishPendingDeadLetters() {
        List<Job> jobs = jobRepository.findByJobStatusAndDeadLetterStatusAndNextDeadLetterAttemptAtLessThanEqual(
                JobStatus.DEAD,
                DeadLetterStatus.PENDING,
                Instant.now()
        );

        for (Job job : jobs) {
            jobService.markDeadLetterAttempt(job.getId(), Instant.now().plusMillis(deadLetterDispatchRetryDelayMs));
            JobDispatchEvent event = new JobDispatchEvent(job.getId(), job.getJobType());

            jobQueueProducer.sendToDlq(event).whenComplete((ignored, ex) -> {
                if (ex == null) {
                    jobService.markDeadLetterSent(job.getId());
                } else {
                    jobService.markDeadLetterRetry(
                            job.getId(),
                            Instant.now().plusMillis(deadLetterDispatchRetryDelayMs),
                            ex.getMessage()
                    );
                }
            });
        }
    }
}
