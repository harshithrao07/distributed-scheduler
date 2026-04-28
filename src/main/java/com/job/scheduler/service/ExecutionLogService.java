package com.job.scheduler.service;

import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.repository.ExecutionLogRepository;
import com.job.scheduler.repository.JobRepository;
import jakarta.persistence.EntityNotFoundException;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ExecutionLogService {
    private final ExecutionLogRepository executionLogRepository;
    private final JobRepository jobRepository;

    public ExecutionLog createEntry(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));

        ExecutionLog executionLog = new ExecutionLog();
        executionLog.setJobDetails(job);
        Long retryCount = executionLogRepository.countByJobId(jobId);
        executionLog.setAttemptNumber((int) (retryCount + 1));
        executionLog.setExecutionStatus(JobStatus.PENDING);

        return executionLogRepository.save(executionLog);
    }

    @Transactional
    public void updateExecutionStatus(
            ExecutionLog executionLog,
            JobStatus jobStatus,
            String errorMessage,
            String workerId
    ) {
        executionLog.setExecutionStatus(jobStatus);
        executionLog.setWorkerId(workerId);

        switch (jobStatus) {
            case RUNNING -> {
                executionLog.setStartedAt(Instant.now());
                executionLog.setCompletedAt(null);
                executionLog.setDurationMs(null);
                executionLog.setErrorMessage(null);
            }

            case FAILED, SUCCESS -> {
                executionLog.setErrorMessage(jobStatus == JobStatus.FAILED ? errorMessage : null);

                Instant completedAt = Instant.now();
                executionLog.setCompletedAt(completedAt);

                if (executionLog.getStartedAt() != null) {
                    executionLog.setDurationMs(
                            Duration.between(executionLog.getStartedAt(), completedAt).toMillis()
                    );
                }
            }

            case PENDING, QUEUED, CANCELED, DEAD -> {
                // No additional execution-log timestamps are updated for these states.
            }
        }

        executionLogRepository.save(executionLog);
    }

}
