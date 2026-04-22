package com.job.scheduler.dto;

import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.enums.DeadLetterStatus;

import java.time.Instant;
import java.util.UUID;

public record JobSummaryDTO(
        UUID id,
        JobType jobType,
        JobStatus jobStatus,
        JobPriority jobPriority,
        String cronExpression,
        Instant nextRunAt,
        Instant queuedAt,
        Instant startedAt,
        Instant completedAt,
        UUID requeuedFromJobId,
        Instant requeuedAt,
        DeadLetterStatus deadLetterStatus,
        Instant deadLetterQueuedAt,
        Instant deadLetterSentAt,
        Instant nextDeadLetterAttemptAt,
        Instant createdAt,
        Instant updatedAt
) {
}
