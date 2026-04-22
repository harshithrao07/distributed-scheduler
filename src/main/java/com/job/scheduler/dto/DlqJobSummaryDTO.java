package com.job.scheduler.dto;

import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;

import java.time.Instant;
import java.util.UUID;

public record DlqJobSummaryDTO(
        UUID jobId,
        JobType jobType,
        JobPriority jobPriority,
        String lastErrorMessage,
        long attemptCount,
        DeadLetterStatus deadLetterStatus,
        Instant deadLetterQueuedAt,
        Instant deadLetterSentAt,
        Instant deadLetterLastAttemptAt,
        Instant nextDeadLetterAttemptAt,
        String deadLetterErrorMessage,
        UUID requeuedFromJobId,
        Instant requeuedAt,
        Instant createdAt,
        Instant updatedAt
) {
}
