package com.job.scheduler.dto;

import tools.jackson.databind.JsonNode;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;

import java.time.Instant;
import java.util.UUID;

public record JobDetailDTO(
        UUID id,
        JobType jobType,
        JobStatus jobStatus,
        JobPriority jobPriority,
        JsonNode payload,
        String cronExpression,
        int maxAttempts,
        String idempotencyKey,
        Instant nextRunAt,
        Instant queuedAt,
        String lastErrorMessage,
        Instant startedAt,
        Instant completedAt,
        UUID requeuedFromJobId,
        Instant requeuedAt,
        DeadLetterStatus deadLetterStatus,
        Instant deadLetterQueuedAt,
        Instant deadLetterSentAt,
        Instant deadLetterLastAttemptAt,
        Instant nextDeadLetterAttemptAt,
        String deadLetterErrorMessage,
        Instant createdAt,
        Instant updatedAt
) {
}
