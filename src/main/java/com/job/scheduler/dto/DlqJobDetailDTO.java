package com.job.scheduler.dto;

import tools.jackson.databind.JsonNode;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record DlqJobDetailDTO(
        UUID jobId,
        JobType jobType,
        JobPriority jobPriority,
        JsonNode payload,
        String cronExpression,
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
        Instant updatedAt,
        List<ExecutionLogDTO> executionLogs,
        boolean canRequeue,
        boolean canRetryDeadLetterPublish
) {
}
