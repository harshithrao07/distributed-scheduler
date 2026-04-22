package com.job.scheduler.dto;

import com.job.scheduler.enums.JobStatus;

import java.time.Instant;
import java.util.UUID;

public record ExecutionLogDTO(
        UUID id,
        UUID jobId,
        int attemptNumber,
        JobStatus executionStatus,
        Instant startedAt,
        Instant completedAt,
        Long durationMs,
        String errorMessage,
        String workerId,
        Instant createdAt
) {
}
