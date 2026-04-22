package com.job.scheduler.dto;

import com.job.scheduler.enums.JobStatus;

import java.util.UUID;

public record CancelJobResponseDTO(
        UUID jobId,
        JobStatus status
) {
}
