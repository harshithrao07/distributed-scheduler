package com.job.scheduler.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

public record JobRequestDTO(
        @NotNull(message = "Job Type cannot be null") JobType jobType,
        @NotNull(message = "Job Priority cannot be null") JobPriority jobPriority,
        @NotNull(message = "Payload cannot be null") JsonNode payload,
        String cronExpression,
        @PositiveOrZero(message = "Max attempts cannot be negative") Integer maxAttempts,
        @NotBlank(message = "Idempotency Key cannot be blank") String idempotencyKey
) {
}
