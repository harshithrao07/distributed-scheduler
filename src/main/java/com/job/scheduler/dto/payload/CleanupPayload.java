package com.job.scheduler.dto.payload;

import jakarta.validation.constraints.Min;

public record CleanupPayload(
        @Min(value = 1, message = "olderThanDays must be at least 1")
        int olderThanDays
) {
}
