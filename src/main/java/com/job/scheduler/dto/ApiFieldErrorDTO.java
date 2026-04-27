package com.job.scheduler.dto;

public record ApiFieldErrorDTO(
        String field,
        String message
) {
}
