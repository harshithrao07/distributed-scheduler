package com.job.scheduler.dto.payload;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record SendEmailPayload(
        @Email(message = "Invalid email address")
        @NotBlank(message = "Recipient email is required")
        String to,

        @NotBlank(message = "Subject is required")
        String subject,

        @NotBlank(message = "Body is required")
        String body
) {}