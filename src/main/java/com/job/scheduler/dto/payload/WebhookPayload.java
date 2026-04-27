package com.job.scheduler.dto.payload;

import tools.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;

public record WebhookPayload(
        @NotBlank(message = "URL is required")
        String url,

        JsonNode body
) {}
