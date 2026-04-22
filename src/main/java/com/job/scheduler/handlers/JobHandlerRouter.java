package com.job.scheduler.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.job.scheduler.dto.JobDispatchEvent;
import com.job.scheduler.dto.payload.*;
import com.job.scheduler.entity.Job;
import com.job.scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class JobHandlerRouter {

    private final ObjectMapper objectMapper;
    private final JobService jobService;
    private final SendEmailHandler sendEmailHandler;
    private final WebhookHandler webhookHandler;
    private final CleanupHandler cleanupHandler;

    public void route(JobDispatchEvent jobDispatchEvent) {
        Job job = jobService.findById(jobDispatchEvent.jobId());

        switch (job.getJobType()) {
            case SEND_EMAIL -> sendEmailHandler.handle(
                    readPayload(job.getPayload(), SendEmailPayload.class)
            );
            case WEBHOOK -> webhookHandler.handle(
                    readPayload(job.getPayload(), WebhookPayload.class)
            );
            case CLEANUP -> cleanupHandler.handle(
                    readPayload(job.getPayload(), CleanupPayload.class)
            );
            default -> throw new IllegalArgumentException("Unsupported job type: " + job.getJobType());
        }
    }

    private <T> T readPayload(JsonNode payload, Class<T> payloadClass) {
        try {
            return objectMapper.treeToValue(payload, payloadClass);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Payload does not match expected shape for " + payloadClass.getSimpleName(), e
            );
        }
    }
}
