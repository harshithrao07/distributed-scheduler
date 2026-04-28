package com.job.scheduler.exception;

import com.job.scheduler.controller.JobController;
import com.job.scheduler.service.JobService;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.UUID;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class ApiExceptionHandlerTest {

    @Mock
    private JobService jobService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders
                .standaloneSetup(new JobController(jobService))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void invalidRequestBodyReturnsValidationError() throws Exception {
        String body = """
                {
                  "jobPriority": "MEDIUM",
                  "payload": {},
                  "idempotencyKey": ""
                }
                """;

        mockMvc.perform(post("/app/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.message").value("Request validation failed"))
                .andExpect(jsonPath("$.path").value("/app/v1/jobs"))
                .andExpect(jsonPath("$.fieldErrors.length()").value(2));
    }

    @Test
    void entityNotFoundReturnsNotFoundResponse() throws Exception {
        UUID jobId = UUID.randomUUID();
        when(jobService.getJob(jobId)).thenThrow(new EntityNotFoundException("Job does not exist"));

        mockMvc.perform(get("/app/v1/jobs/{jobId}", jobId))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("RESOURCE_NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("Job does not exist"));
    }

    @Test
    void illegalStateReturnsConflictResponse() throws Exception {
        UUID jobId = UUID.randomUUID();
        when(jobService.cancelJob(jobId)).thenThrow(new IllegalStateException("RUNNING jobs cannot be canceled"));

        mockMvc.perform(post("/app/v1/jobs/{jobId}/cancel", jobId))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("INVALID_STATE"))
                .andExpect(jsonPath("$.message").value("RUNNING jobs cannot be canceled"));
    }

    @Test
    void badEnumQueryParameterReturnsBadRequest() throws Exception {
        mockMvc.perform(get("/app/v1/jobs").param("status", "NOT_A_STATUS"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("BAD_REQUEST"))
                .andExpect(jsonPath("$.path").value("/app/v1/jobs"));
    }

    @Test
    void unexpectedExceptionReturnsInternalServerError() throws Exception {
        UUID jobId = UUID.randomUUID();
        when(jobService.getJob(jobId)).thenThrow(new RuntimeException("boom"));

        mockMvc.perform(get("/app/v1/jobs/{jobId}", jobId))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("INTERNAL_SERVER_ERROR"))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred"));
    }
}
