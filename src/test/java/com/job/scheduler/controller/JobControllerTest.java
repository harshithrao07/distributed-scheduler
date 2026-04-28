package com.job.scheduler.controller;

import com.job.scheduler.dto.CancelJobResponseDTO;
import com.job.scheduler.dto.JobDetailDTO;
import com.job.scheduler.dto.JobPageDTO;
import com.job.scheduler.dto.JobSummaryDTO;
import com.job.scheduler.dto.RequeueJobResponseDTO;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.exception.ApiExceptionHandler;
import com.job.scheduler.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class JobControllerTest {

    @Mock
    private JobService jobService;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        mockMvc = MockMvcBuilders
                .standaloneSetup(new JobController(jobService))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void submitJobReturnsJobId() throws Exception {
        UUID jobId = UUID.randomUUID();
        when(jobService.submitJob(org.mockito.ArgumentMatchers.any())).thenReturn(jobId);

        String body = """
                {
                  "jobType": "WEBHOOK",
                  "jobPriority": "MEDIUM",
                  "payload": {
                    "url": "https://example.com/hook",
                    "body": {
                      "message": "ping"
                    }
                  },
                  "idempotencyKey": "controller-job-1"
                }
                """;

        mockMvc.perform(post("/app/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value(jobId.toString()));
    }

    @Test
    void getJobsReturnsPagedResponse() throws Exception {
        UUID jobId = UUID.randomUUID();
        Instant now = Instant.parse("2026-04-26T00:00:00Z");

        JobSummaryDTO summary = new JobSummaryDTO(
                jobId,
                JobType.WEBHOOK,
                JobStatus.PENDING,
                JobPriority.MEDIUM,
                null,
                now,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                now,
                now
        );

        JobPageDTO page = new JobPageDTO(List.of(summary), 0, 20, 1, 1, true, true);

        when(jobService.getJobs(
                JobStatus.PENDING,
                JobType.WEBHOOK,
                JobPriority.MEDIUM,
                null,
                null,
                0,
                20
        )).thenReturn(page);

        mockMvc.perform(get("/app/v1/jobs")
                        .param("status", "PENDING")
                        .param("type", "WEBHOOK")
                        .param("priority", "MEDIUM"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].id").value(jobId.toString()))
                .andExpect(jsonPath("$.content[0].jobType").value("WEBHOOK"))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.first").value(true))
                .andExpect(jsonPath("$.last").value(true));
    }

    @Test
    void getJobReturnsJobDetail() throws Exception {
        UUID jobId = UUID.randomUUID();
        Instant now = Instant.parse("2026-04-26T00:00:00Z");
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com/hook");

        JobDetailDTO detail = new JobDetailDTO(
                jobId,
                JobType.WEBHOOK,
                JobStatus.PENDING,
                JobPriority.MEDIUM,
                payload,
                null,
                3,
                "job-key-1",
                now,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                now,
                now
        );

        when(jobService.getJob(jobId)).thenReturn(detail);

        mockMvc.perform(get("/app/v1/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(jobId.toString()))
                .andExpect(jsonPath("$.jobType").value("WEBHOOK"))
                .andExpect(jsonPath("$.jobStatus").value("PENDING"))
                .andExpect(jsonPath("$.payload.url").value("https://example.com/hook"));
    }

    @Test
    void cancelJobReturnsCanceledStatus() throws Exception {
        UUID jobId = UUID.randomUUID();
        when(jobService.cancelJob(jobId)).thenReturn(new CancelJobResponseDTO(jobId, JobStatus.CANCELED));

        mockMvc.perform(post("/app/v1/jobs/{jobId}/cancel", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value(jobId.toString()))
                .andExpect(jsonPath("$.status").value("CANCELED"));
    }

    @Test
    void requeueJobReturnsNewJobId() throws Exception {
        UUID originalJobId = UUID.randomUUID();
        UUID requeuedJobId = UUID.randomUUID();
        when(jobService.requeueJob(originalJobId)).thenReturn(new RequeueJobResponseDTO(requeuedJobId));

        mockMvc.perform(post("/app/v1/jobs/{jobId}/requeue", originalJobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value(requeuedJobId.toString()));
    }
}
