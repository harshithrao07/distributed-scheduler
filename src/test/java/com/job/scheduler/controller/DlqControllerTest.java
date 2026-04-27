package com.job.scheduler.controller;

import com.job.scheduler.dto.DlqJobDetailDTO;
import com.job.scheduler.dto.DlqJobSummaryDTO;
import com.job.scheduler.dto.DlqPageDTO;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.exception.ApiExceptionHandler;
import com.job.scheduler.service.JobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class DlqControllerTest {

    @Mock
    private JobService jobService;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        mockMvc = MockMvcBuilders
                .standaloneSetup(new DlqController(jobService))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
    }

    @Test
    void getDeadLetterJobsReturnsPagedResponse() throws Exception {
        UUID jobId = UUID.randomUUID();
        Instant now = Instant.parse("2026-04-26T00:00:00Z");

        DlqJobSummaryDTO summary = new DlqJobSummaryDTO(
                jobId,
                JobType.WEBHOOK,
                JobPriority.HIGH,
                "handler failed",
                3,
                DeadLetterStatus.PENDING,
                now,
                null,
                null,
                now,
                "broker unavailable",
                null,
                null,
                now,
                now
        );

        DlqPageDTO page = new DlqPageDTO(List.of(summary), 0, 20, 1, 1, true, true);

        when(jobService.getDeadLetterJobs(
                eq(DeadLetterStatus.PENDING),
                eq(JobType.WEBHOOK),
                eq(JobPriority.HIGH),
                eq(null),
                eq(null),
                eq(0),
                eq(20)
        )).thenReturn(page);

        mockMvc.perform(get("/app/v1/dlq")
                        .param("deadLetterStatus", "PENDING")
                        .param("type", "WEBHOOK")
                        .param("priority", "HIGH"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].jobId").value(jobId.toString()))
                .andExpect(jsonPath("$.content[0].deadLetterStatus").value("PENDING"))
                .andExpect(jsonPath("$.content[0].attemptCount").value(3))
                .andExpect(jsonPath("$.totalElements").value(1));
    }

    @Test
    void getDeadLetterJobReturnsDetail() throws Exception {
        UUID jobId = UUID.randomUUID();
        Instant now = Instant.parse("2026-04-26T00:00:00Z");
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com/hook");

        DlqJobDetailDTO detail = new DlqJobDetailDTO(
                jobId,
                JobType.WEBHOOK,
                JobPriority.HIGH,
                payload,
                null,
                "handler failed",
                4,
                DeadLetterStatus.PENDING,
                now,
                null,
                null,
                now,
                "broker unavailable",
                null,
                null,
                now,
                now,
                List.of(),
                true,
                true
        );

        when(jobService.getDeadLetterJob(jobId)).thenReturn(detail);

        mockMvc.perform(get("/app/v1/dlq/{jobId}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value(jobId.toString()))
                .andExpect(jsonPath("$.jobType").value("WEBHOOK"))
                .andExpect(jsonPath("$.attemptCount").value(4))
                .andExpect(jsonPath("$.canRequeue").value(true))
                .andExpect(jsonPath("$.canRetryDeadLetterPublish").value(true))
                .andExpect(jsonPath("$.payload.url").value("https://example.com/hook"));
    }
}
