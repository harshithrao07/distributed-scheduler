package com.job.scheduler.controller;

import com.job.scheduler.dto.ExecutionLogDTO;
import com.job.scheduler.dto.CancelJobResponseDTO;
import com.job.scheduler.dto.JobDetailDTO;
import com.job.scheduler.dto.JobPageDTO;
import com.job.scheduler.dto.JobRequestDTO;
import com.job.scheduler.dto.JobSummaryDTO;
import com.job.scheduler.dto.RequeueJobResponseDTO;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.service.JobService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/app/v1/jobs")
public class JobController {
    private final JobService jobService;

    @PostMapping
    public ResponseEntity<UUID> submitJob(@Valid @RequestBody JobRequestDTO jobRequestDTO) {
        UUID jobId = jobService.submitJob(jobRequestDTO);
        return ResponseEntity.ok(jobId);
    }

    @GetMapping
    public ResponseEntity<JobPageDTO> getJobs(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) JobType type,
            @RequestParam(required = false) JobPriority priority,
            @RequestParam(required = false) Instant createdFrom,
            @RequestParam(required = false) Instant createdTo,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return ResponseEntity.ok(jobService.getJobs(status, type, priority, createdFrom, createdTo, page, size));
    }

    @GetMapping("/dead")
    public ResponseEntity<List<JobSummaryDTO>> getDeadJobs() {
        return ResponseEntity.ok(jobService.getDeadJobs());
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobDetailDTO> getJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.getJob(jobId));
    }

    @GetMapping("/{jobId}/logs")
    public ResponseEntity<List<ExecutionLogDTO>> getExecutionLogs(@PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.getExecutionLogs(jobId));
    }

    @PostMapping("/{jobId}/requeue")
    public ResponseEntity<RequeueJobResponseDTO> requeueJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.requeueJob(jobId));
    }

    @PostMapping("/{jobId}/cancel")
    public ResponseEntity<CancelJobResponseDTO> cancelJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.cancelJob(jobId));
    }
}
