package com.job.scheduler.controller;

import com.job.scheduler.dto.DlqJobDetailDTO;
import com.job.scheduler.dto.DlqPageDTO;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/app/v1/dlq")
public class DlqController {
    private final JobService jobService;

    @GetMapping
    public ResponseEntity<DlqPageDTO> getDeadLetterJobs(
            @RequestParam(required = false) DeadLetterStatus deadLetterStatus,
            @RequestParam(required = false) JobType type,
            @RequestParam(required = false) JobPriority priority,
            @RequestParam(required = false) Instant createdFrom,
            @RequestParam(required = false) Instant createdTo,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return ResponseEntity.ok(jobService.getDeadLetterJobs(
                deadLetterStatus,
                type,
                priority,
                createdFrom,
                createdTo,
                page,
                size
        ));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<DlqJobDetailDTO> getDeadLetterJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.getDeadLetterJob(jobId));
    }
}
