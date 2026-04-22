package com.job.scheduler.dto;

import com.job.scheduler.enums.JobType;

import java.util.UUID;

public record JobDispatchEvent(UUID jobId, JobType jobType) {
}
