package com.job.scheduler.monitoring.events;

import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;

import java.time.Duration;

public record JobExecutedEvent(
        JobType jobType,
        JobPriority jobPriority,
        JobStatus result,
        Duration duration
) {
}
