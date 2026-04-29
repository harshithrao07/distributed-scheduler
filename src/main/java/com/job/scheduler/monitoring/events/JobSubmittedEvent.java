package com.job.scheduler.monitoring.events;

import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobType;

public record JobSubmittedEvent(JobType jobType, JobPriority jobPriority) {
}
