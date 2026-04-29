package com.job.scheduler.monitoring;

import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;

import java.time.Duration;

public interface SchedulerMetrics {

    SchedulerMetrics NOOP = new SchedulerMetrics() {
    };

    default void recordJobSubmitted(JobType jobType, JobPriority jobPriority) {
    }

    default void recordJobRequeued(JobType jobType, JobPriority jobPriority) {
    }

    default void recordJobCanceled(JobType jobType, JobPriority jobPriority) {
    }

    default void recordJobDispatched(JobType jobType, JobPriority jobPriority) {
    }

    default void recordJobExecution(JobType jobType, JobPriority jobPriority, JobStatus result, Duration duration) {
    }

    default void recordJobDeadLettered(JobType jobType, JobPriority jobPriority) {
    }
}
