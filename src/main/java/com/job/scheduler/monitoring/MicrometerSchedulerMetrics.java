package com.job.scheduler.monitoring;

import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.repository.JobRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class MicrometerSchedulerMetrics implements SchedulerMetrics {

    private final MeterRegistry meterRegistry;

    public MicrometerSchedulerMetrics(MeterRegistry meterRegistry, JobRepository jobRepository) {
        this.meterRegistry = meterRegistry;
        registerJobStatusGauges(meterRegistry, jobRepository);
    }

    @Override
    public void recordJobSubmitted(JobType jobType, JobPriority jobPriority) {
        counter("scheduler_jobs_submitted_total", jobType, jobPriority).increment();
    }

    @Override
    public void recordJobRequeued(JobType jobType, JobPriority jobPriority) {
        counter("scheduler_jobs_requeued_total", jobType, jobPriority).increment();
    }

    @Override
    public void recordJobCanceled(JobType jobType, JobPriority jobPriority) {
        counter("scheduler_jobs_canceled_total", jobType, jobPriority).increment();
    }

    @Override
    public void recordJobDispatched(JobType jobType, JobPriority jobPriority) {
        counter("scheduler_jobs_dispatched_total", jobType, jobPriority).increment();
    }

    @Override
    public void recordJobExecution(JobType jobType, JobPriority jobPriority, JobStatus result, Duration duration) {
        Tags tags = jobTags(jobType, jobPriority).and("result", result.name());
        meterRegistry.counter("scheduler_jobs_executed_total", tags).increment();
        meterRegistry.timer("scheduler_job_execution_seconds", tags).record(duration);
    }

    @Override
    public void recordJobDeadLettered(JobType jobType, JobPriority jobPriority) {
        counter("scheduler_jobs_dead_lettered_total", jobType, jobPriority).increment();
    }

    private io.micrometer.core.instrument.Counter counter(String name, JobType jobType, JobPriority jobPriority) {
        return meterRegistry.counter(name, jobTags(jobType, jobPriority));
    }

    private Tags jobTags(JobType jobType, JobPriority jobPriority) {
        return Tags.of(
                "type", jobType.name(),
                "priority", jobPriority.name()
        );
    }

    private void registerJobStatusGauges(MeterRegistry meterRegistry, JobRepository jobRepository) {
        for (JobStatus status : JobStatus.values()) {
            meterRegistry.gauge(
                    "scheduler_jobs_status",
                    Tags.of("status", status.name()),
                    jobRepository,
                    repository -> repository.countByJobStatus(status)
            );
        }
    }
}
