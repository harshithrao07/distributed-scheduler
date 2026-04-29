package com.job.scheduler.monitoring;

import com.job.scheduler.monitoring.events.JobCanceledEvent;
import com.job.scheduler.monitoring.events.JobDeadLetteredEvent;
import com.job.scheduler.monitoring.events.JobDispatchedEvent;
import com.job.scheduler.monitoring.events.JobExecutedEvent;
import com.job.scheduler.monitoring.events.JobRequeuedEvent;
import com.job.scheduler.monitoring.events.JobSubmittedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SchedulerMetricsListener {

    private final SchedulerMetrics schedulerMetrics;

    @EventListener
    public void onJobSubmitted(JobSubmittedEvent event) {
        schedulerMetrics.recordJobSubmitted(event.jobType(), event.jobPriority());
    }

    @EventListener
    public void onJobRequeued(JobRequeuedEvent event) {
        schedulerMetrics.recordJobRequeued(event.jobType(), event.jobPriority());
    }

    @EventListener
    public void onJobCanceled(JobCanceledEvent event) {
        schedulerMetrics.recordJobCanceled(event.jobType(), event.jobPriority());
    }

    @EventListener
    public void onJobDispatched(JobDispatchedEvent event) {
        schedulerMetrics.recordJobDispatched(event.jobType(), event.jobPriority());
    }

    @EventListener
    public void onJobExecuted(JobExecutedEvent event) {
        schedulerMetrics.recordJobExecution(
                event.jobType(),
                event.jobPriority(),
                event.result(),
                event.duration()
        );
    }

    @EventListener
    public void onJobDeadLettered(JobDeadLetteredEvent event) {
        schedulerMetrics.recordJobDeadLettered(event.jobType(), event.jobPriority());
    }
}
