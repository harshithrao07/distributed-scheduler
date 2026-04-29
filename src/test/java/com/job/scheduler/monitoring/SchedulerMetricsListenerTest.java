package com.job.scheduler.monitoring;

import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.monitoring.events.JobCanceledEvent;
import com.job.scheduler.monitoring.events.JobDeadLetteredEvent;
import com.job.scheduler.monitoring.events.JobDispatchedEvent;
import com.job.scheduler.monitoring.events.JobExecutedEvent;
import com.job.scheduler.monitoring.events.JobRequeuedEvent;
import com.job.scheduler.monitoring.events.JobSubmittedEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SchedulerMetricsListenerTest {

    private final SchedulerMetrics schedulerMetrics = mock(SchedulerMetrics.class);
    private final SchedulerMetricsListener listener = new SchedulerMetricsListener(schedulerMetrics);

    @Test
    void translatesDomainEventsToMetrics() {
        listener.onJobSubmitted(new JobSubmittedEvent(JobType.WEBHOOK, JobPriority.HIGH));
        listener.onJobRequeued(new JobRequeuedEvent(JobType.SEND_EMAIL, JobPriority.MEDIUM));
        listener.onJobCanceled(new JobCanceledEvent(JobType.CLEANUP, JobPriority.LOW));
        listener.onJobDispatched(new JobDispatchedEvent(JobType.WEBHOOK, JobPriority.MEDIUM));
        listener.onJobDeadLettered(new JobDeadLetteredEvent(JobType.SEND_EMAIL, JobPriority.HIGH));

        Duration duration = Duration.ofMillis(125);
        listener.onJobExecuted(new JobExecutedEvent(JobType.CLEANUP, JobPriority.MEDIUM, JobStatus.SUCCESS, duration));

        verify(schedulerMetrics).recordJobSubmitted(JobType.WEBHOOK, JobPriority.HIGH);
        verify(schedulerMetrics).recordJobRequeued(JobType.SEND_EMAIL, JobPriority.MEDIUM);
        verify(schedulerMetrics).recordJobCanceled(JobType.CLEANUP, JobPriority.LOW);
        verify(schedulerMetrics).recordJobDispatched(JobType.WEBHOOK, JobPriority.MEDIUM);
        verify(schedulerMetrics).recordJobDeadLettered(JobType.SEND_EMAIL, JobPriority.HIGH);
        verify(schedulerMetrics).recordJobExecution(JobType.CLEANUP, JobPriority.MEDIUM, JobStatus.SUCCESS, duration);
    }
}
