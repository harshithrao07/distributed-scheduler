package com.job.scheduler.integration;

import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.scheduler.QueuedJobWatchdogService;
import com.job.scheduler.scheduler.RunningJobWatchdogService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerRecoveryIntegrationTest extends AbstractSchedulerFlowIntegrationTest {

    @Autowired
    private QueuedJobWatchdogService queuedJobWatchdogService;

    @Autowired
    private RunningJobWatchdogService runningJobWatchdogService;

    @Test
    void queuedWatchdogRecoversStaleQueuedJobs() {
        var staleQueued = saveCleanupJob(JobStatus.QUEUED, null, "queued-watchdog-" + UUID.randomUUID());
        staleQueued.setQueuedAt(Instant.now().minusSeconds(600));
        jobRepository.saveAndFlush(staleQueued);

        queuedJobWatchdogService.recoverStaleQueuedJobs();

        var recovered = jobRepository.findById(staleQueued.getId()).orElseThrow();
        assertThat(recovered.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(recovered.getQueuedAt()).isNull();
        assertThat(recovered.getStartedAt()).isNull();
        assertThat(recovered.getCompletedAt()).isNull();
        assertThat(recovered.getLastErrorMessage()).isNull();
        assertThat(recovered.getNextRunAt()).isBeforeOrEqualTo(Instant.now());
    }

    @Test
    void runningWatchdogRecoversStaleRunningJobs() {
        var staleRunning = saveCleanupJob(JobStatus.RUNNING, null, "running-watchdog-" + UUID.randomUUID());
        staleRunning.setStartedAt(Instant.now().minusSeconds(1200));
        jobRepository.saveAndFlush(staleRunning);

        runningJobWatchdogService.recoverStaleRunningJobs();

        var recovered = jobRepository.findById(staleRunning.getId()).orElseThrow();
        assertThat(recovered.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(recovered.getQueuedAt()).isNull();
        assertThat(recovered.getStartedAt()).isNull();
        assertThat(recovered.getCompletedAt()).isNull();
        assertThat(recovered.getNextRunAt()).isBeforeOrEqualTo(Instant.now());
        assertThat(recovered.getLastErrorMessage()).isEqualTo("Recovered by running-job watchdog");
    }
}
