package com.job.scheduler.service;

import com.job.scheduler.dto.CancelJobResponseDTO;
import com.job.scheduler.dto.JobRequestDTO;
import com.job.scheduler.dto.RequeueJobResponseDTO;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.repository.ExecutionLogRepository;
import com.job.scheduler.repository.JobRepository;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JobServiceTest {
    @Mock
    private JobRepository jobRepository;

    @Mock
    private ExecutionLogRepository executionLogRepository;

    @Mock
    private Validator validator;

    private ObjectMapper objectMapper;
    private JobService jobService;


    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        jobService = new JobService(
                jobRepository,
                objectMapper,
                executionLogRepository,
                validator
        );
    }

    @Test
    void submitJobCreatesPendingImmediateJob() {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com");
        payload.set("body", objectMapper.createObjectNode());

        JobRequestDTO request = new JobRequestDTO(
                JobType.WEBHOOK,
                JobPriority.MEDIUM,
                payload,
                null,
                5,
                "job-key-1"
        );

        when(validator.validate(any())).thenReturn(Set.of());

        UUID savedId = UUID.randomUUID();
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(savedId);
            return job;
        });

        UUID result = jobService.submitJob(request);

        assertThat(result).isEqualTo(savedId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobType()).isEqualTo(JobType.WEBHOOK);
        assertThat(savedJob.getJobPriority()).isEqualTo(JobPriority.MEDIUM);
        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getPayload()).isEqualTo(payload.toString());
        assertThat(savedJob.getCronExpression()).isNull();
        assertThat(savedJob.getNextRunAt()).isNotNull();
        assertThat(savedJob.getMaxAttempts()).isEqualTo(5);
        assertThat(savedJob.getIdempotencyKey()).isEqualTo("job-key-1");
    }

    @Test
    void submitJobUsesDefaultMaxAttemptsWhenMissing() {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com");

        JobRequestDTO request = new JobRequestDTO(
                JobType.WEBHOOK,
                JobPriority.MEDIUM,
                payload,
                null,
                null,
                "job-key-2"
        );

        when(validator.validate(any())).thenReturn(Set.of());

        UUID savedId = UUID.randomUUID();
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(savedId);
            return job;
        });

        jobService.submitJob(request);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getMaxAttempts()).isEqualTo(3);
    }
    @Test
    void submitJobRejectsInvalidCronExpression() {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com");

        JobRequestDTO request = new JobRequestDTO(
                JobType.WEBHOOK,
                JobPriority.MEDIUM,
                payload,
                "not-a-cron",
                3,
                "job-key-3"
        );

        assertThatThrownBy(() -> jobService.submitJob(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid cron expression");
    }

    @Test
    void submitJobCreatesPendingCronJob() {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("url", "https://example.com");

        JobRequestDTO request = new JobRequestDTO(
                JobType.WEBHOOK,
                JobPriority.HIGH,
                payload,
                "0 */5 * * * *",
                3,
                "cron-job-key"
        );

        when(validator.validate(any())).thenReturn(Set.of());

        UUID savedId = UUID.randomUUID();
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(savedId);
            return job;
        });

        UUID result = jobService.submitJob(request);

        assertThat(result).isEqualTo(savedId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getCronExpression()).isEqualTo("0 */5 * * * *");
        assertThat(savedJob.getNextRunAt()).isAfter(Instant.now().minusSeconds(1));
    }

    @Test
    void submitJobRejectsInvalidPayloadShape() {
        JobRequestDTO request = new JobRequestDTO(
                JobType.WEBHOOK,
                JobPriority.MEDIUM,
                objectMapper.createArrayNode().add("not-an-object"),
                null,
                3,
                "job-key-invalid-payload"
        );

        assertThatThrownBy(() -> jobService.submitJob(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Payload does not match expected shape");
    }

    @Test
    void submitJobThrowsConstraintViolationForInvalidTypedPayload() {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("to", "not-an-email");
        payload.put("subject", "");
        payload.put("body", "");

        JobRequestDTO request = new JobRequestDTO(
                JobType.SEND_EMAIL,
                JobPriority.MEDIUM,
                payload,
                null,
                3,
                "job-key-invalid-email"
        );

        @SuppressWarnings("unchecked")
        ConstraintViolation<Object> violation = (ConstraintViolation<Object>) org.mockito.Mockito.mock(ConstraintViolation.class);
        when(violation.getMessage()).thenReturn("Invalid email address");
        when(validator.validate(any())).thenReturn(Set.of(violation));

        assertThatThrownBy(() -> jobService.submitJob(request))
                .isInstanceOf(ConstraintViolationException.class);
    }

    @Test
    void getJobThrowsWhenJobDoesNotExist() {
        UUID jobId = UUID.randomUUID();

        when(jobRepository.findById(jobId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> jobService.getJob(jobId))
                .isInstanceOf(EntityNotFoundException.class)
                .hasMessage("Job does not exist");
    }

    @Test
    void getExecutionLogsThrowsWhenJobDoesNotExist() {
        UUID jobId = UUID.randomUUID();

        when(jobRepository.existsById(jobId)).thenReturn(false);

        assertThatThrownBy(() -> jobService.getExecutionLogs(jobId))
                .isInstanceOf(EntityNotFoundException.class)
                .hasMessage("Job does not exist");
    }

    @Test
    void cancelJobCancelsPendingJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobType(JobType.WEBHOOK);
        job.setJobPriority(JobPriority.MEDIUM);
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(Instant.now());
        job.setQueuedAt(Instant.now());

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        CancelJobResponseDTO response = jobService.cancelJob(jobId);

        assertThat(response.jobId()).isEqualTo(jobId);
        assertThat(response.status()).isEqualTo(JobStatus.CANCELED);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.CANCELED);
        assertThat(savedJob.getNextRunAt()).isNull();
        assertThat(savedJob.getQueuedAt()).isNull();
        assertThat(savedJob.getStartedAt()).isNull();
        assertThat(savedJob.getCompletedAt()).isNotNull();
        assertThat(savedJob.getLastErrorMessage()).isEqualTo("Canceled by request");
    }

    @Test
    void cancelJobRejectsRunningJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.RUNNING);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> jobService.cancelJob(jobId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("RUNNING jobs cannot be canceled");
    }

    @Test
    void cancelJobRejectsSuccessJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.SUCCESS);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> jobService.cancelJob(jobId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Only active jobs can be canceled");
    }

    @Test
    void cancelJobRejectsDeadJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.DEAD);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> jobService.cancelJob(jobId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Only active jobs can be canceled");
    }

    @Test
    void cancelJobRejectsCanceledJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.CANCELED);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> jobService.cancelJob(jobId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Only active jobs can be canceled");
    }

    @Test
    void requeueJobCreatesNewPendingJobFromDeadJob() {
        UUID deadJobId = UUID.randomUUID();
        UUID requeuedJobId = UUID.randomUUID();

        Job deadJob = new Job();
        deadJob.setId(deadJobId);
        deadJob.setJobType(JobType.WEBHOOK);
        deadJob.setJobPriority(JobPriority.HIGH);
        deadJob.setJobStatus(JobStatus.DEAD);
        deadJob.setPayload(objectMapper.createObjectNode().put("url", "https://example.com").toString());
        deadJob.setCronExpression(null);
        deadJob.setMaxAttempts(4);
        deadJob.setIdempotencyKey("old-key");

        when(jobRepository.findById(deadJobId)).thenReturn(Optional.of(deadJob));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(requeuedJobId);
            return job;
        });

        RequeueJobResponseDTO response = jobService.requeueJob(deadJobId);

        assertThat(response.jobId()).isEqualTo(requeuedJobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getJobType()).isEqualTo(JobType.WEBHOOK);
        assertThat(savedJob.getJobPriority()).isEqualTo(JobPriority.HIGH);
        assertThat(savedJob.getMaxAttempts()).isEqualTo(4);
        assertThat(savedJob.getRequeuedFromJobId()).isEqualTo(deadJobId);
        assertThat(savedJob.getRequeuedAt()).isNotNull();
        assertThat(savedJob.getNextRunAt()).isNotNull();
        assertThat(savedJob.getIdempotencyKey()).startsWith("old-key:requeue:");
    }

    @Test
    void requeueJobRejectsNonDeadJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.SUCCESS);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        assertThatThrownBy(() -> jobService.requeueJob(jobId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Only DEAD jobs can be requeued");
    }

    @Test
    void markDispatchSucceededMovesJobToQueued() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(Instant.now());

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.markDispatchSucceeded(jobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(savedJob.getNextRunAt()).isNull();
        assertThat(savedJob.getQueuedAt()).isNotNull();
    }

    @Test
    void markJobDeadSetsDeadStatusAndDeadLetterFields() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.RUNNING);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.markJobDead(jobId, "handler failed");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(savedJob.getNextRunAt()).isNull();
        assertThat(savedJob.getQueuedAt()).isNull();
        assertThat(savedJob.getCompletedAt()).isNotNull();
        assertThat(savedJob.getLastErrorMessage()).isEqualTo("handler failed");

        assertThat(savedJob.getDeadLetterStatus()).isEqualTo(DeadLetterStatus.PENDING);
        assertThat(savedJob.getDeadLetterQueuedAt()).isNotNull();
        assertThat(savedJob.getNextDeadLetterAttemptAt()).isNotNull();
        assertThat(savedJob.getDeadLetterErrorMessage()).isEqualTo("handler failed");
    }

    @Test
    void scheduleRetryResetsJobToPending() {
        UUID jobId = UUID.randomUUID();
        Instant retryAt = Instant.now().plusSeconds(30);

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.RUNNING);
        job.setQueuedAt(Instant.now());
        job.setStartedAt(Instant.now());
        job.setCompletedAt(Instant.now());

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.scheduleRetry(jobId, retryAt, "temporary failure");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getNextRunAt()).isEqualTo(retryAt);
        assertThat(savedJob.getQueuedAt()).isNull();
        assertThat(savedJob.getStartedAt()).isNull();
        assertThat(savedJob.getCompletedAt()).isNull();
        assertThat(savedJob.getLastErrorMessage()).isEqualTo("temporary failure");
    }

    @Test
    void scheduleNextCronRunReschedulesCronJob() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.RUNNING);
        job.setCronExpression("0 */5 * * * *");
        job.setQueuedAt(Instant.now());
        job.setStartedAt(Instant.now());
        job.setCompletedAt(Instant.now());
        job.setLastErrorMessage("old error");

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.scheduleNextCronRun(jobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getNextRunAt()).isAfter(Instant.now().minusSeconds(1));
        assertThat(savedJob.getQueuedAt()).isNull();
        assertThat(savedJob.getStartedAt()).isNull();
        assertThat(savedJob.getCompletedAt()).isNull();
        assertThat(savedJob.getLastErrorMessage()).isNull();
    }

    @Test
    void scheduleNextCronRunMarksOneTimeJobSuccess() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.RUNNING);
        job.setCronExpression(null);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.scheduleNextCronRun(jobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.SUCCESS);
        assertThat(savedJob.getNextRunAt()).isNull();
        assertThat(savedJob.getCompletedAt()).isNotNull();
    }

    @Test
    void markDeadLetterSentClearsRetryFields() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setDeadLetterStatus(DeadLetterStatus.PENDING);
        job.setNextDeadLetterAttemptAt(Instant.now().plusSeconds(30));
        job.setDeadLetterErrorMessage("send failed");

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.markDeadLetterSent(jobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getDeadLetterStatus()).isEqualTo(DeadLetterStatus.SENT);
        assertThat(savedJob.getDeadLetterSentAt()).isNotNull();
        assertThat(savedJob.getNextDeadLetterAttemptAt()).isNull();
        assertThat(savedJob.getDeadLetterErrorMessage()).isNull();
    }

    @Test
    void markDeadLetterRetryUpdatesRetryMetadata() {
        UUID jobId = UUID.randomUUID();
        Instant nextAttemptAt = Instant.now().plusSeconds(45);

        Job job = new Job();
        job.setId(jobId);
        job.setDeadLetterStatus(DeadLetterStatus.PENDING);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.markDeadLetterRetry(jobId, nextAttemptAt, "broker unavailable");

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getDeadLetterStatus()).isEqualTo(DeadLetterStatus.PENDING);
        assertThat(savedJob.getNextDeadLetterAttemptAt()).isEqualTo(nextAttemptAt);
        assertThat(savedJob.getDeadLetterErrorMessage()).isEqualTo("broker unavailable");
    }

    @Test
    void recoverQueuedJobMovesBackToPending() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.QUEUED);
        job.setQueuedAt(Instant.now());

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.recoverQueuedJob(jobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getNextRunAt()).isNotNull();
        assertThat(savedJob.getQueuedAt()).isNull();
    }

    @Test
    void recoverRunningJobMovesBackToPendingWithWatchdogMessage() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setJobStatus(JobStatus.RUNNING);
        job.setQueuedAt(Instant.now());
        job.setStartedAt(Instant.now());
        job.setCompletedAt(Instant.now());

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.recoverRunningJob(jobId);

        ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
        verify(jobRepository).save(jobCaptor.capture());

        Job savedJob = jobCaptor.getValue();

        assertThat(savedJob.getJobStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(savedJob.getNextRunAt()).isNotNull();
        assertThat(savedJob.getQueuedAt()).isNull();
        assertThat(savedJob.getStartedAt()).isNull();
        assertThat(savedJob.getCompletedAt()).isNull();
        assertThat(savedJob.getLastErrorMessage()).isEqualTo("Recovered by running-job watchdog");
    }

    @Test
    void maxAttemptsExceededReturnsTrueWhenAttemptCountReached() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setMaxAttempts(3);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(executionLogRepository.countByJobId(jobId)).thenReturn(3L);

        boolean result = jobService.maxAttemptsExceeded(jobId);

        assertThat(result).isTrue();
    }

    @Test
    void maxAttemptsExceededReturnsFalseWhenAttemptsRemain() {
        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setMaxAttempts(3);

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(executionLogRepository.countByJobId(jobId)).thenReturn(2L);

        boolean result = jobService.maxAttemptsExceeded(jobId);

        assertThat(result).isFalse();
    }
}
