package com.job.scheduler.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.job.scheduler.dto.ExecutionLogDTO;
import com.job.scheduler.dto.JobDetailDTO;
import com.job.scheduler.dto.JobRequestDTO;
import com.job.scheduler.dto.JobSummaryDTO;
import com.job.scheduler.dto.RequeueJobResponseDTO;
import com.job.scheduler.dto.payload.CleanupPayload;
import com.job.scheduler.dto.payload.SendEmailPayload;
import com.job.scheduler.dto.payload.WebhookPayload;
import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.repository.ExecutionLogRepository;
import com.job.scheduler.repository.JobRepository;
import jakarta.persistence.EntityNotFoundException;
import jakarta.transaction.Transactional;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;
    private final ExecutionLogRepository executionLogRepository;
    private final Validator validator;

    public UUID submitJob(JobRequestDTO jobRequestDTO) {
        validateTypedPayload(jobRequestDTO.jobType(), jobRequestDTO.payload());
        CronExpression cronExpression = parseCronExpression(jobRequestDTO.cronExpression());

        Job job = new Job();
        job.setJobType(jobRequestDTO.jobType());
        job.setJobStatus(JobStatus.PENDING);
        job.setJobPriority(jobRequestDTO.jobPriority());
        job.setPayload(jobRequestDTO.payload());
        job.setCronExpression(jobRequestDTO.cronExpression());
        if (cronExpression != null) {
            job.setNextRunAt(nextRunAt(cronExpression));
        } else {
            job.setNextRunAt(Instant.now());
        }
        job.setMaxRetries(jobRequestDTO.maxRetries() != null ? jobRequestDTO.maxRetries() : job.getMaxRetries());
        job.setIdempotencyKey(jobRequestDTO.idempotencyKey());

        Job savedJob = jobRepository.save(job);
        return savedJob.getId();
    }

    public List<JobSummaryDTO> getJobs() {
        return jobRepository.findAllByOrderByCreatedAtDesc()
                .stream()
                .map(this::toSummary)
                .toList();
    }

    public List<JobSummaryDTO> getDeadJobs() {
        return jobRepository.findByJobStatusOrderByUpdatedAtDesc(JobStatus.DEAD)
                .stream()
                .map(this::toSummary)
                .toList();
    }

    public JobDetailDTO getJob(UUID jobId) {
        return toDetail(findById(jobId));
    }

    public List<ExecutionLogDTO> getExecutionLogs(UUID jobId) {
        if (!jobRepository.existsById(jobId)) {
            throw new EntityNotFoundException("Job does not exist");
        }

        return executionLogRepository.findByJobIdOrderByAttemptNumberAsc(jobId)
                .stream()
                .map(this::toExecutionLog)
                .toList();
    }

    @Transactional
    public RequeueJobResponseDTO requeueJob(UUID jobId) {
        Job deadJob = findById(jobId);

        if (deadJob.getJobStatus() != JobStatus.DEAD) {
            throw new IllegalStateException("Only DEAD jobs can be requeued");
        }

        Job requeuedJob = new Job();
        requeuedJob.setJobType(deadJob.getJobType());
        requeuedJob.setJobStatus(JobStatus.PENDING);
        requeuedJob.setJobPriority(deadJob.getJobPriority());
        requeuedJob.setPayload(deadJob.getPayload());
        requeuedJob.setCronExpression(deadJob.getCronExpression());
        requeuedJob.setMaxRetries(deadJob.getMaxRetries());
        requeuedJob.setIdempotencyKey(deadJob.getIdempotencyKey() + ":requeue:" + UUID.randomUUID());
        requeuedJob.setNextRunAt(Instant.now());
        requeuedJob.setRequeuedFromJobId(deadJob.getId());
        requeuedJob.setRequeuedAt(Instant.now());

        Job savedJob = jobRepository.save(requeuedJob);
        return new RequeueJobResponseDTO(savedJob.getId());
    }

    private void validateTypedPayload(JobType jobType, JsonNode payload) {
        switch (jobType) {
            case SEND_EMAIL -> validateRecord(payload, SendEmailPayload.class);
            case WEBHOOK -> validateRecord(payload, WebhookPayload.class);
            case CLEANUP -> validateRecord(payload, CleanupPayload.class);
            default -> throw new IllegalArgumentException("Unsupported job type: " + jobType);
        }
    }

    private <T> void validateRecord(JsonNode payload, Class<T> payloadClass) {
        final T typedPayload;
        try {
            typedPayload = objectMapper.treeToValue(payload, payloadClass);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Payload does not match expected shape for " + payloadClass.getSimpleName(), e
            );
        }

        Set<ConstraintViolation<T>> violations = validator.validate(typedPayload);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }

    @Transactional
    public void updateJobStatus(UUID jobId, JobStatus jobStatus) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setJobStatus(jobStatus);
        if (jobStatus == JobStatus.QUEUED) {
            job.setQueuedAt(Instant.now());
        }
        if (jobStatus == JobStatus.RUNNING) {
            job.setStartedAt(Instant.now());
            job.setCompletedAt(null);
        }
        if (jobStatus == JobStatus.SUCCESS || jobStatus == JobStatus.DEAD || jobStatus == JobStatus.RUNNING) {
            job.setNextRunAt(null);
        }
        if (jobStatus == JobStatus.SUCCESS || jobStatus == JobStatus.DEAD) {
            job.setCompletedAt(Instant.now());
        }
        if (jobStatus == JobStatus.DEAD) {
            markDeadLetterPending(job, null);
        }
        if (jobStatus == JobStatus.PENDING || jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.SUCCESS || jobStatus == JobStatus.DEAD) {
            job.setQueuedAt(null);
        }
        jobRepository.save(job);
    }

    @Transactional
    public void markJobDead(UUID jobId, String errorMessage) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setJobStatus(JobStatus.DEAD);
        job.setNextRunAt(null);
        job.setQueuedAt(null);
        job.setCompletedAt(Instant.now());
        job.setLastErrorMessage(errorMessage);
        markDeadLetterPending(job, errorMessage);
        jobRepository.save(job);
    }

    @Transactional
    public void markDeadLetterAttempt(UUID jobId, Instant nextAttemptAt) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setDeadLetterLastAttemptAt(Instant.now());
        job.setNextDeadLetterAttemptAt(nextAttemptAt);
        jobRepository.save(job);
    }

    @Transactional
    public void markDeadLetterSent(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setDeadLetterStatus(DeadLetterStatus.SENT);
        job.setDeadLetterSentAt(Instant.now());
        job.setNextDeadLetterAttemptAt(null);
        job.setDeadLetterErrorMessage(null);
        jobRepository.save(job);
    }

    @Transactional
    public void markDeadLetterRetry(UUID jobId, Instant nextAttemptAt, String errorMessage) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setDeadLetterStatus(DeadLetterStatus.PENDING);
        job.setNextDeadLetterAttemptAt(nextAttemptAt);
        job.setDeadLetterErrorMessage(errorMessage);
        jobRepository.save(job);
    }

    @Transactional
    public void scheduleNextCronRun(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        CronExpression cronExpression = parseCronExpression(job.getCronExpression());

        if (cronExpression == null) {
            job.setJobStatus(JobStatus.SUCCESS);
            job.setNextRunAt(null);
            job.setCompletedAt(Instant.now());
        } else {
            job.setJobStatus(JobStatus.PENDING);
            job.setNextRunAt(nextRunAt(cronExpression));
            job.setQueuedAt(null);
            job.setStartedAt(null);
            job.setCompletedAt(null);
            job.setLastErrorMessage(null);
        }

        jobRepository.save(job);
    }

    @Transactional
    public void scheduleRetry(UUID jobId, Instant nextRunAt, String errorMessage) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(nextRunAt);
        job.setQueuedAt(null);
        job.setStartedAt(null);
        job.setCompletedAt(null);
        job.setLastErrorMessage(errorMessage);
        jobRepository.save(job);
    }

    @Transactional
    public void markDispatchSucceeded(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setJobStatus(JobStatus.QUEUED);
        job.setNextRunAt(null);
        job.setQueuedAt(Instant.now());
        jobRepository.save(job);
    }

    @Transactional
    public void markDispatchAttempt(UUID jobId, Instant retryDispatchAt) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setNextRunAt(retryDispatchAt);
        jobRepository.save(job);
    }

    @Transactional
    public void recoverQueuedJob(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(Instant.now());
        job.setQueuedAt(null);
        jobRepository.save(job);
    }

    @Transactional
    public void recoverRunningJob(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(Instant.now());
        job.setQueuedAt(null);
        job.setStartedAt(null);
        job.setCompletedAt(null);
        job.setLastErrorMessage("Recovered by running-job watchdog");
        jobRepository.save(job);
    }

    public boolean maxRetriesExceeded(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
        return getAttemptCount(jobId) >= job.getMaxRetries();
    }

    public long getAttemptCount(UUID jobId) {
        return executionLogRepository.countByJobId(jobId);
    }

    public Job findById(UUID jobId) {
        return jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException("Job does not exist"));
    }

    public boolean hasCronExpression(Job job) {
        return job.getCronExpression() != null && !job.getCronExpression().isBlank();
    }

    private CronExpression parseCronExpression(String cronExpression) {
        if (cronExpression == null || cronExpression.isBlank()) {
            return null;
        }

        try {
            return CronExpression.parse(cronExpression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid cron expression: " + cronExpression, e);
        }
    }

    private Instant nextRunAt(CronExpression cronExpression) {
        ZonedDateTime nextRun = cronExpression.next(ZonedDateTime.now(ZoneOffset.UTC));

        if (nextRun == null) {
            throw new IllegalArgumentException("Cron expression does not produce a future run");
        }

        return nextRun.toInstant();
    }

    private void markDeadLetterPending(Job job, String errorMessage) {
        if (job.getDeadLetterStatus() == DeadLetterStatus.SENT) {
            return;
        }

        Instant now = Instant.now();
        job.setDeadLetterStatus(DeadLetterStatus.PENDING);
        job.setDeadLetterQueuedAt(now);
        job.setNextDeadLetterAttemptAt(now);
        job.setDeadLetterErrorMessage(errorMessage);
    }

    private JobSummaryDTO toSummary(Job job) {
        return new JobSummaryDTO(
                job.getId(),
                job.getJobType(),
                job.getJobStatus(),
                job.getJobPriority(),
                job.getCronExpression(),
                job.getNextRunAt(),
                job.getQueuedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.getRequeuedFromJobId(),
                job.getRequeuedAt(),
                job.getDeadLetterStatus(),
                job.getDeadLetterQueuedAt(),
                job.getDeadLetterSentAt(),
                job.getNextDeadLetterAttemptAt(),
                job.getCreatedAt(),
                job.getUpdatedAt()
        );
    }

    private JobDetailDTO toDetail(Job job) {
        return new JobDetailDTO(
                job.getId(),
                job.getJobType(),
                job.getJobStatus(),
                job.getJobPriority(),
                job.getPayload(),
                job.getCronExpression(),
                job.getMaxRetries(),
                job.getIdempotencyKey(),
                job.getNextRunAt(),
                job.getQueuedAt(),
                job.getLastErrorMessage(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.getRequeuedFromJobId(),
                job.getRequeuedAt(),
                job.getDeadLetterStatus(),
                job.getDeadLetterQueuedAt(),
                job.getDeadLetterSentAt(),
                job.getDeadLetterLastAttemptAt(),
                job.getNextDeadLetterAttemptAt(),
                job.getDeadLetterErrorMessage(),
                job.getCreatedAt(),
                job.getUpdatedAt()
        );
    }

    private ExecutionLogDTO toExecutionLog(ExecutionLog executionLog) {
        return new ExecutionLogDTO(
                executionLog.getId(),
                executionLog.getJobDetails().getId(),
                executionLog.getAttemptNumber(),
                executionLog.getExecutionStatus(),
                executionLog.getStartedAt(),
                executionLog.getCompletedAt(),
                executionLog.getDurationMs(),
                executionLog.getErrorMessage(),
                executionLog.getWorkerId(),
                executionLog.getCreatedAt()
        );
    }
}
