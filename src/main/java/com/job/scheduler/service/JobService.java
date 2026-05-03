package com.job.scheduler.service;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import com.job.scheduler.dto.CancelJobResponseDTO;
import com.job.scheduler.dto.DlqJobDetailDTO;
import com.job.scheduler.dto.DlqJobSummaryDTO;
import com.job.scheduler.dto.DlqPageDTO;
import com.job.scheduler.dto.ExecutionLogDTO;
import com.job.scheduler.dto.JobDetailDTO;
import com.job.scheduler.dto.JobPageDTO;
import com.job.scheduler.dto.JobRequestDTO;
import com.job.scheduler.dto.JobSummaryDTO;
import com.job.scheduler.dto.RequeueJobResponseDTO;
import com.job.scheduler.dto.payload.CleanupPayload;
import com.job.scheduler.dto.payload.SendEmailPayload;
import com.job.scheduler.dto.payload.WebhookPayload;
import com.job.scheduler.entity.ExecutionLog;
import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import com.job.scheduler.monitoring.events.JobCanceledEvent;
import com.job.scheduler.monitoring.events.JobDeadLetteredEvent;
import com.job.scheduler.monitoring.events.JobDispatchedEvent;
import com.job.scheduler.monitoring.events.JobRequeuedEvent;
import com.job.scheduler.monitoring.events.JobSubmittedEvent;
import com.job.scheduler.repository.ExecutionLogRepository;
import com.job.scheduler.repository.JobRepository;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.EntityNotFoundException;
import jakarta.transaction.Transactional;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
public class JobService {
    private static final String CREATED_AT_FIELD = "createdAt";
    private static final String JOB_NOT_FOUND_MESSAGE = "Job does not exist";

    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;
    private final ExecutionLogRepository executionLogRepository;
    private final Validator validator;
    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public JobService(
            JobRepository jobRepository,
            ObjectMapper objectMapper,
            ExecutionLogRepository executionLogRepository,
            Validator validator,
            ApplicationEventPublisher eventPublisher
    ) {
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
        this.executionLogRepository = executionLogRepository;
        this.validator = validator;
        this.eventPublisher = eventPublisher;
    }

    public JobService(
            JobRepository jobRepository,
            ObjectMapper objectMapper,
            ExecutionLogRepository executionLogRepository,
            Validator validator
    ) {
        this(jobRepository, objectMapper, executionLogRepository, validator, event -> {
        });
    }

    public UUID submitJob(JobRequestDTO jobRequestDTO) {
        validateTypedPayload(jobRequestDTO.jobType(), jobRequestDTO.payload());
        CronExpression cronExpression = parseCronExpression(jobRequestDTO.cronExpression());
        UUID existingJobId = findExistingJobId(jobRequestDTO.idempotencyKey());
        if (existingJobId != null) {
            return existingJobId;
        }

        Job job = new Job();
        job.setJobType(jobRequestDTO.jobType());
        job.setJobStatus(JobStatus.PENDING);
        job.setJobPriority(jobRequestDTO.jobPriority());
        job.setPayload(writePayload(jobRequestDTO.payload()));
        job.setCronExpression(jobRequestDTO.cronExpression());
        if (cronExpression != null) {
            job.setNextRunAt(nextRunAt(cronExpression));
        } else {
            job.setNextRunAt(Instant.now());
        }
        job.setMaxAttempts(jobRequestDTO.maxAttempts() != null ? jobRequestDTO.maxAttempts() : job.getMaxAttempts());
        job.setIdempotencyKey(jobRequestDTO.idempotencyKey());

        try {
            Job savedJob = jobRepository.save(job);
            publish(new JobSubmittedEvent(savedJob.getJobType(), savedJob.getJobPriority()));
            return savedJob.getId();
        } catch (DataIntegrityViolationException exception) {
            UUID duplicateJobId = findExistingJobId(jobRequestDTO.idempotencyKey());
            if (duplicateJobId != null) {
                return duplicateJobId;
            }
            throw exception;
        }
    }

    public JobPageDTO getJobs(
            JobStatus status,
            JobType type,
            JobPriority priority,
            Instant createdFrom,
            Instant createdTo,
            int page,
            int size
    ) {
        int safePage = Math.max(page, 0);
        int safeSize = Math.max(1, Math.min(size, 100));
        PageRequest pageRequest = PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, CREATED_AT_FIELD));

        Page<JobSummaryDTO> jobs = jobRepository.findAll(jobFilters(status, type, priority, createdFrom, createdTo), pageRequest)
                .map(this::toSummary);

        return new JobPageDTO(
                jobs.getContent(),
                jobs.getNumber(),
                jobs.getSize(),
                jobs.getTotalElements(),
                jobs.getTotalPages(),
                jobs.isFirst(),
                jobs.isLast()
        );
    }

    public List<JobSummaryDTO> getDeadJobs() {
        return jobRepository.findByJobStatusOrderByUpdatedAtDesc(JobStatus.DEAD)
                .stream()
                .map(this::toSummary)
                .toList();
    }

    public DlqPageDTO getDeadLetterJobs(
            DeadLetterStatus deadLetterStatus,
            JobType type,
            JobPriority priority,
            Instant createdFrom,
            Instant createdTo,
            int page,
            int size
    ) {
        int safePage = Math.max(page, 0);
        int safeSize = Math.max(1, Math.min(size, 100));
        PageRequest pageRequest = PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "updatedAt"));

        Page<DlqJobSummaryDTO> jobs = jobRepository.findAll(
                        deadLetterFilters(deadLetterStatus, type, priority, createdFrom, createdTo),
                        pageRequest
                )
                .map(this::toDlqSummary);

        return new DlqPageDTO(
                jobs.getContent(),
                jobs.getNumber(),
                jobs.getSize(),
                jobs.getTotalElements(),
                jobs.getTotalPages(),
                jobs.isFirst(),
                jobs.isLast()
        );
    }

    public DlqJobDetailDTO getDeadLetterJob(UUID jobId) {
        Job job = findById(jobId);

        if (job.getJobStatus() != JobStatus.DEAD) {
            throw new IllegalStateException("Only DEAD jobs can be inspected through the DLQ API");
        }

        List<ExecutionLogDTO> executionLogs = executionLogRepository.findByJobIdOrderByAttemptNumberAsc(jobId)
                .stream()
                .map(this::toExecutionLog)
                .toList();

        return new DlqJobDetailDTO(
                job.getId(),
                job.getJobType(),
                job.getJobPriority(),
                readPayload(job.getPayload()),
                job.getCronExpression(),
                job.getLastErrorMessage(),
                getAttemptCount(job.getId()),
                job.getDeadLetterStatus(),
                job.getDeadLetterQueuedAt(),
                job.getDeadLetterSentAt(),
                job.getDeadLetterLastAttemptAt(),
                job.getNextDeadLetterAttemptAt(),
                job.getDeadLetterErrorMessage(),
                job.getRequeuedFromJobId(),
                job.getRequeuedAt(),
                job.getCreatedAt(),
                job.getUpdatedAt(),
                executionLogs,
                true,
                job.getDeadLetterStatus() == DeadLetterStatus.PENDING
        );
    }

    public JobDetailDTO getJob(UUID jobId) {
        return toDetail(findById(jobId));
    }

    public List<ExecutionLogDTO> getExecutionLogs(UUID jobId) {
        if (!jobRepository.existsById(jobId)) {
            throw new EntityNotFoundException(JOB_NOT_FOUND_MESSAGE);
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
        requeuedJob.setMaxAttempts(deadJob.getMaxAttempts());
        requeuedJob.setIdempotencyKey(deadJob.getIdempotencyKey() + ":requeue:" + UUID.randomUUID());
        requeuedJob.setNextRunAt(Instant.now());
        requeuedJob.setRequeuedFromJobId(deadJob.getId());
        requeuedJob.setRequeuedAt(Instant.now());

        Job savedJob = jobRepository.save(requeuedJob);
        publish(new JobRequeuedEvent(savedJob.getJobType(), savedJob.getJobPriority()));
        return new RequeueJobResponseDTO(savedJob.getId());
    }

    @Transactional
    public CancelJobResponseDTO cancelJob(UUID jobId) {
        Job job = findById(jobId);

        if (job.getJobStatus() == JobStatus.SUCCESS
                || job.getJobStatus() == JobStatus.DEAD
                || job.getJobStatus() == JobStatus.CANCELED) {
            throw new IllegalStateException("Only active jobs can be canceled");
        }

        if (job.getJobStatus() == JobStatus.RUNNING) {
            throw new IllegalStateException("RUNNING jobs cannot be canceled");
        }

        job.setJobStatus(JobStatus.CANCELED);
        job.setNextRunAt(null);
        job.setQueuedAt(null);
        job.setStartedAt(null);
        job.setCompletedAt(Instant.now());
        job.setLastErrorMessage("Canceled by request");

        Job savedJob = jobRepository.save(job);
        publish(new JobCanceledEvent(savedJob.getJobType(), savedJob.getJobPriority()));
        return new CancelJobResponseDTO(savedJob.getId(), savedJob.getJobStatus());
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

    private UUID findExistingJobId(String idempotencyKey) {
        return jobRepository.findByIdempotencyKey(idempotencyKey)
                .map(Job::getId)
                .orElse(null);
    }

    @Transactional
    public void updateJobStatus(UUID jobId, JobStatus jobStatus) {
        Job job = findById(jobId);
        job.setJobStatus(jobStatus);
        if (jobStatus == JobStatus.QUEUED) {
            job.setQueuedAt(Instant.now());
        }
        if (jobStatus == JobStatus.RUNNING) {
            job.setStartedAt(Instant.now());
            job.setCompletedAt(null);
        }
        if (jobStatus == JobStatus.SUCCESS || jobStatus == JobStatus.DEAD || jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.CANCELED) {
            job.setNextRunAt(null);
        }
        if (jobStatus == JobStatus.SUCCESS || jobStatus == JobStatus.DEAD || jobStatus == JobStatus.CANCELED) {
            job.setCompletedAt(Instant.now());
        }
        if (jobStatus == JobStatus.DEAD) {
            markDeadLetterPending(job, null);
        }
        if (jobStatus == JobStatus.PENDING || jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.SUCCESS || jobStatus == JobStatus.DEAD || jobStatus == JobStatus.CANCELED) {
            job.setQueuedAt(null);
        }
        jobRepository.save(job);
    }

    @Transactional
    public void markJobDead(UUID jobId, String errorMessage) {
        Job job = findById(jobId);
        job.setJobStatus(JobStatus.DEAD);
        job.setNextRunAt(null);
        job.setQueuedAt(null);
        job.setCompletedAt(Instant.now());
        job.setLastErrorMessage(errorMessage);
        markDeadLetterPending(job, errorMessage);
        jobRepository.save(job);
        publish(new JobDeadLetteredEvent(job.getJobType(), job.getJobPriority()));
    }

    @Transactional
    public void markDeadLetterAttempt(UUID jobId, Instant nextAttemptAt) {
        Job job = findById(jobId);
        job.setDeadLetterLastAttemptAt(Instant.now());
        job.setNextDeadLetterAttemptAt(nextAttemptAt);
        jobRepository.save(job);
    }

    @Transactional
    public void markDeadLetterSent(UUID jobId) {
        Job job = findById(jobId);
        job.setDeadLetterStatus(DeadLetterStatus.SENT);
        job.setDeadLetterSentAt(Instant.now());
        job.setNextDeadLetterAttemptAt(null);
        job.setDeadLetterErrorMessage(null);
        jobRepository.save(job);
    }

    @Transactional
    public void markDeadLetterRetry(UUID jobId, Instant nextAttemptAt, String errorMessage) {
        Job job = findById(jobId);
        job.setDeadLetterStatus(DeadLetterStatus.PENDING);
        job.setNextDeadLetterAttemptAt(nextAttemptAt);
        job.setDeadLetterErrorMessage(errorMessage);
        jobRepository.save(job);
    }

    @Transactional
    public void scheduleNextCronRun(UUID jobId) {
        Job job = findById(jobId);
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

    // Combined start-of-execution write: flips the job from QUEUED -> RUNNING and creates
    // its execution log row in a single transaction. Replaces three separate transactions
    // (updateJobStatus + ExecutionLogService.createEntry + ExecutionLogService.updateExecutionStatus)
    // along the worker hot path. Returns the new ExecutionLog so the caller can update it on completion.
    @Transactional
    public ExecutionLog markJobStartingAtomic(UUID jobId, String workerId) {
        Job job = findById(jobId);
        Instant now = Instant.now();

        job.setJobStatus(JobStatus.RUNNING);
        job.setStartedAt(now);
        job.setCompletedAt(null);
        job.setNextRunAt(null);
        job.setQueuedAt(null);
        jobRepository.save(job);

        ExecutionLog log = new ExecutionLog();
        log.setJobDetails(job);
        long retryCount = executionLogRepository.countByJobId(jobId);
        log.setAttemptNumber((int) (retryCount + 1));
        log.setExecutionStatus(JobStatus.RUNNING);
        log.setWorkerId(workerId);
        log.setStartedAt(now);
        log.setCompletedAt(null);
        log.setDurationMs(null);
        log.setErrorMessage(null);
        return executionLogRepository.save(log);
    }

    // Combined end-of-execution write for the SUCCESS path. Marks the execution log SUCCESS
    // and either flips the job to SUCCESS (one-shot) or schedules the next cron fire (recurring),
    // all in a single transaction.
    @Transactional
    public void markJobCompletedAtomic(UUID jobId, ExecutionLog executionLog, String workerId) {
        Job job = findById(jobId);
        Instant now = Instant.now();

        executionLog.setExecutionStatus(JobStatus.SUCCESS);
        executionLog.setWorkerId(workerId);
        executionLog.setCompletedAt(now);
        executionLog.setErrorMessage(null);
        if (executionLog.getStartedAt() != null) {
            executionLog.setDurationMs(Duration.between(executionLog.getStartedAt(), now).toMillis());
        }
        executionLogRepository.save(executionLog);

        if (hasCronExpression(job)) {
            CronExpression cronExpression = parseCronExpression(job.getCronExpression());
            if (cronExpression == null) {
                job.setJobStatus(JobStatus.SUCCESS);
                job.setNextRunAt(null);
                job.setCompletedAt(now);
            } else {
                job.setJobStatus(JobStatus.PENDING);
                job.setNextRunAt(nextRunAt(cronExpression));
                job.setQueuedAt(null);
                job.setStartedAt(null);
                job.setCompletedAt(null);
                job.setLastErrorMessage(null);
            }
        } else {
            job.setJobStatus(JobStatus.SUCCESS);
            job.setCompletedAt(now);
            job.setNextRunAt(null);
            job.setQueuedAt(null);
        }
        jobRepository.save(job);
    }

    @Transactional
    public void scheduleRetry(UUID jobId, Instant nextRunAt, String errorMessage) {
        Job job = findById(jobId);
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(nextRunAt);
        job.setQueuedAt(null);
        job.setStartedAt(null);
        job.setCompletedAt(null);
        job.setLastErrorMessage(errorMessage);
        jobRepository.save(job);
    }

    @Transactional
    public void markDispatchQueued(UUID jobId) {
        Job job = findById(jobId);
        job.setJobStatus(JobStatus.QUEUED);
        job.setNextRunAt(null);
        job.setQueuedAt(Instant.now());
        jobRepository.save(job);
    }

    @Transactional
    public void markDispatchSucceeded(UUID jobId) {
        Job job = findById(jobId);
        job.setJobStatus(JobStatus.QUEUED);
        job.setNextRunAt(null);
        job.setQueuedAt(Instant.now());
        jobRepository.save(job);
        publish(new JobDispatchedEvent(job.getJobType(), job.getJobPriority()));
    }

    @Transactional
    public void markDispatchAttempt(UUID jobId, Instant retryDispatchAt) {
        Job job = findById(jobId);
        job.setNextRunAt(retryDispatchAt);
        jobRepository.save(job);
    }

    @Transactional
    public void recoverQueuedJob(UUID jobId) {
        Job job = findById(jobId);
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(Instant.now());
        job.setQueuedAt(null);
        jobRepository.save(job);
    }

    @Transactional
    public void recoverRunningJob(UUID jobId) {
        Job job = findById(jobId);
        job.setJobStatus(JobStatus.PENDING);
        job.setNextRunAt(Instant.now());
        job.setQueuedAt(null);
        job.setStartedAt(null);
        job.setCompletedAt(null);
        job.setLastErrorMessage("Recovered by running-job watchdog");
        jobRepository.save(job);
    }

    public boolean maxAttemptsExceeded(UUID jobId) {
        Job job = findById(jobId);
        return getAttemptCount(jobId) >= job.getMaxAttempts();
    }

    public long getAttemptCount(UUID jobId) {
        return executionLogRepository.countByJobId(jobId);
    }

    public Job findById(UUID jobId) {
        return jobRepository.findById(jobId).orElseThrow(() -> new EntityNotFoundException(JOB_NOT_FOUND_MESSAGE));
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

    private Specification<Job> jobFilters(
            JobStatus status,
            JobType type,
            JobPriority priority,
            Instant createdFrom,
            Instant createdTo
    ) {
        return (root, query, criteriaBuilder) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (status != null) {
                predicates.add(criteriaBuilder.equal(root.get("jobStatus"), status));
            }
            if (type != null) {
                predicates.add(criteriaBuilder.equal(root.get("jobType"), type));
            }
            if (priority != null) {
                predicates.add(criteriaBuilder.equal(root.get("jobPriority"), priority));
            }
            if (createdFrom != null) {
                predicates.add(criteriaBuilder.greaterThanOrEqualTo(root.get(CREATED_AT_FIELD), createdFrom));
            }
            if (createdTo != null) {
                predicates.add(criteriaBuilder.lessThanOrEqualTo(root.get(CREATED_AT_FIELD), createdTo));
            }

            return criteriaBuilder.and(predicates.toArray(Predicate[]::new));
        };
    }

    private Specification<Job> deadLetterFilters(
            DeadLetterStatus deadLetterStatus,
            JobType type,
            JobPriority priority,
            Instant createdFrom,
            Instant createdTo
    ) {
        return (root, query, criteriaBuilder) -> {
            List<Predicate> predicates = new ArrayList<>();

            predicates.add(criteriaBuilder.equal(root.get("jobStatus"), JobStatus.DEAD));
            if (deadLetterStatus != null) {
                predicates.add(criteriaBuilder.equal(root.get("deadLetterStatus"), deadLetterStatus));
            }
            if (type != null) {
                predicates.add(criteriaBuilder.equal(root.get("jobType"), type));
            }
            if (priority != null) {
                predicates.add(criteriaBuilder.equal(root.get("jobPriority"), priority));
            }
            if (createdFrom != null) {
                predicates.add(criteriaBuilder.greaterThanOrEqualTo(root.get(CREATED_AT_FIELD), createdFrom));
            }
            if (createdTo != null) {
                predicates.add(criteriaBuilder.lessThanOrEqualTo(root.get(CREATED_AT_FIELD), createdTo));
            }

            return criteriaBuilder.and(predicates.toArray(Predicate[]::new));
        };
    }

    private DlqJobSummaryDTO toDlqSummary(Job job) {
        return new DlqJobSummaryDTO(
                job.getId(),
                job.getJobType(),
                job.getJobPriority(),
                job.getLastErrorMessage(),
                getAttemptCount(job.getId()),
                job.getDeadLetterStatus(),
                job.getDeadLetterQueuedAt(),
                job.getDeadLetterSentAt(),
                job.getDeadLetterLastAttemptAt(),
                job.getNextDeadLetterAttemptAt(),
                job.getDeadLetterErrorMessage(),
                job.getRequeuedFromJobId(),
                job.getRequeuedAt(),
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
                readPayload(job.getPayload()),
                job.getCronExpression(),
                job.getMaxAttempts(),
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

    private String writePayload(JsonNode payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not serialize job payload", e);
        }
    }

    private JsonNode readPayload(String payload) {
        try {
            return objectMapper.readTree(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Could not deserialize persisted job payload", e);
        }
    }

    private void publish(Object event) {
        eventPublisher.publishEvent(event);
    }
}
