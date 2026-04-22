package com.job.scheduler.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Entity
@Data
@Table(
        name = "jobs",
        indexes = {
                @Index(name = "idx_jobs_status_next_run_at", columnList = "job_status,next_run_at"),
                @Index(name = "idx_jobs_status_queued_at", columnList = "job_status,queued_at"),
                @Index(name = "idx_jobs_status_started_at", columnList = "job_status,started_at"),
                @Index(name = "idx_jobs_status_updated_at", columnList = "job_status,updated_at"),
                @Index(name = "idx_jobs_status_dead_letter_next_attempt", columnList = "job_status,dead_letter_status,next_dead_letter_attempt_at"),
                @Index(name = "idx_jobs_created_at", columnList = "created_at"),
                @Index(name = "idx_jobs_requeued_from_job_id", columnList = "requeued_from_job_id")
        }
)
public class Job {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_type",  nullable = false)
    private JobType jobType;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_status",  nullable = false)
    private JobStatus jobStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_priority",  nullable = false)
    private JobPriority jobPriority;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private JsonNode payload;

    @Column(name = "cron_expression")
    private String cronExpression;

    @Column(name = "max_retries", nullable = false)
    @ColumnDefault("3")
    private int maxRetries = 3;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "next_run_at")
    private Instant nextRunAt;

    @Column(name = "queued_at")
    private Instant queuedAt;

    @Column(name = "last_error_message", columnDefinition = "TEXT")
    private String lastErrorMessage;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "requeued_from_job_id")
    private UUID requeuedFromJobId;

    @Column(name = "requeued_at")
    private Instant requeuedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "dead_letter_status")
    private DeadLetterStatus deadLetterStatus;

    @Column(name = "dead_letter_queued_at")
    private Instant deadLetterQueuedAt;

    @Column(name = "dead_letter_sent_at")
    private Instant deadLetterSentAt;

    @Column(name = "dead_letter_last_attempt_at")
    private Instant deadLetterLastAttemptAt;

    @Column(name = "next_dead_letter_attempt_at")
    private Instant nextDeadLetterAttemptAt;

    @Column(name = "dead_letter_error_message", columnDefinition = "TEXT")
    private String deadLetterErrorMessage;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "jobDetails", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ExecutionLog> executionLogs;
}
