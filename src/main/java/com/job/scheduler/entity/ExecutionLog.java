package com.job.scheduler.entity;

import com.job.scheduler.enums.JobStatus;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Data
@Table(
        name = "execution_logs",
        indexes = {
                @Index(name = "idx_execution_logs_job_id", columnList = "job_id"),
                @Index(name = "idx_execution_logs_job_id_attempt_number", columnList = "job_id,attempt_number"),
                @Index(name = "idx_execution_logs_created_at", columnList = "created_at")
        }
)
public class ExecutionLog {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", referencedColumnName = "id")
    private Job jobDetails;

    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "execution_status", nullable = false)
    private JobStatus executionStatus;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "duration_ms")
    private Long durationMs;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "worker_id")
    private String workerId;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
}
