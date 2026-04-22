package com.job.scheduler.repository;

import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.DeadLetterStatus;
import com.job.scheduler.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID>, JpaSpecificationExecutor<Job> {
    List<Job> findAllByOrderByCreatedAtDesc();

    List<Job> findByJobStatusOrderByUpdatedAtDesc(JobStatus jobStatus);

    List<Job> findByJobStatusAndNextRunAtLessThanEqual(JobStatus jobStatus, Instant now);

    List<Job> findByJobStatusAndQueuedAtLessThanEqual(JobStatus jobStatus, Instant cutoff);

    List<Job> findByJobStatusAndStartedAtLessThanEqual(JobStatus jobStatus, Instant cutoff);

    List<Job> findByJobStatusAndDeadLetterStatusAndNextDeadLetterAttemptAtLessThanEqual(
            JobStatus jobStatus,
            DeadLetterStatus deadLetterStatus,
            Instant now
    );
}
