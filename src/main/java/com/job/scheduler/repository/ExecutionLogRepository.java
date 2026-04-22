package com.job.scheduler.repository;

import com.job.scheduler.entity.ExecutionLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface ExecutionLogRepository extends JpaRepository<ExecutionLog, UUID> {
    @Query("SELECT COUNT(e) FROM ExecutionLog e JOIN e.jobDetails j WHERE j.id = :jobId")
    Long countByJobId(@Param("jobId") UUID jobId);

    @Query("SELECT e FROM ExecutionLog e JOIN e.jobDetails j WHERE j.id = :jobId ORDER BY e.attemptNumber ASC")
    List<ExecutionLog> findByJobIdOrderByAttemptNumberAsc(@Param("jobId") UUID jobId);

    @Modifying
    @Query("DELETE FROM ExecutionLog e WHERE e.createdAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") Instant cutoff);
}
