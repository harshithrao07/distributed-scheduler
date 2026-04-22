package com.job.scheduler.handlers;

import com.job.scheduler.dto.payload.CleanupPayload;
import com.job.scheduler.repository.ExecutionLogRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class CleanupHandler implements JobHandler<CleanupPayload> {
    private final ExecutionLogRepository executionLogRepository;

    @Override
    @Transactional
    public void handle(CleanupPayload payload) {
        Instant cutoff = Instant.now().minus(payload.olderThanDays(), ChronoUnit.DAYS);
        int deletedCount = executionLogRepository.deleteOlderThan(cutoff);
        log.info("Deleted {} execution log rows older than {} days", deletedCount, payload.olderThanDays());
    }
}
