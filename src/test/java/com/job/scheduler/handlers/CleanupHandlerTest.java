package com.job.scheduler.handlers;

import com.job.scheduler.dto.payload.CleanupPayload;
import com.job.scheduler.repository.ExecutionLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CleanupHandlerTest {

    @Mock
    private ExecutionLogRepository executionLogRepository;

    private CleanupHandler cleanupHandler;

    @BeforeEach
    void setUp() {
        cleanupHandler = new CleanupHandler(executionLogRepository);
    }

    @Test
    void handleDeletesLogsOlderThanRequestedDays() {
        when(executionLogRepository.deleteOlderThan(org.mockito.ArgumentMatchers.any())).thenReturn(7);

        Instant before = Instant.now();
        cleanupHandler.handle(new CleanupPayload(30));
        Instant after = Instant.now();

        ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
        verify(executionLogRepository).deleteOlderThan(captor.capture());

        Instant cutoff = captor.getValue();
        Instant expectedEarliest = before.minus(30, ChronoUnit.DAYS);
        Instant expectedLatest = after.minus(30, ChronoUnit.DAYS);

        assertThat(cutoff).isBetween(expectedEarliest.minusSeconds(1), expectedLatest.plusSeconds(1));
    }
}
