package com.job.scheduler.repository;

import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class JobRepositoryIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("jobscheduler")
            .withUsername("postgres")
            .withPassword("postgres");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private JobRepository jobRepository;

    @Test
    void claimDueJobsForDispatchClaimsOnlyDuePendingJobs() {
        Job duePending = saveJob(JobStatus.PENDING, Instant.now().minusSeconds(10), "due-pending");
        saveJob(JobStatus.PENDING, Instant.now().plusSeconds(60), "future-pending");
        saveJob(JobStatus.QUEUED, Instant.now().minusSeconds(10), "queued-job");

        Instant retryAt = Instant.now().plusSeconds(30);
        List<Job> claimedJobs = jobRepository.claimDueJobsForDispatch(Instant.now(), retryAt, 10);

        assertThat(claimedJobs).hasSize(1);
        assertThat(claimedJobs.get(0).getId()).isEqualTo(duePending.getId());
        assertThat(Math.abs(claimedJobs.get(0).getNextRunAt().toEpochMilli() - retryAt.toEpochMilli()))
                .isLessThanOrEqualTo(1000);

        Job refreshed = jobRepository.findById(duePending.getId()).orElseThrow();
        assertThat(Math.abs(refreshed.getNextRunAt().toEpochMilli() - retryAt.toEpochMilli()))
                .isLessThanOrEqualTo(1000);
    }

    @Test
    void claimDueJobsForDispatchRespectsLimit() {
        Job first = saveJob(JobStatus.PENDING, Instant.now().minusSeconds(20), "first");
        Job second = saveJob(JobStatus.PENDING, Instant.now().minusSeconds(10), "second");

        List<Job> claimedJobs = jobRepository.claimDueJobsForDispatch(
                Instant.now(),
                Instant.now().plusSeconds(30),
                1
        );

        assertThat(claimedJobs).hasSize(1);
        assertThat(claimedJobs.get(0).getId()).isEqualTo(first.getId());

        Job refreshedSecond = jobRepository.findById(second.getId()).orElseThrow();
        assertThat(refreshedSecond.getNextRunAt()).isBeforeOrEqualTo(Instant.now());
    }

    private Job saveJob(JobStatus status, Instant nextRunAt, String idempotencyKey) {
        Job job = new Job();
        job.setJobType(JobType.WEBHOOK);
        job.setJobStatus(status);
        job.setJobPriority(JobPriority.MEDIUM);
        job.setPayload(OBJECT_MAPPER.createObjectNode().put("url", "https://example.com/hook"));
        job.setIdempotencyKey(idempotencyKey);
        job.setNextRunAt(nextRunAt);
        return jobRepository.saveAndFlush(job);
    }
}
