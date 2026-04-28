package com.job.scheduler.repository;

import com.job.scheduler.entity.Job;
import com.job.scheduler.enums.JobPriority;
import com.job.scheduler.enums.JobStatus;
import com.job.scheduler.enums.JobType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create");
    }

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private EntityManager entityManager;

    @Test
    void claimDueJobsForDispatchClaimsOnlyDuePendingJobs() {
        Job duePending = saveJob(JobStatus.PENDING, Instant.now().minusSeconds(10), "due-pending");
        saveJob(JobStatus.PENDING, Instant.now().plusSeconds(60), "future-pending");
        saveJob(JobStatus.QUEUED, Instant.now().minusSeconds(10), "queued-job");

        Instant retryAt = Instant.now().plusSeconds(30);
        List<Job> claimedJobs = jobRepository.claimDueJobsForDispatch(Instant.now(), retryAt, 10);

        assertThat(claimedJobs).hasSize(1);
        assertThat(claimedJobs.get(0).getId()).isEqualTo(duePending.getId());

        entityManager.clear();
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

        entityManager.clear();
        Job refreshedSecond = jobRepository.findById(second.getId()).orElseThrow();
        assertThat(refreshedSecond.getNextRunAt()).isBeforeOrEqualTo(Instant.now());
    }

    @Test
    void savingDuplicateIdempotencyKeyFails() {
        Instant now = Instant.now();
        saveJob(JobStatus.PENDING, now, "same-key");

        assertThatThrownBy(() -> saveJob(JobStatus.PENDING, now, "same-key"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void savePersistsJsonbPayload() {
        Job job = new Job();
        job.setJobType(JobType.WEBHOOK);
        job.setJobStatus(JobStatus.PENDING);
        job.setJobPriority(JobPriority.MEDIUM);
        job.setPayload(OBJECT_MAPPER.createObjectNode()
                .put("url", "https://example.com/hook")
                .set("body", OBJECT_MAPPER.createObjectNode().put("kind", "integration"))
                .toString());
        job.setIdempotencyKey("jsonb-payload");
        job.setNextRunAt(Instant.now());

        Job saved = jobRepository.saveAndFlush(job);
        Job reloaded = jobRepository.findById(saved.getId()).orElseThrow();
        var payload = OBJECT_MAPPER.readTree(reloaded.getPayload());

        assertThat(payload.get("url").stringValue()).isEqualTo("https://example.com/hook");
        assertThat(payload.get("body").get("kind").stringValue()).isEqualTo("integration");
    }

    private Job saveJob(JobStatus status, Instant nextRunAt, String idempotencyKey) {
        Job job = new Job();
        job.setJobType(JobType.WEBHOOK);
        job.setJobStatus(status);
        job.setJobPriority(JobPriority.MEDIUM);
        job.setPayload(OBJECT_MAPPER.createObjectNode().put("url", "https://example.com/hook").toString());
        job.setIdempotencyKey(idempotencyKey);
        job.setNextRunAt(nextRunAt);
        return jobRepository.saveAndFlush(job);
    }
}
