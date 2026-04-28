# Distributed Job Scheduler

A Spring Boot based distributed job scheduler that accepts background jobs via a REST API, stores them durably in PostgreSQL, dispatches due jobs to Kafka, and processes them with workers. Built to demonstrate how production schedulers handle durability, retries, observability, and recovery from real failure modes.

---

## Table of Contents

- [Architecture](#architecture)
- [Request Flow](#request-flow)
- [Job Lifecycle](#job-lifecycle)
- [Scheduling Patterns](#scheduling-patterns)
- [Reliability Features](#reliability-features)
- [REST API](#rest-api)
- [Job Types](#job-types)
- [Persistence Model](#persistence-model)
- [Kafka Topics and Redis Keys](#kafka-topics-and-redis-keys)
- [Local Infrastructure](#local-infrastructure)
- [Configuration](#configuration)
- [Package Layout](#package-layout)
- [Status](#status)
- [System Design Talking Points](#system-design-talking-points)
- [Local Verification](#local-verification)

---

## Architecture

The API never depends on Kafka being available. Jobs are written to PostgreSQL first with a `nextRunAt` timestamp. A scheduler component polls for due jobs and dispatches them to Kafka asynchronously. PostgreSQL is the source of truth; Kafka is the delivery mechanism.

```mermaid
flowchart TD
    client["Client"]
    api["REST API<br/>JobController"]
    jobService["JobService"]
    postgres[("PostgreSQL<br/>jobs + execution_logs")]

    client --> api --> jobService --> postgres

    subgraph Dispatch
        scheduler["DueJobSchedulerService"]
        producer["JobQueueProducer"]
    end

    postgres --> scheduler --> producer

    subgraph Kafka Topics
        kafkaMain[["job-queue"]]
        kafkaHigh[["job-queue-high"]]
        dlq[["job-dlq"]]
    end

    producer --> kafkaMain
    producer --> kafkaHigh
    producer --> dlq

    subgraph Workers
        consumer["JobQueueConsumer"]
        worker["WorkerService"]
        handlers["JobHandlerRouter"]
    end

    kafkaMain --> consumer
    kafkaHigh --> consumer
    consumer --> worker --> handlers

    subgraph Handler Types
        email["SEND_EMAIL"]
        webhook["WEBHOOK"]
        cleanup["CLEANUP"]
    end

    handlers --> email
    handlers --> webhook
    handlers --> cleanup

    worker --> postgres
    worker <--> redis[("Redis<br/>locks + done markers")]

    subgraph Recovery
        queuedWatchdog["QueuedJobWatchdogService"]
        runningWatchdog["RunningJobWatchdogService"]
        dlqScheduler["DeadLetterSchedulerService"]
        redisMonitor["RedisKafkaListenerPauseService"]
    end

    queuedWatchdog --> postgres
    runningWatchdog --> postgres
    dlqScheduler --> postgres
    dlqScheduler --> producer
    redisMonitor --> redis
    redisMonitor --> consumer
```

---

## Request Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant API as JobController
    participant DB as PostgreSQL
    participant Sched as DueJobSchedulerService
    participant Kafka
    participant Worker as WorkerService
    participant Redis
    participant Handler as JobHandler

    Client->>API: POST /app/v1/jobs
    API->>DB: Insert job as PENDING
    API-->>Client: jobId

    Sched->>DB: Poll due PENDING jobs
    Sched->>Kafka: Publish JobDispatchEvent
    Sched->>DB: Mark job QUEUED

    Kafka->>Worker: Deliver job event
    Worker->>Redis: Check done marker and acquire lock
    Worker->>DB: Mark RUNNING, create execution log
    Worker->>Handler: Execute typed job

    alt One-time success
        Handler-->>Worker: Result
        Worker->>DB: Mark SUCCESS
        Worker->>Redis: Release lock, write done marker
    else Cron success
        Handler-->>Worker: Result
        Worker->>DB: Mark PENDING with next cron fire time
        Worker->>Redis: Release lock
    else Failure
        Handler-->>Worker: Error
        Worker->>DB: Mark PENDING for retry or DEAD if exhausted
        Worker->>Redis: Release lock
    end
```

---

## Job Lifecycle

```mermaid
stateDiagram-v2
    direction LR
    [*] --> PENDING : submitted

    PENDING --> QUEUED : scheduler dispatched to Kafka
    QUEUED --> RUNNING : worker picked up

    RUNNING --> SUCCESS : one-time job complete
    RUNNING --> PENDING : retry scheduled
    RUNNING --> PENDING : cron rescheduled
    RUNNING --> DEAD : max attempts exceeded

    QUEUED --> PENDING : queued watchdog reset
    RUNNING --> PENDING : running watchdog reset

    PENDING --> CANCELED : canceled by API
    QUEUED --> CANCELED : canceled by API

    SUCCESS --> [*]
    DEAD --> [*]
    CANCELED --> [*]
```

| Status | Meaning |
|---|---|
| `PENDING` | Stored in DB, waiting for `nextRunAt` |
| `QUEUED` | Kafka accepted the job; awaiting worker pickup |
| `RUNNING` | Worker is actively processing |
| `SUCCESS` | Terminal success for one-time jobs |
| `FAILED` | Transient failure before retry or `DEAD` |
| `DEAD` | Terminal failure after max attempts or permanent error |
| `CANCELED` | Terminal state for jobs canceled before execution starts |

---

## Scheduling Patterns

### Immediate Jobs

```mermaid
flowchart LR
    A["POST /app/v1/jobs"] --> B["PENDING<br/>nextRunAt = now"]
    B --> C["QUEUED<br/>Kafka accepted"]
    C --> D["RUNNING<br/>lock acquired"]
    D --> E["SUCCESS"]
    D --> F["PENDING<br/>retry + backoff"]
    D --> G["DEAD"]
```

### Cron Jobs

Spring cron expressions use 6 fields, including seconds. Example: `0 */5 * * * *` runs every 5 minutes.

```mermaid
flowchart LR
    A["POST /app/v1/jobs<br/>cronExpression set"] --> B["PENDING<br/>nextRunAt = next fire"]
    B --> C["QUEUED"]
    C --> D["RUNNING"]
    D --> E["PENDING<br/>nextRunAt = following fire"]
    E --> C
```

### Retryable Failures

Retry delay follows exponential backoff: `1s`, `2s`, `4s`, and so on, capped by configuration.

```mermaid
flowchart LR
    A["RUNNING"] --> B["FAILED"]
    B --> C["PENDING<br/>nextRunAt = now + backoff"]
    C --> D["QUEUED"]
    D --> A
```

### Permanent Failures and Dead Letter

Permanent failures include missing jobs, invalid payloads, unsupported job types, and max attempts exceeded.

```mermaid
flowchart LR
    A["RUNNING"] --> B["DEAD"]
    B --> C["deadLetterStatus<br/>= PENDING"]
    C --> D["DeadLetterSchedulerService"]
    D --> E{"Publish<br/>succeeds?"}
    E -->|yes| F["deadLetterStatus<br/>= SENT"]
    E -->|no| G["Keep PENDING<br/>nextDeadLetterAttemptAt<br/>= later"]
    G --> D
```

---

## Reliability Features

### DB-Backed Dispatch

Jobs survive Kafka outages. If Kafka is unavailable at dispatch time, the job stays `PENDING` and the scheduler retries later.

```mermaid
flowchart LR
    A["Job persisted<br/>as PENDING"] --> B["Scheduler polls<br/>due jobs"]
    B --> C{"Kafka publish<br/>succeeds?"}
    C -->|yes| D["Mark QUEUED"]
    C -->|no| E["Keep PENDING<br/>advance nextRunAt"]
    E --> B
```

### Watchdogs

Two independent watchdogs recover jobs from different stuck states:

| Watchdog | Trigger | Recovery |
|---|---|---|
| `QUEUED` watchdog | Job stuck `QUEUED` past timeout | Reset to `PENDING`, `nextRunAt = now` |
| `RUNNING` watchdog | Job stuck `RUNNING` past timeout | Reset to `PENDING`, `nextRunAt = now` |

### Redis Locking With Lua

Workers use Redis locks with Lua scripts to ensure ownership-safe release and renewal. A worker only releases or renews a lock if it still holds the exact token.

Lock value format:

```text
workerId:randomUUID
```

This prevents a worker from deleting another worker's lock after TTL expiry and reacquisition.

### Redis-Aware Kafka Pause

Workers depend on Redis for locking and idempotency. `RedisKafkaListenerPauseService` monitors Redis health and pauses Kafka consumers when Redis is unavailable, then resumes them once healthy.

```mermaid
flowchart LR
    A["RedisKafkaListenerPauseService<br/>polls every 5s"] --> B{"Redis<br/>healthy?"}
    B -->|yes| C["Resume Kafka<br/>listeners"]
    B -->|no| D["Pause Kafka<br/>listeners"]
```

### Idempotency Marker

Completed one-time jobs write a `job-done:{jobId}` key to Redis with a 24 hour TTL. If Kafka redelivers the same event, the worker checks this marker and skips already-completed work.

### External Call Timeouts

Webhook and mail sends have explicit timeouts to prevent worker threads from hanging indefinitely on stuck dependencies.

---

## REST API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/app/v1/jobs` | Submit a new job |
| `GET` | `/app/v1/jobs` | List jobs with pagination and optional filters |
| `GET` | `/app/v1/jobs/dead` | List dead jobs |
| `GET` | `/app/v1/jobs/{jobId}` | Get job detail |
| `GET` | `/app/v1/jobs/{jobId}/logs` | Get execution logs |
| `POST` | `/app/v1/jobs/{jobId}/requeue` | Re-create a dead job |
| `POST` | `/app/v1/jobs/{jobId}/cancel` | Cancel a pending or queued job |
| `GET` | `/app/v1/dlq` | Inspect dead-letter jobs with pagination and filters |
| `GET` | `/app/v1/dlq/{jobId}` | Inspect one dead-letter job with execution logs |

### List Jobs Query Parameters

`GET /app/v1/jobs` returns a paginated response sorted by newest jobs first.

| Parameter | Required | Example | Purpose |
|---|---|---|---|
| `status` | No | `PENDING` | Filter by job status |
| `type` | No | `WEBHOOK` | Filter by job type |
| `priority` | No | `HIGH` | Filter by job priority |
| `createdFrom` | No | `2026-04-01T00:00:00Z` | Include jobs created at or after this time |
| `createdTo` | No | `2026-04-22T23:59:59Z` | Include jobs created at or before this time |
| `page` | No | `0` | Zero-based page number. Defaults to `0` |
| `size` | No | `20` | Page size. Defaults to `20`, capped at `100` |

Example:

```text
GET /app/v1/jobs?status=PENDING&type=WEBHOOK&priority=HIGH&page=0&size=10
```

Response shape:

```json
{
  "content": [],
  "page": 0,
  "size": 10,
  "totalElements": 0,
  "totalPages": 0,
  "first": true,
  "last": true
}
```

### Cancellation

`POST /app/v1/jobs/{jobId}/cancel` cancels jobs that have not started running yet.

Cancelable states:

| State | Behavior |
|---|---|
| `PENDING` | Marked `CANCELED` and removed from scheduling |
| `QUEUED` | Marked `CANCELED`; if Kafka later delivers the event, the worker skips it |

Already terminal jobs and currently `RUNNING` jobs are rejected.

### DLQ Inspection

`GET /app/v1/dlq` returns only `DEAD` jobs and includes DLQ publish metadata, attempt counts, final errors, and requeue metadata.

| Parameter | Required | Example | Purpose |
|---|---|---|---|
| `deadLetterStatus` | No | `PENDING` | Filter by DLQ publish state |
| `type` | No | `WEBHOOK` | Filter by job type |
| `priority` | No | `HIGH` | Filter by job priority |
| `createdFrom` | No | `2026-04-01T00:00:00Z` | Include jobs created at or after this time |
| `createdTo` | No | `2026-04-22T23:59:59Z` | Include jobs created at or before this time |
| `page` | No | `0` | Zero-based page number. Defaults to `0` |
| `size` | No | `20` | Page size. Defaults to `20`, capped at `100` |

Example:

```text
GET /app/v1/dlq?deadLetterStatus=PENDING&type=WEBHOOK&page=0&size=10
```

Summary response items include:

```json
{
  "jobId": "4b77f1d2-0000-0000-0000-000000000000",
  "jobType": "WEBHOOK",
  "jobPriority": "HIGH",
  "lastErrorMessage": "Max attempts exceeded",
  "attemptCount": 3,
  "deadLetterStatus": "PENDING",
  "deadLetterQueuedAt": "2026-04-22T03:30:10Z",
  "deadLetterSentAt": null,
  "deadLetterLastAttemptAt": "2026-04-22T03:30:12Z",
  "nextDeadLetterAttemptAt": "2026-04-22T03:30:42Z",
  "deadLetterErrorMessage": "Kafka timeout",
  "requeuedFromJobId": null,
  "requeuedAt": null
}
```

`GET /app/v1/dlq/{jobId}` returns the full payload, final error, DLQ metadata, execution logs, and action flags such as `canRequeue` and `canRetryDeadLetterPublish`.

### Example Requests

Immediate webhook:

```json
{
  "jobType": "WEBHOOK",
  "jobPriority": "HIGH",
  "payload": {
    "url": "https://example.com/webhook",
    "body": { "event": "demo" }
  },
  "maxAttempts": 3,
  "idempotencyKey": "webhook-demo-001"
}
```

Recurring webhook:

```json
{
  "jobType": "WEBHOOK",
  "jobPriority": "MEDIUM",
  "cronExpression": "0 */5 * * * *",
  "payload": {
    "url": "https://example.com/webhook",
    "body": { "event": "cron-demo" }
  },
  "maxAttempts": 3,
  "idempotencyKey": "webhook-cron-demo-001"
}
```

Cleanup job:

```json
{
  "jobType": "CLEANUP",
  "jobPriority": "LOW",
  "payload": { "olderThanDays": 30 },
  "maxAttempts": 3,
  "idempotencyKey": "cleanup-logs-30-days"
}
```

---

## Job Types

| Type | Status | Purpose |
|---|---|---|
| `SEND_EMAIL` | Implemented | Sends email via Spring Mail |
| `WEBHOOK` | Implemented | Sends HTTP POST with JSON payload |
| `CLEANUP` | Implemented | Deletes old execution logs |

Planned future handlers include `REPORT` and `SCRAPE`.

---

## Persistence Model

```mermaid
erDiagram
    JOBS ||--o{ EXECUTION_LOGS : "has many"

    JOBS {
        uuid id PK
        string job_type
        string job_status
        string job_priority
        jsonb payload
        string cron_expression
        int max_attempts
        string idempotency_key UK
        timestamp next_run_at
        timestamp queued_at
        timestamp started_at
        timestamp completed_at
        string last_error_message
        uuid requeued_from_job_id
        timestamp requeued_at
        string dead_letter_status
        timestamp next_dead_letter_attempt_at
        timestamp created_at
        timestamp updated_at
    }

    EXECUTION_LOGS {
        uuid id PK
        uuid job_id FK
        int attempt_number
        string execution_status
        string error_message
        string worker_id
        timestamp started_at
        timestamp completed_at
        long duration_ms
        timestamp created_at
    }
```

`jobs` stores one row per submitted job. Key columns:

| Column | Purpose |
|---|---|
| `next_run_at` | When the scheduler should dispatch this job |
| `queued_at` | When Kafka accepted the job |
| `started_at` | When the worker began processing |
| `completed_at` | Terminal completion timestamp |
| `last_error_message` | Last failure reason or watchdog recovery note |
| `dead_letter_status` | DLQ publish state, such as `PENDING` or `SENT` |
| `next_dead_letter_attempt_at` | Next DLQ retry time |
| `requeued_from_job_id` | Source dead job when manually requeued |

`execution_logs` stores one row per attempt. It tracks duration, worker ID, status, and error per execution.

---

## Kafka Topics and Redis Keys

### Kafka Topics

| Topic | Purpose |
|---|---|
| `job-queue` | Standard-priority jobs |
| `job-queue-high` | High-priority jobs |
| `job-dlq` | Dead-letter queue |

`HIGH` priority jobs route to `job-queue-high`; all others go to `job-queue`.

### Redis Keys

| Key | Purpose | TTL |
|---|---|---|
| `job-lock:{jobId}` | Worker execution lock, renewed while running | 30s |
| `job-done:{jobId}` | Idempotency marker for completed one-time jobs | 24h |

---

## Local Infrastructure

The project includes a Docker Compose setup for the local dependencies:

| Service | Port | Purpose |
|---|---:|---|
| PostgreSQL | `5432` | Durable job and execution log storage |
| Kafka | `9092` | Job queue, high-priority queue, and DLQ |
| Redis | `6379` | Worker locks and idempotency markers |

Start the infrastructure:

```bash
docker compose up -d
```

Stop it:

```bash
docker compose down
```

Remove local volumes if you want a clean database, Kafka log, and Redis state:

```bash
docker compose down -v
```

---

## Configuration

```properties
# Local infrastructure connections
spring.datasource.url=jdbc:postgresql://localhost:5432/jobscheduler
spring.datasource.username=postgres
spring.datasource.password=postgres
spring.kafka.bootstrap-servers=localhost:9092
spring.data.redis.host=localhost
spring.data.redis.port=6379

# Scheduler toggle. Only one instance should run scheduling.
scheduler.enabled=true
scheduler.worker-id=worker-1

# Retry backoff
scheduler.retry.base-delay-ms=1000
scheduler.retry.max-delay-ms=30000

# Due-job poller
scheduler.due-job.poll-delay-ms=1000
scheduler.due-job.dispatch-retry-delay-ms=5000
scheduler.due-job.claim-limit=100

# Watchdogs
scheduler.queued-watchdog.timeout-ms=300000
scheduler.queued-watchdog.poll-delay-ms=60000
scheduler.running-watchdog.timeout-ms=600000
scheduler.running-watchdog.poll-delay-ms=60000

# Redis health and Kafka pause
scheduler.redis-health.poll-delay-ms=5000
scheduler.kafka.redis-retry-backoff-ms=5000

# Dead-letter scheduler
scheduler.dead-letter.poll-delay-ms=30000
scheduler.dead-letter.dispatch-retry-delay-ms=30000

# Webhook timeouts
scheduler.webhook.connect-timeout-ms=5000
scheduler.webhook.read-timeout-ms=10000

# Mail timeouts
spring.mail.properties.mail.smtp.connectiontimeout=5000
spring.mail.properties.mail.smtp.timeout=10000
spring.mail.properties.mail.smtp.writetimeout=10000
```

Due-job dispatch uses atomic PostgreSQL row claiming with `FOR UPDATE SKIP LOCKED`. Multiple scheduler instances can run with `scheduler.enabled=true`; each poll claims a bounded batch of due `PENDING` jobs and moves their `nextRunAt` forward before publishing to Kafka. The database row lock is held only for the claim transaction, not while Kafka publishing is in progress. If publish succeeds, the job becomes `QUEUED`; if the scheduler crashes or Kafka publish fails, the job becomes due again after `scheduler.due-job.dispatch-retry-delay-ms`.

---

## Package Layout

```text
config/       HTTP client, Kafka topics, error handling
constants/    Kafka topic name constants
consumers/    Kafka listeners
controller/   REST API
dto/          Request/event/response DTOs, typed payload records
entity/       JPA entities
enums/        JobStatus, JobType, JobPriority, DeadLetterStatus
exception/    Domain exceptions
handlers/     Per-job-type handlers and JobHandlerRouter
monitoring/   Redis-aware Kafka listener pause/resume
producers/    Kafka producer wrapper
repository/   Spring Data JPA repositories
scheduler/    Due-job dispatcher, watchdogs, DLQ publisher
service/      Job lifecycle, worker, Redis lock, execution logs, Redis health
utility/      Key builders for locks and done markers
```

---

## Status

### Done

- Job submission, listing, detail, and execution log APIs
- Dead job listing, DLQ inspection, manual requeue, and cancellation APIs
- Typed payload validation for email, webhook, and cleanup
- PostgreSQL entities with indexes for scheduler and status queries
- DB-backed due-job dispatch
- Kafka producer and consumer flow
- High-priority and normal queue routing
- Durable dead-letter publishing with PostgreSQL-backed retry state
- DLQ inspection views with attempt counts, final errors, and publish retry metadata
- Redis lock with Lua ownership-safe release and renewal
- Redis idempotency marker for completed one-time jobs
- Redis health checks and Kafka listener pause/resume
- Per-attempt execution logs
- Exponential backoff via `nextRunAt`
- Cron scheduling with Spring `CronExpression`
- `QUEUED` and `RUNNING` watchdogs
- Single-scheduler-instance flag
- `SEND_EMAIL`, `WEBHOOK`, and `CLEANUP` handlers
- Docker Compose for local PostgreSQL, Kafka, and Redis
- Pagination and filtering for the job list API
- Cancellation support for pending and queued jobs

### Up Next

- Runtime configuration profiles for Kafka, Redis, PostgreSQL, and Mail
- Unit and integration tests for worker lifecycle, retry, cron, watchdogs, DLQ, and handler routing
- Testcontainers for integration tests

### Feature Roadmap

- `REPORT` and `SCRAPE` handler implementations
- SSE live updates for job status
- Metrics: success rate, average duration, jobs per status/type
- Structured API error responses
- GitHub Actions CI
- React dashboard

### Production Hardening

- Leader election or atomic DB row claiming to replace the single-scheduler flag
- Stronger concurrency protection around due-job claiming
- Authentication and authorization for job management APIs

---

## System Design Talking Points

- PostgreSQL is the source of truth. The API writes jobs to the DB before any Kafka interaction, so jobs survive Kafka outages at submission time.
- Kafka provides async fan-out. Kafka is the delivery layer; PostgreSQL holds the canonical state.
- `nextRunAt` unifies scheduling. Immediate jobs, retries, and cron jobs all use the same scheduling column.
- `QUEUED` is a distinct state. It separates "Kafka accepted this" from "a worker started this."
- Two watchdogs recover two failure windows. The `QUEUED` watchdog handles jobs that were dispatched but never picked up. The `RUNNING` watchdog handles worker crashes mid-execution.
- Redis locks reduce duplicate execution under Kafka's at-least-once delivery model.
- Lua scripts make lock operations ownership-safe.
- Redis health checks pause Kafka consumption when locks and idempotency checks cannot be trusted.
- Dead-letter state is durable. DLQ publish failures are retried from PostgreSQL state, not memory.

---

## Local Verification

```bash
# Start PostgreSQL, Kafka, and Redis
docker compose up -d

# Compile only. No local infrastructure required.
mvn -DskipTests compile
```

The test suite now uses Testcontainers for integration coverage. Make sure Docker is available, and if Docker Desktop on Windows needs the TCP endpoint for Testcontainers, set:

```powershell
setx DOCKER_HOST "tcp://localhost:2375"
```

Then reopen the terminal and run:

```bash
mvn test
```

To generate coverage and send analysis to SonarQube or SonarCloud:

```bash
mvn clean verify sonar:sonar \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.token=your_sonar_token
```

JaCoCo writes the coverage report to `target/site/jacoco/jacoco.xml`, and Surefire writes test results to `target/surefire-reports`.
