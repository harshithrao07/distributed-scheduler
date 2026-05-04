# Benchmarks

Two scripts that produce the headline numbers worth putting on a resume.

## Prerequisites

- Docker Desktop running
- Main stack already up: `docker compose up --build -d`
- That's it — k6 and the echo target run as containers, no host installs.

## 1. Throughput + end-to-end latency

```powershell
.\bench\run-bench.ps1 -Rps 100 -Duration 60s
```

What it measures (single summary block at the end):

| Number | Meaning |
|---|---|
| `submitted` / `actualRps` | Jobs the API actually accepted vs. the rate it sustained |
| `e2eP50/P95/P99 ms` | Wall-clock latency from `created_at` to `completed_at` for SUCCESS jobs (DB-derived) |
| `handlerP95 ms` | Worker handler execution p95 from `scheduler_job_execution_seconds` (Prometheus) |
| `success / total` | Sanity check that nothing was dropped |

Tips:
- Start with `-Rps 50 -Duration 30s` to confirm the pipeline runs cleanly.
- Push `-Rps` upward in increments (50 → 100 → 200 → 500). The point at which `e2eP95` blows up or `stillPending` stays > 0 after the drain budget is the saturation point.
- `host.docker.internal` is used so the in-container app can reach the host-mapped echo port; works on Docker Desktop (Windows/Mac).

## 2. Kafka outage survival

```powershell
.\bench\recovery-test.ps1 -JobCount 200 -OutageSeconds 60
```

What it measures:

| Number | Meaning |
|---|---|
| `accepted` while Kafka down | Demonstrates DB-backed dispatch — the API stays up even when the broker is down |
| `outageDurationSec` | How long Kafka was actually stopped |
| `recoveryDurationSec` | Seconds from `docker start scheduler-kafka` to all jobs reaching SUCCESS |

This is the metric backing the architectural claim "PostgreSQL is source of truth; jobs survive Kafka outages."

## Resume-line examples

Numbers below are from this repo's actual benchmark runs (single-node Docker Desktop stack, 12-partition topics, listener `concurrency=12`, 30-connection HikariCP pool):

> Built and load-tested a distributed job scheduler (Spring Boot, PostgreSQL, Kafka, Redis) — single-instance sustains **300 jobs/sec** end-to-end with **API submission p95 5.9 ms**, **e2e p95 708 ms / p99 929 ms**, **100% completion across 18,000 submissions**. **Horizontally scaled to 3 replicas behind nginx LB**: same 300 rps drops to **e2e p95 169 ms / p99 206 ms** (4.5× tail-latency improvement) at 100% completion across 18,001 submissions; identified **PostgreSQL CPU as the architectural ceiling** (~250% CPU during the run, app tier at ~100% each) via live `docker stats` — the correct bottleneck to have, since PG is source of truth.

> Diagnosed and fixed a write-after-write race in the Kafka dispatcher that leaked ~1.4% of jobs to a stuck-`QUEUED` state — surfaced only after raising listener concurrency from 1 → 12 — by removing a redundant idempotent producer-success callback that was overwriting the worker's `SUCCESS` update.

> Reduced per-job DB round-trips by collapsing the worker hot-path's start (`status→RUNNING` + create execution log + log→`RUNNING`) and finish (`log→SUCCESS` + `status→SUCCESS`) sequences from **5 transactions → 2 transactions**, dropping p99 e2e latency by 17% (1.59 s → 1.32 s).

> Implemented PostgreSQL-backed dispatch so job submission is decoupled from Kafka availability — **accepted 200/200 jobs during a 60-second Kafka outage with 0 API rejections**, draining 199/200 within ~9 seconds of the broker returning; the 1 remaining job stayed `QUEUED` after the abrupt restart and was recoverable by the `QUEUED` watchdog within its timeout window, demonstrating the layered defense (DB-backed dispatch + Kafka redelivery + watchdog) that keeps the system lossless without manual intervention.

> Achieved **75% test coverage** (auto-regenerated in CI) across a Testcontainers integration suite covering PostgreSQL, Kafka, Redis, concurrency, and failure paths.

## Output artifacts

Each run writes a JSON summary to `bench/results/<runId>.json`. Add `bench/results/` to `.gitignore` if you don't want those committed.
