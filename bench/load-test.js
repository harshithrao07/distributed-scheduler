// k6 load test: submits WEBHOOK jobs at a fixed rate and reports submission throughput.
// End-to-end latency is measured separately by the harness via PostgreSQL.
//
// Run via:
//   docker run --rm -i --network host -e API_BASE=http://localhost:8080 \
//     -e RPS=100 -e DURATION=60s -e WEBHOOK_URL=http://host.docker.internal:8081/ \
//     grafana/k6 run - < bench/load-test.js
//
// Tunable env vars:
//   API_BASE      base URL of the scheduler API           (default http://localhost:8080)
//   WEBHOOK_URL   URL the scheduled job will POST to      (default http://echo:8080/)
//   RPS           sustained submission rate (jobs/sec)    (default 50)
//   DURATION      sustained-load duration                  (default 60s)
//   RUN_ID        prefix appended to idempotency keys     (default ts-<unix>)

import http from 'k6/http';
import { check } from 'k6';

const API_BASE = __ENV.API_BASE || 'http://localhost:8080';
const WEBHOOK_URL = __ENV.WEBHOOK_URL || 'http://echo:8080/';
const RPS = parseInt(__ENV.RPS || '50', 10);
const DURATION = __ENV.DURATION || '60s';
const RUN_ID = __ENV.RUN_ID || `ts-${Date.now()}`;

export const options = {
    scenarios: {
        submit_jobs: {
            executor: 'constant-arrival-rate',
            rate: RPS,
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs: Math.max(20, RPS),
            maxVUs: Math.max(100, RPS * 2),
        },
    },
    thresholds: {
        // Submission API itself should be very fast — these protect against regressions.
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<200'],
    },
};

export default function () {
    const idem = `${RUN_ID}-${__VU}-${__ITER}`;
    const body = JSON.stringify({
        jobType: 'WEBHOOK',
        jobPriority: 'MEDIUM',
        payload: {
            url: WEBHOOK_URL,
            body: { runId: RUN_ID, vu: __VU, iter: __ITER },
        },
        maxAttempts: 1,
        idempotencyKey: idem,
    });

    const res = http.post(`${API_BASE}/app/v1/jobs`, body, {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'submit_job' },
    });

    check(res, {
        'submission accepted (2xx)': (r) => r.status >= 200 && r.status < 300,
    });
}

export function handleSummary(data) {
    const submit = data.metrics.http_req_duration;
    const fails = data.metrics.http_req_failed;
    const iters = data.metrics.iterations;

    const out = {
        runId: RUN_ID,
        targetRps: RPS,
        duration: DURATION,
        submissionsAttempted: iters?.values?.count ?? null,
        submissionsActualRps: iters?.values?.rate ?? null,
        submissionFailureRate: fails?.values?.rate ?? null,
        submissionLatencyMs: {
            p50: submit?.values?.['p(50)'] ?? null,
            p95: submit?.values?.['p(95)'] ?? null,
            p99: submit?.values?.['p(99)'] ?? null,
            max: submit?.values?.max ?? null,
        },
    };

    // The harness mounts repo's bench/ at /scripts/ inside the container.
    return {
        stdout: '\n=== k6 submission summary ===\n' + JSON.stringify(out, null, 2) + '\n',
        '/scripts/results/k6-summary.json': JSON.stringify(out, null, 2),
    };
}
