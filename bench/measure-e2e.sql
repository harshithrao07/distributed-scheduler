-- End-to-end latency percentiles for jobs in this run.
-- Pass run_id via:  psql -v run_id='bench-1777827333'
-- Latency = completed_at - created_at, in milliseconds, for SUCCESS jobs.

WITH run AS (
    SELECT
        EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000 AS latency_ms,
        job_status
    FROM jobs
    WHERE idempotency_key LIKE :'run_id' || '-%'
)
SELECT
    COUNT(*) FILTER (WHERE job_status = 'SUCCESS')                            AS success_count,
    COUNT(*) FILTER (WHERE job_status = 'DEAD')                               AS dead_count,
    COUNT(*) FILTER (WHERE job_status NOT IN ('SUCCESS','DEAD'))              AS still_pending,
    COUNT(*)                                                                   AS total_jobs,
    ROUND(MIN(latency_ms) FILTER (WHERE job_status = 'SUCCESS')::numeric, 1)  AS e2e_min_ms,
    ROUND((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)
          FILTER (WHERE job_status = 'SUCCESS'))::numeric, 1)                  AS e2e_p50_ms,
    ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)
          FILTER (WHERE job_status = 'SUCCESS'))::numeric, 1)                  AS e2e_p95_ms,
    ROUND((PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)
          FILTER (WHERE job_status = 'SUCCESS'))::numeric, 1)                  AS e2e_p99_ms,
    ROUND(MAX(latency_ms) FILTER (WHERE job_status = 'SUCCESS')::numeric, 1)  AS e2e_max_ms
FROM run;
