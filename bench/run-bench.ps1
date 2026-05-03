# Throughput + end-to-end latency benchmark for the distributed scheduler.
#
# Usage (from repo root):
#   .\bench\run-bench.ps1                          # defaults: 50 rps for 60s
#   .\bench\run-bench.ps1 -Rps 100 -Duration 120s
#   .\bench\run-bench.ps1 -Rps 200 -Duration 60s -DrainTimeoutSec 180
#
# Prerequisites:
#   - docker desktop running
#   - main stack already up (docker compose up --build -d)
#
# What it does:
#   1. Brings up bench/docker-compose.bench.yml so the WEBHOOK target is local.
#   2. Tags this run with a unique RUN_ID so its jobs are isolated in the DB.
#   3. Runs k6 in a docker container against the API.
#   4. Polls the jobs table until all run jobs reach SUCCESS / DEAD or the drain timeout fires.
#   5. Prints a single summary block: target rps, actual rps, submission p95,
#      end-to-end p50/p95/p99, success rate, handler p95 from Prometheus.

[CmdletBinding()]
param(
    [int]    $Rps             = 50,
    [string] $Duration        = '60s',
    [int]    $DrainTimeoutSec = 120,
    # k6 and the prometheus query both run inside containers, so the API URL
    # must be reachable from inside a container - use host.docker.internal,
    # not localhost (which would resolve to the container itself).
    [string] $ApiBase         = 'http://host.docker.internal:8080',
    [string] $PrometheusBase  = 'http://localhost:9090'
)

$ErrorActionPreference = 'Continue'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

# Sanity check: confirm the API is up from the host before burning a load run.
try {
    $hostApi = $ApiBase -replace 'host\.docker\.internal', 'localhost'
    Invoke-RestMethod -Uri "$hostApi/actuator/health" -TimeoutSec 5 | Out-Null
} catch {
    Write-Error "scheduler API not reachable at $hostApi/actuator/health - is the stack up? ($($_.Exception.Message))"
    exit 1
}

$runId = "bench-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
$resultsDir = Join-Path $repoRoot 'bench\results'
New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Host ""
Write-Host "=== distributed-scheduler benchmark ===" -ForegroundColor Cyan
Write-Host "  run id:       $runId"
Write-Host "  target rps:   $Rps"
Write-Host "  duration:     $Duration"
Write-Host "  drain budget: ${DrainTimeoutSec}s"
Write-Host ""

# 1. Bring up the local echo target so webhook calls don't leave the host.
Write-Host "[1/5] starting echo target (bench/docker-compose.bench.yml)..." -ForegroundColor Yellow
docker compose -f bench/docker-compose.bench.yml up -d | Out-Null

# 2. Run k6 against the API. Use host.docker.internal so the job (running inside
#    scheduler-app) can reach the echo container's host-mapped port.
Write-Host "[2/5] running k6 load (target $Rps rps for $Duration)..." -ForegroundColor Yellow
$loadStart = Get-Date
docker run --rm -i `
    --add-host=host.docker.internal:host-gateway `
    -e API_BASE=$ApiBase `
    -e WEBHOOK_URL=http://host.docker.internal:8081/ `
    -e RPS=$Rps `
    -e DURATION=$Duration `
    -e RUN_ID=$runId `
    -v "${repoRoot}/bench:/scripts" `
    grafana/k6 run /scripts/load-test.js
if ($LASTEXITCODE -ne 0) {
    Write-Warning "k6 reported a non-zero exit. Inspect the output above; latency below still reflects whatever was submitted."
}
$loadEnd = Get-Date

# 3. Wait for jobs from this run to drain.
Write-Host ""
Write-Host "[3/5] draining: waiting for run jobs to reach a terminal state..." -ForegroundColor Yellow
$deadline = (Get-Date).AddSeconds($DrainTimeoutSec)
$pending = -1
while ((Get-Date) -lt $deadline) {
    $pending = (docker exec scheduler-postgres psql -U postgres -d jobscheduler -tAc `
        "SELECT COUNT(*) FROM jobs WHERE idempotency_key LIKE '$runId-%' AND job_status NOT IN ('SUCCESS','DEAD','CANCELED')").Trim()
    if ($pending -eq '0') { break }
    Write-Host "    still pending: $pending" -ForegroundColor DarkGray
    Start-Sleep -Seconds 3
}
if ($pending -ne '0') {
    Write-Warning "drain timeout hit; $pending jobs from this run never reached terminal state. Latency stats reflect only completed jobs."
}

# 4. End-to-end latency from Postgres.
Write-Host ""
Write-Host "[4/5] computing end-to-end latency from PostgreSQL..." -ForegroundColor Yellow
$sqlPath = Join-Path $repoRoot 'bench\measure-e2e.sql'
$e2e = Get-Content -Raw $sqlPath | docker exec -i scheduler-postgres `
    psql -U postgres -d jobscheduler -v "run_id=$runId" -A -F '|' -t
$e2eParts = ($e2e | Where-Object { $_ -and $_ -notmatch '^\s*$' } | Select-Object -First 1) -split '\|'

# 5. Worker handler p95 from Prometheus.
Write-Host "[5/5] querying Prometheus for handler duration p95..." -ForegroundColor Yellow
$promQuery = 'histogram_quantile(0.95, sum(rate(scheduler_job_execution_seconds_bucket[5m])) by (le))'
$handlerP95Sec = $null
try {
    $promResp = Invoke-RestMethod -Uri "$PrometheusBase/api/v1/query" -Body @{ query = $promQuery } -Method Get
    if ($promResp.status -eq 'success' -and $promResp.data.result.Count -gt 0) {
        $handlerP95Sec = [double]$promResp.data.result[0].value[1]
    }
} catch {
    Write-Warning "prometheus query failed: $($_.Exception.Message)"
}
$handlerP95Ms = if ($null -ne $handlerP95Sec) { [math]::Round($handlerP95Sec * 1000, 1) } else { $null }

# Read k6 summary if it landed in the mounted volume.
$k6SummaryPath = Join-Path $resultsDir 'k6-summary.json'
$k6 = if (Test-Path $k6SummaryPath) { Get-Content $k6SummaryPath -Raw | ConvertFrom-Json } else { $null }

# Compose summary.
$summary = [ordered]@{
    runId           = $runId
    targetRps       = $Rps
    duration        = $Duration
    submitted       = if ($k6) { $k6.submissionsAttempted } else { $null }
    actualRps       = if ($k6) { [math]::Round([double]$k6.submissionsActualRps, 2) } else { $null }
    submitFailRate  = if ($k6) { $k6.submissionFailureRate } else { $null }
    submitP95Ms     = if ($k6) { [math]::Round([double]$k6.submissionLatencyMs.p95, 1) } else { $null }
    success         = $e2eParts[0]
    dead            = $e2eParts[1]
    stillPending    = $e2eParts[2]
    totalRunJobs    = $e2eParts[3]
    e2eMinMs        = $e2eParts[4]
    e2eP50Ms        = $e2eParts[5]
    e2eP95Ms        = $e2eParts[6]
    e2eP99Ms        = $e2eParts[7]
    e2eMaxMs        = $e2eParts[8]
    handlerP95Ms    = $handlerP95Ms
    loadStartedAt   = $loadStart.ToString('o')
    loadFinishedAt  = $loadEnd.ToString('o')
}

$summaryJson = $summary | ConvertTo-Json -Depth 4
$summaryJson | Out-File -FilePath (Join-Path $resultsDir "$runId.json") -Encoding utf8

Write-Host ""
Write-Host "=== headline numbers ===" -ForegroundColor Green
Write-Host ("  throughput:       {0} jobs submitted at {1} jobs/sec actual" -f $summary.submitted, $summary.actualRps)
Write-Host ("  e2e latency:      p50={0}ms  p95={1}ms  p99={2}ms" -f $summary.e2eP50Ms, $summary.e2eP95Ms, $summary.e2eP99Ms)
Write-Host ("  handler p95:      {0}ms (from Prometheus)" -f $summary.handlerP95Ms)
Write-Host ("  success / total:  {0} / {1}  (dead={2}, pending={3})" -f $summary.success, $summary.totalRunJobs, $summary.dead, $summary.stillPending)
Write-Host ""
Write-Host "  full summary:     bench\results\$runId.json" -ForegroundColor DarkGray
Write-Host ""
