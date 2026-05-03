# Kafka outage survival test.
#
# Scenario:
#   1. Stop Kafka while the app is healthy.
#   2. Submit N webhook jobs through the REST API. The API still returns 200 because
#      submissions write straight to PostgreSQL - Kafka is only the dispatch layer.
#   3. Restart Kafka.
#   4. Measure how long it takes for all submitted jobs to reach SUCCESS.
#
# This is the metric that backs the resume claim "jobs survive Kafka outages and
# resume automatically once the broker returns."
#
# Usage:
#   .\bench\recovery-test.ps1                # defaults: 50 jobs, 30s outage
#   .\bench\recovery-test.ps1 -JobCount 200 -OutageSeconds 60

[CmdletBinding()]
param(
    [int] $JobCount       = 50,
    [int] $OutageSeconds  = 30,
    [int] $RecoveryBudget = 180,
    [string] $ApiBase     = 'http://localhost:8080'
)

$ErrorActionPreference = 'Continue'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$runId = "recov-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"

Write-Host ""
Write-Host "=== kafka outage survival test ===" -ForegroundColor Cyan
Write-Host "  run id:           $runId"
Write-Host "  jobs to submit:   $JobCount"
Write-Host "  outage duration:  ${OutageSeconds}s"
Write-Host "  recovery budget:  ${RecoveryBudget}s"
Write-Host ""

# Make sure echo target exists for the WEBHOOK calls when Kafka returns.
docker compose -f bench/docker-compose.bench.yml up -d | Out-Null

# 1. Stop Kafka.
Write-Host "[1/5] stopping Kafka container..." -ForegroundColor Yellow
$kafkaStoppedAt = Get-Date
docker stop scheduler-kafka | Out-Null

# 2. Submit jobs while Kafka is down. API should accept all of them.
Write-Host "[2/5] submitting $JobCount jobs while Kafka is down..." -ForegroundColor Yellow
$accepted = 0
$rejected = 0
for ($i = 0; $i -lt $JobCount; $i++) {
    $body = @{
        jobType        = 'WEBHOOK'
        jobPriority    = 'MEDIUM'
        payload        = @{
            url  = 'http://host.docker.internal:8081/'
            body = @{ runId = $runId; idx = $i }
        }
        maxAttempts    = 3
        idempotencyKey = "$runId-$i"
    } | ConvertTo-Json -Depth 6 -Compress

    try {
        Invoke-RestMethod -Uri "$ApiBase/app/v1/jobs" -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 5 | Out-Null
        $accepted++
    } catch {
        $rejected++
    }
}
Write-Host ("    accepted: {0}, rejected: {1}" -f $accepted, $rejected)

# 3. Hold the outage so the scheduler clearly observes Kafka is down.
$remaining = $OutageSeconds - ((Get-Date) - $kafkaStoppedAt).TotalSeconds
if ($remaining -gt 0) {
    Write-Host "[3/5] holding outage for $([math]::Round($remaining,1))s more..." -ForegroundColor Yellow
    Start-Sleep -Seconds ([int]$remaining)
} else {
    Write-Host "[3/5] submission already exceeded the outage window - restarting Kafka now." -ForegroundColor Yellow
}

# 4. Bring Kafka back.
Write-Host "[4/5] restarting Kafka..." -ForegroundColor Yellow
$kafkaRestartedAt = Get-Date
docker start scheduler-kafka | Out-Null

# 5. Wait for the run's jobs to drain.
Write-Host "[5/5] measuring recovery: waiting for all $accepted jobs to reach SUCCESS..." -ForegroundColor Yellow
$deadline = $kafkaRestartedAt.AddSeconds($RecoveryBudget)
$success = '0'
$dead = '0'
$pending = '0'
$firstAllDoneAt = $null
while ((Get-Date) -lt $deadline) {
    $row = docker exec scheduler-postgres psql -U postgres -d jobscheduler -tAc `
        "SELECT COUNT(*) FILTER (WHERE job_status='SUCCESS') || '|' || COUNT(*) FILTER (WHERE job_status='DEAD') || '|' || COUNT(*) FILTER (WHERE job_status NOT IN ('SUCCESS','DEAD','CANCELED')) FROM jobs WHERE idempotency_key LIKE '$runId-%'"
    $parts = $row.Trim() -split '\|'
    $success = $parts[0]; $dead = $parts[1]; $pending = $parts[2]
    Write-Host ("    success={0}  dead={1}  pending={2}" -f $success, $dead, $pending) -ForegroundColor DarkGray

    if ([int]$success -eq $accepted) {
        $firstAllDoneAt = Get-Date
        break
    }
    Start-Sleep -Seconds 2
}

$recoverySec = if ($firstAllDoneAt) { [math]::Round(($firstAllDoneAt - $kafkaRestartedAt).TotalSeconds, 1) } else { $null }
$totalOutageSec = [math]::Round(($kafkaRestartedAt - $kafkaStoppedAt).TotalSeconds, 1)

Write-Host ""
Write-Host "=== recovery summary ===" -ForegroundColor Green
Write-Host ("  jobs submitted while Kafka was down:    {0} accepted, {1} rejected" -f $accepted, $rejected)
Write-Host ("  Kafka outage actual duration:           ${totalOutageSec}s")
if ($recoverySec) {
    Write-Host ("  time to drain all $accepted jobs after Kafka came back: ${recoverySec}s") -ForegroundColor Green
} else {
    Write-Warning ("  recovery did NOT complete within ${RecoveryBudget}s - final state: success=$success, dead=$dead, pending=$pending")
}
Write-Host ""

$resultsDir = Join-Path $repoRoot 'bench\results'
New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null
[ordered]@{
    runId               = $runId
    jobCountRequested   = $JobCount
    accepted            = $accepted
    rejected            = $rejected
    outageDurationSec   = $totalOutageSec
    recoveryDurationSec = $recoverySec
    finalSuccess        = [int]$success
    finalDead           = [int]$dead
    finalPending        = [int]$pending
} | ConvertTo-Json | Out-File (Join-Path $resultsDir "$runId.json") -Encoding utf8

Write-Host "  full summary: bench\results\$runId.json" -ForegroundColor DarkGray
Write-Host ""
