Param(
    [switch]$SkipGitPull,
    [int]$CollectorFreshSec = 180
)

$ErrorActionPreference = "Stop"

function Write-StatusFile {
    param(
        [string]$Path,
        [hashtable]$Status
    )
    $parent = Split-Path -Parent $Path
    if ($parent) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    $json = $Status | ConvertTo-Json -Depth 8
    Set-Content -LiteralPath $Path -Value $json -Encoding UTF8
}

function Get-LastEventTs {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return 0.0
    }
    try {
        $lastLine = Get-Content -LiteralPath $Path -Tail 1 -ErrorAction Stop
        if (-not $lastLine) {
            return 0.0
        }
        $obj = $lastLine | ConvertFrom-Json
        return [double]$obj.ts
    }
    catch {
        return 0.0
    }
}

function Get-LockCollectorPid {
    param([string]$RootDir)
    $lockFile = Join-Path $RootDir "data\signal_events_all_pairs.jsonl.lock"
    if (-not (Test-Path -LiteralPath $lockFile)) {
        return 0
    }
    try {
        $lock = Get-Content -LiteralPath $lockFile -Raw -Encoding UTF8 | ConvertFrom-Json
        $lockPid = 0
        if ([int]::TryParse([string]$lock.pid, [ref]$lockPid) -and $lockPid -gt 0) {
            return $lockPid
        }
    }
    catch {
        return 0
    }
    return 0
}

function Get-CollectorState {
    param(
        [string]$PidFile,
        [string]$EventsFile,
        [int]$FreshSec
    )

    $pidAlive = $false
    $collectorPid = 0
    if (Test-Path -LiteralPath $PidFile) {
        $pidRaw = Get-Content -LiteralPath $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1
        if ([int]::TryParse($pidRaw, [ref]$collectorPid) -and $collectorPid -gt 0) {
            $proc = Get-Process -Id $collectorPid -ErrorAction SilentlyContinue
            if ($proc) {
                $pidAlive = $true
            }
        }
    }

    $lastTs = Get-LastEventTs -Path $EventsFile
    $nowTs = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $isFresh = $false
    if ($lastTs -gt 0) {
        $isFresh = (($nowTs - $lastTs) -le $FreshSec)
    }

    $lockPid = Get-LockCollectorPid -RootDir (Split-Path -Parent $PidFile)
    $lockPidAlive = $false
    if ($lockPid -gt 0) {
        $lockProc = Get-Process -Id $lockPid -ErrorAction SilentlyContinue
        if ($lockProc) {
            $lockPidAlive = $true
        }
    }

    $stateLabel = "stale"
    if ($pidAlive) {
        $stateLabel = "alive_pid"
    }
    elseif ($lockPidAlive) {
        $stateLabel = "alive_lock"
    }
    elseif ($isFresh) {
        $stateLabel = "fresh_log_only"
    }

    return @{
        pid_alive = $pidAlive
        pid = $collectorPid
        lock_pid = $lockPid
        lock_pid_alive = $lockPidAlive
        last_ts = $lastTs
        fresh = $isFresh
        state = $stateLabel
    }
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}
Set-Location $root

$eventsFile = Join-Path $root "data\signal_events_all_pairs.jsonl"
$pidFile = Join-Path $root "data\collector.pid"
$statusFile = Join-Path $root "data\reports\startup_sync_status.json"

$startupStatus = @{
    startup_triggered = $true
    timestamp = (Get-Date).ToString("s")
    root = $root
    skip_git_pull = [bool]$SkipGitPull
    git_pull_status = "pending"
    git_pull_output = ""
    collector_check_status = "pending"
    collector_restart_status = "not_needed"
    summary_status = "pending"
    readiness_status = "pending"
    collector_status = "unknown"
    sync_status = "unknown"
    error_message = ""
}

Write-StatusFile -Path $statusFile -Status $startupStatus

$syncStatus = "failure"
if ($SkipGitPull) {
    $syncStatus = "skipped"
    $startupStatus.git_pull_status = "skipped"
    $startupStatus.sync_status = $syncStatus
    Write-StatusFile -Path $statusFile -Status $startupStatus
}
else {
    try {
        $gitOutputLines = & git pull --ff-only 2>&1
        $gitExitCode = $LASTEXITCODE
        $gitOutput = ($gitOutputLines | Out-String)
        if ($gitExitCode -eq 0) {
            $syncStatus = "success"
            $startupStatus.git_pull_status = "success"
            $startupStatus.git_pull_output = $gitOutput.Trim()
        }
        else {
            $startupStatus.git_pull_status = "failed"
            $startupStatus.git_pull_output = $gitOutput.Trim()
        }
    }
    catch {
        $syncStatus = "failure"
        $startupStatus.git_pull_status = "failed"
        $startupStatus.git_pull_output = $_.Exception.Message
    }
    $startupStatus.sync_status = $syncStatus
    Write-StatusFile -Path $statusFile -Status $startupStatus
}

$collectorState = Get-CollectorState -PidFile $pidFile -EventsFile $eventsFile -FreshSec $CollectorFreshSec
$collectorStatus = "stale"

if ($collectorState.state -eq "alive_pid" -and $collectorState.fresh) {
    $collectorStatus = "alive"
    $startupStatus.collector_check_status = "alive"
    $startupStatus.collector_restart_status = "not_needed"
}
elseif ($collectorState.state -eq "alive_lock") {
    if ($collectorState.lock_pid -gt 0) {
        Set-Content -Path $pidFile -Value $collectorState.lock_pid -NoNewline -Encoding UTF8
    }
    $collectorStatus = "alive"
    $startupStatus.collector_check_status = "alive_via_lock"
    $startupStatus.collector_restart_status = "pid_healed_from_lock"
}
elseif ($collectorState.state -eq "fresh_log_only") {
    if (Test-Path -LiteralPath $pidFile) {
        Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
    }
    $collectorStatus = "alive"
    $startupStatus.collector_check_status = "fresh_log_pid_stale"
    $startupStatus.collector_restart_status = "not_needed"
}
else {
    $startupStatus.collector_check_status = "stale_or_not_running"
    $startupStatus.collector_restart_status = "attempted"
    Write-StatusFile -Path $statusFile -Status $startupStatus
    if (Test-Path -LiteralPath $pidFile) {
        Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
    }
    try {
        & ".\start_collector.ps1" | Out-Host
        Start-Sleep -Seconds 12
        $collectorState = Get-CollectorState -PidFile $pidFile -EventsFile $eventsFile -FreshSec $CollectorFreshSec
        if ($collectorState.fresh -and ($collectorState.pid_alive -or $collectorState.lock_pid_alive -or $collectorState.state -eq "fresh_log_only")) {
            $collectorStatus = "started"
            $startupStatus.collector_restart_status = "started"
        }
        elseif ($collectorState.pid_alive -or $collectorState.lock_pid_alive) {
            $collectorStatus = "restarted"
            $startupStatus.collector_restart_status = "restarted_but_not_fresh"
        }
        else {
            $collectorStatus = "failed"
            $startupStatus.collector_restart_status = "failed"
        }
    }
    catch {
        $collectorStatus = "failed"
        $startupStatus.collector_restart_status = "failed"
        $startupStatus.error_message = "collector_restart: $($_.Exception.Message)"
    }
}
$startupStatus.collector_status = $collectorStatus
Write-StatusFile -Path $statusFile -Status $startupStatus

$env:PYTHONDONTWRITEBYTECODE = "1"
try {
    & ".\venv\Scripts\python.exe" ".\daily_paper_trade_summary.py" `
      --log-path "data\signal_events_all_pairs.jsonl" `
      --cloud-glob "cloud_data/events_*.jsonl" `
      --profile "trash_ask" `
      --min-score 160 `
      --entry-weight 0.12 `
      --out-json "data\reports\daily_paper_trade_summary_trash_ask.json" `
      --out-md-dir "data\reports" `
      --history-path "data\reports\daily_metrics_history.jsonl" `
      --sync-status $syncStatus `
      --collector-status $collectorStatus
    if ($LASTEXITCODE -eq 0) {
        $startupStatus.summary_status = "success"
    }
    else {
        $startupStatus.summary_status = "failed"
    }
}
catch {
    $startupStatus.summary_status = "failed"
    if (-not $startupStatus.error_message) {
        $startupStatus.error_message = "summary: $($_.Exception.Message)"
    }
}
Write-StatusFile -Path $statusFile -Status $startupStatus

try {
    & ".\venv\Scripts\python.exe" ".\microsize_readiness_report.py" `
      --history-path "data\reports\daily_metrics_history.jsonl" `
      --profile "trash_ask" `
      --mode "B" `
      --out-json "data\reports\microsize_readiness_trash_ask.json" `
      --out-md "data\reports\microsize_readiness_trash_ask.md"
    if ($LASTEXITCODE -eq 0) {
        $startupStatus.readiness_status = "success"
    }
    else {
        $startupStatus.readiness_status = "failed"
    }
}
catch {
    $startupStatus.readiness_status = "failed"
    if (-not $startupStatus.error_message) {
        $startupStatus.error_message = "readiness: $($_.Exception.Message)"
    }
}
$startupStatus.timestamp = (Get-Date).ToString("s")
$startupStatus.sync_status = $syncStatus
$startupStatus.collector_status = $collectorStatus
Write-StatusFile -Path $statusFile -Status $startupStatus

Write-Output "operational_status: sync=$syncStatus collector=$collectorStatus last_ts=$($collectorState.last_ts)"
