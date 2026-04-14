Param(
    [switch]$SkipGitPull,
    [int]$CollectorFreshSec = 180
)

$ErrorActionPreference = "Stop"

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

function Get-CollectorState {
    param(
        [string]$PidFile,
        [string]$EventsFile,
        [int]$FreshSec
    )

    $pidAlive = $false
    if (Test-Path -LiteralPath $PidFile) {
        $pidRaw = Get-Content -LiteralPath $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1
        $collectorPid = 0
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

    return @{
        pid_alive = $pidAlive
        last_ts = $lastTs
        fresh = $isFresh
    }
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}
Set-Location $root

$eventsFile = Join-Path $root "data\signal_events_all_pairs.jsonl"
$pidFile = Join-Path $root "data\collector.pid"

$syncStatus = "failure"
if ($SkipGitPull) {
    $syncStatus = "skipped"
}
else {
    try {
        git pull --ff-only
        if ($LASTEXITCODE -eq 0) {
            $syncStatus = "success"
        }
    }
    catch {
        $syncStatus = "failure"
    }
}

$collectorState = Get-CollectorState -PidFile $pidFile -EventsFile $eventsFile -FreshSec $CollectorFreshSec
$collectorStatus = "stale"

if ($collectorState.pid_alive -and $collectorState.fresh) {
    $collectorStatus = "alive"
}
else {
    & ".\start_collector.ps1" | Out-Host
    Start-Sleep -Seconds 12
    $collectorState = Get-CollectorState -PidFile $pidFile -EventsFile $eventsFile -FreshSec $CollectorFreshSec
    if ($collectorState.pid_alive -and $collectorState.fresh) {
        $collectorStatus = "started"
    }
    elseif ($collectorState.pid_alive) {
        $collectorStatus = "restarted"
    }
    else {
        $collectorStatus = "failed"
    }
}

$env:PYTHONDONTWRITEBYTECODE = "1"
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

& ".\venv\Scripts\python.exe" ".\microsize_readiness_report.py" `
  --history-path "data\reports\daily_metrics_history.jsonl" `
  --profile "trash_ask" `
  --mode "B" `
  --out-json "data\reports\microsize_readiness_trash_ask.json" `
  --out-md "data\reports\microsize_readiness_trash_ask.md"

Write-Output "operational_status: sync=$syncStatus collector=$collectorStatus last_ts=$($collectorState.last_ts)"
