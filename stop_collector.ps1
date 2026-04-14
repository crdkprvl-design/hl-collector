Param()

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}

$pidFile = Join-Path $root "data\collector.pid"
$lockFile = Join-Path $root "data\signal_events_all_pairs.jsonl.lock"

function Stop-ByPid {
    param(
        [int]$TargetPid,
        [string]$Reason
    )

    if ($TargetPid -le 0) {
        return $false
    }

    $proc = Get-Process -Id $TargetPid -ErrorAction SilentlyContinue
    if (-not $proc) {
        return $false
    }

    Stop-Process -Id $TargetPid -Force -ErrorAction SilentlyContinue
    Write-Output "Collector stopped. PID=$TargetPid ($Reason)"
    return $true
}

$stoppedAny = $false

if (Test-Path $pidFile) {
    $pidRaw = Get-Content -Path $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    $collectorPid = 0
    if ([int]::TryParse($pidRaw, [ref]$collectorPid)) {
        if (Stop-ByPid -TargetPid $collectorPid -Reason "pid-file") {
            $stoppedAny = $true
        }
    }
}

if (Test-Path $lockFile) {
    try {
        $lock = Get-Content -Path $lockFile -Raw -Encoding UTF8 | ConvertFrom-Json
        $lockPid = [int]$lock.pid
        if (Stop-ByPid -TargetPid $lockPid -Reason "lock-file") {
            $stoppedAny = $true
        }
    }
    catch {
        # ignore malformed lock file
    }
}

Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $lockFile -Force -ErrorAction SilentlyContinue

if (Test-Path $pidFile) {
    try {
        Set-Content -Path $pidFile -Value "0" -NoNewline -Encoding UTF8
    }
    catch {
        # best effort
    }
}

if (Test-Path $lockFile) {
    try {
        Set-Content -Path $lockFile -Value '{"pid":0,"collector_instance_id":"","collector_started_at_ts":0}' -NoNewline -Encoding UTF8
    }
    catch {
        # best effort
    }
}

if (-not $stoppedAny) {
    Write-Output "Collector process not running."
}
