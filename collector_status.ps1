Param()

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}

$pidFile = Join-Path $root "data\collector.pid"
$eventsFile = Join-Path $root "data\signal_events_all_pairs.jsonl"
$stdoutFile = Join-Path $root "data\collector_stdout.log"
$stderrFile = Join-Path $root "data\collector_stderr.log"

if (-not (Test-Path $pidFile)) {
    Write-Output "Collector status: not running (no PID file)."
} else {
    $pidRaw = Get-Content -Path $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    $collectorPid = 0
    if ([int]::TryParse($pidRaw, [ref]$collectorPid)) {
        $proc = Get-Process -Id $collectorPid -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Output "Collector status: running. PID=$collectorPid"
        } else {
            Write-Output "Collector status: stale PID file (PID=$collectorPid not found)."
        }
    } else {
        Write-Output "Collector status: invalid PID file."
    }
}

if (Test-Path $eventsFile) {
    $lineCount = (Get-Content -Path $eventsFile -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
    Write-Output "Events file: $eventsFile"
    Write-Output "Events lines: $lineCount"
} else {
    Write-Output "Events file does not exist yet: $eventsFile"
}

if (Test-Path $stdoutFile) {
    Write-Output "Stdout log: $stdoutFile"
}

if (Test-Path $stderrFile) {
    Write-Output "Stderr log: $stderrFile"
}
