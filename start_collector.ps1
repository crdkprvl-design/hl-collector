Param()

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}

$dataDir = Join-Path $root "data"
$logFile = Join-Path $dataDir "collector_stdout.log"
$errFile = Join-Path $dataDir "collector_stderr.log"
$pidFile = Join-Path $dataDir "collector.pid"
$jsonlFile = Join-Path $dataDir "signal_events_all_pairs.jsonl"

New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

if (Test-Path $pidFile) {
    $existingPidRaw = (Get-Content -Path $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($existingPidRaw) {
        $existingPid = 0
        if ([int]::TryParse($existingPidRaw, [ref]$existingPid)) {
            $proc = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Output "Collector already running. PID=$existingPid"
                Write-Output "Log: $jsonlFile"
                exit 0
            }
        }
    }
}

$args = @(
    "-3", "-u", "density_screener.py",
    "--min-wall-ratio", "3",
    "--min-wall-usd", "10000",
    "--min-day-volume-usd", "0",
    "--approach-ticks", "4",
    "--breakout-ticks", "2",
    "--bounce-pct", "0.5",
    "--scan-interval-sec", "15",
    "--mids-poll-sec", "1",
    "--metadata-refresh-sec", "300",
    "--log-path", "data/signal_events_all_pairs.jsonl"
)

$proc = Start-Process `
    -FilePath "py" `
    -ArgumentList $args `
    -WorkingDirectory $root `
    -RedirectStandardOutput $logFile `
    -RedirectStandardError $errFile `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $pidFile -Value $proc.Id -NoNewline

Write-Output "Collector started. PID=$($proc.Id)"
Write-Output "Events: $jsonlFile"
Write-Output "Stdout: $logFile"
Write-Output "Stderr: $errFile"
