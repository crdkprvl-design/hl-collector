Param()

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}

$pidFile = Join-Path $root "data\collector.pid"

if (-not (Test-Path $pidFile)) {
    Write-Output "Collector PID file not found."
    exit 0
}

$pidRaw = Get-Content -Path $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
$collectorPid = 0
if (-not [int]::TryParse($pidRaw, [ref]$collectorPid)) {
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
    Write-Output "Invalid PID file removed."
    exit 0
}

$proc = Get-Process -Id $collectorPid -ErrorAction SilentlyContinue
if ($proc) {
    Stop-Process -Id $collectorPid -Force
    Write-Output "Collector stopped. PID=$collectorPid"
} else {
    Write-Output "Collector process not running (PID=$collectorPid)."
}

Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
