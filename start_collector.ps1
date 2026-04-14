Param()

$ErrorActionPreference = "Stop"

function Convert-ToPsLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Test-PythonCandidate {
    param(
        [string]$FilePath,
        [string[]]$PrefixArgs = @()
    )

    if ($FilePath -ne "py" -and $FilePath -ne "python" -and -not (Test-Path -LiteralPath $FilePath)) {
        return $false
    }

    try {
        $null = & $FilePath @PrefixArgs -c "import requests, sys; print(sys.executable)" 2>$null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Resolve-PythonLaunch {
    param([string]$RootDir)

    $candidates = @(
        @{
            FilePath = (Join-Path $RootDir "venv\Scripts\python.exe")
            PrefixArgs = @()
            DisplayName = "venv\Scripts\python.exe"
        },
        @{
            FilePath = (Join-Path $RootDir ".venv\Scripts\python.exe")
            PrefixArgs = @()
            DisplayName = ".venv\Scripts\python.exe"
        },
        @{
            FilePath = "py"
            PrefixArgs = @("-3")
            DisplayName = "py -3"
        },
        @{
            FilePath = "python"
            PrefixArgs = @()
            DisplayName = "python"
        }
    )

    foreach ($candidate in $candidates) {
        if (Test-PythonCandidate -FilePath $candidate.FilePath -PrefixArgs $candidate.PrefixArgs) {
            return $candidate
        }
    }

    return $null
}

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
        if ([int]::TryParse($existingPidRaw, [ref]$existingPid) -and $existingPid -gt 0) {
            $proc = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Output "Collector already running. PID=$existingPid"
                Write-Output "Log: $jsonlFile"
                exit 0
            }
        }
    }

    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
}

$python = Resolve-PythonLaunch -RootDir $root
if (-not $python) {
    Write-Error "No working Python interpreter with requests was found. Checked local venv, .venv, py -3, and python."
}

$collectorArgs = @(
    "-u", "density_screener.py",
    "--min-wall-ratio", "3",
    "--min-wall-usd", "0",
    "--min-day-volume-usd", "0",
    "--max-wall-distance-pct", "3",
    "--approach-ticks", "4",
    "--breakout-ticks", "2",
    "--bounce-pct", "0.5",
    "--min-persistence-sec", "20",
    "--max-signal-age-sec", "900",
    "--scan-interval-sec", "8",
    "--mids-poll-sec", "1",
    "--metadata-refresh-sec", "600",
    "--book-source", "auto",
    "--ws-max-books", "0",
    "--log-path", "data/signal_events_all_pairs.jsonl"
)

$pythonParts = @($python.FilePath) + $python.PrefixArgs
$pythonCmd = ($pythonParts | ForEach-Object { Convert-ToPsLiteral $_ }) -join " "
$collectorArgLiterals = ($collectorArgs | ForEach-Object { Convert-ToPsLiteral $_ }) -join " "
$collectorCmd = @(
    "Remove-Item Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:ALL_PROXY,Env:http_proxy,Env:https_proxy,Env:all_proxy -ErrorAction SilentlyContinue"
    "Set-Location -LiteralPath $(Convert-ToPsLiteral $root)"
    "& $pythonCmd $collectorArgLiterals 1>> $(Convert-ToPsLiteral $logFile) 2>> $(Convert-ToPsLiteral $errFile)"
) -join "; "

$wrapper = Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList @("-NoProfile", "-WindowStyle", "Hidden", "-Command", $collectorCmd) `
    -PassThru

# Prefer real collector PID from lock file; fallback to wrapper PID.
$resolvedPid = $wrapper.Id
$lockFile = Join-Path $dataDir "signal_events_all_pairs.jsonl.lock"
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 250
    if (-not (Test-Path -LiteralPath $lockFile)) {
        continue
    }
    try {
        $lock = Get-Content -Path $lockFile -Raw -Encoding UTF8 | ConvertFrom-Json
        $lockPid = [int]$lock.pid
        if ($lockPid -gt 0 -and (Get-Process -Id $lockPid -ErrorAction SilentlyContinue)) {
            $resolvedPid = $lockPid
            break
        }
    }
    catch {
        continue
    }
}

Set-Content -Path $pidFile -Value $resolvedPid -NoNewline -Encoding UTF8

Write-Output "Collector started. PID=$resolvedPid"
Write-Output "Python: $($python.DisplayName)"
Write-Output "Events: $jsonlFile"
Write-Output "Stdout: $logFile"
Write-Output "Stderr: $errFile"
