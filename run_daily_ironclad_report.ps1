Param(
    [int]$WindowHours = 24,
    [int]$MinResolvedForRule = 40,
    [string]$OutDir = "data/reports"
)

$ErrorActionPreference = "Stop"

function Test-PythonCandidate {
    param(
        [string]$FilePath,
        [string[]]$PrefixArgs = @()
    )

    if ($FilePath -ne "py" -and $FilePath -ne "python" -and -not (Test-Path -LiteralPath $FilePath)) {
        return $false
    }

    try {
        $null = & $FilePath @PrefixArgs -c "import json,sys; print(sys.version)" 2>$null
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

$python = Resolve-PythonLaunch -RootDir $root
if (-not $python) {
    throw "No working Python interpreter found."
}

$argsList = @()
$argsList += $python.PrefixArgs
$argsList += @(
    "daily_ironclad_report.py",
    "--log-path", "data/signal_events_all_pairs.jsonl",
    "--window-hours", $WindowHours.ToString(),
    "--min-resolved-for-rule", $MinResolvedForRule.ToString(),
    "--out-dir", $OutDir
)

Push-Location $root
try {
    & $python.FilePath @argsList
}
finally {
    Pop-Location
}
