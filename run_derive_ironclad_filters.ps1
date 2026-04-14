Param(
    [int]$WindowHours = 0,
    [int]$MinResolved = 80,
    [int]$MinUniqueCoins = 10,
    [double]$MaxTopCoinSharePct = 45.0
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
    "derive_ironclad_filters.py",
    "--log-path", "data/signal_events_all_pairs.jsonl",
    "--window-hours", $WindowHours.ToString(),
    "--min-resolved", $MinResolved.ToString(),
    "--min-unique-coins", $MinUniqueCoins.ToString(),
    "--max-top-coin-share-pct", $MaxTopCoinSharePct.ToString(),
    "--out-json", "data/ironclad_filters.json"
)

Push-Location $root
try {
    & $python.FilePath @argsList
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    $symbolArgs = @()
    $symbolArgs += $python.PrefixArgs
    $symbolArgs += @(
        "derive_symbol_ironclad_filters.py",
        "--log-path", "data/signal_events_all_pairs.jsonl",
        "--window-hours", $WindowHours.ToString(),
        "--out-json", "data/symbol_ironclad_filters.json"
    )

    & $python.FilePath @symbolArgs
}
finally {
    Pop-Location
}
