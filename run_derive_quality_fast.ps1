param(
    [int]$TailLines = 250000,
    [int]$MinResolved = 40,
    [string]$SourceLog = "data/signal_events_all_pairs.jsonl",
    [string]$TailLog = "data/_recent_events_tail.jsonl",
    [string]$OutJson = "data/quality_rules.json"
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $SourceLog)) {
    throw "Source log not found: $SourceLog"
}

Write-Host "Preparing tail log: $TailLines lines from $SourceLog"
Get-Content $SourceLog -Tail $TailLines | Set-Content -Path $TailLog -Encoding UTF8

$env:PYTHONDONTWRITEBYTECODE = "1"
$python = if (Test-Path ".\venv\Scripts\python.exe") { ".\venv\Scripts\python.exe" } else { "py -3" }

$cmd = "derive_quality_rules.py --log-path `"$TailLog`" --out-json `"$OutJson`" --min-resolved $MinResolved"
Write-Host "Running: $python $cmd"

if ($python -eq "py -3") {
    py -3 derive_quality_rules.py --log-path $TailLog --out-json $OutJson --min-resolved $MinResolved
} else {
    & .\venv\Scripts\python.exe derive_quality_rules.py --log-path $TailLog --out-json $OutJson --min-resolved $MinResolved
}

Write-Host "Done. Updated: $OutJson"
