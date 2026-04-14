$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$env:PYTHONDONTWRITEBYTECODE = "1"

& ".\venv\Scripts\python.exe" ".\daily_paper_trade_summary.py" `
  --log-path "data\signal_events_all_pairs.jsonl" `
  --cloud-glob "cloud_data/events_*.jsonl" `
  --profile "trash_ask" `
  --min-score 160 `
  --entry-weight 0.12 `
  --out-json "data\reports\daily_paper_trade_summary_trash_ask.json" `
  --out-md-dir "data\reports" `
  --history-path "data\reports\daily_metrics_history.jsonl"

& ".\venv\Scripts\python.exe" ".\microsize_readiness_report.py" `
  --history-path "data\reports\daily_metrics_history.jsonl" `
  --profile "trash_ask" `
  --mode "B" `
  --out-json "data\reports\microsize_readiness_trash_ask.json" `
  --out-md "data\reports\microsize_readiness_trash_ask.md"
