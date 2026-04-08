# Cloud Collector Data

This directory is updated by the GitHub Actions workflow:
`.github/workflows/hyperliquid-collector.yml`.

- Daily event logs: `events_YYYY-MM-DD.jsonl`
- Run registry: `run_log.csv`

Each JSON line is an event emitted by `density_screener.py`
(`armed`, `touched`, `bounced`, `failed_breakout`, `failed_breakdown`, `expired`).
