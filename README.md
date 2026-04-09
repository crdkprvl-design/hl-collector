# Hyperliquid Density Bounce Screener

This repository now includes a real-time screener focused on large order-book densities ("walls")
for both Hyperliquid perp and spot markets.

Trading idea:
- detect large bid/ask density
- wait for touch or near-touch (`3-4` ticks from the wall)
- track bounce outcome `>= 0.5%` from the wall

## Files

- `density_screener.py`: real-time scanner and signal logger
- `analyze_signal_log.py`: post-analysis of logged signal outcomes (win-rate by density bucket)
- `calibrate_thresholds.py`: one-shot market-wide calibration for wall-size thresholds
- `backtest_imbalance_sample.py`: historical backtest for Imbalance Labs sample (24 symbols, 7d)
- `derive_quality_rules.py`: derives robust quality filters from collected real-time outcomes
- `hyperliquid_client.py`, `app.py`, `screener.py`: previous MVP files (kept for reference)

## Requirements

- Python 3.10+
- `requests`

## Install

```bash
py -3 -m pip install -r requirements.txt
```

## Run Real-Time Screener

```bash
py -3 density_screener.py ^
  --min-wall-ratio 10 ^
  --min-wall-usd 50000 ^
  --min-day-volume-usd 1000000 ^
  --approach-ticks 4 ^
  --bounce-pct 0.5 ^
  --run-seconds 0
```

Logs are written to `data/signal_events.jsonl`.

## Background Collector (All Hyperliquid Pairs)

Start continuous collection in background:

```bash
powershell -ExecutionPolicy Bypass -File .\start_collector.ps1
```

Check status:

```bash
powershell -ExecutionPolicy Bypass -File .\collector_status.ps1
```

Stop collector:

```bash
powershell -ExecutionPolicy Bypass -File .\stop_collector.ps1
```

Collector outputs:
- `data/signal_events_all_pairs.jsonl`
- `data/collector_stdout.log`
- `data/collector_stderr.log`

## Cloud Collector via GitHub Actions (No Server Needed)

Use workflow: `.github/workflows/hyperliquid-collector.yml`

What it does:
- runs every 5 minutes (`cron`)
- collects events for ~4 minutes
- appends events to `cloud_data/events_YYYY-MM-DD.jsonl`
- commits and pushes updates automatically
- derives `cloud_data/quality_rules.json` from collected outcomes
- triggers next run automatically (cron fallback) unless disabled

Important:
- this is server-like cloud execution, but not a dedicated always-on VPS
- scheduled jobs can have slight delays
- repository must allow workflow write access to contents
- optional repo variable `HL_CHAIN_ENABLED=false` disables self-trigger chain

Analyze cloud logs:

```bash
py -3 analyze_signal_log.py --log-glob "cloud_data/events_*.jsonl"
```

Derive "high-quality wall" rules from local or cloud collector logs:

```bash
py -3 derive_quality_rules.py --log-path data/signal_events_all_pairs.jsonl --out-json data/quality_rules.json
```

Run screener with derived quality profile:

```bash
py -3 density_screener.py --quality-rules-json data/quality_rules.json --quality-profile strict
```

## One-Shot Threshold Calibration

```bash
py -3 calibrate_thresholds.py --min-day-volume-usd 1000000 --min-wall-usd 50000 --top 30
```

## Analyze Logged Signals

```bash
py -3 analyze_signal_log.py --log-path data/signal_events.jsonl
```

This script reports bounce hit-rate by:
- market (`spot` / `perp`)
- density buckets (`wall_ratio`, `wall_notional_usd`)

## Backtest on Downloaded Imbalance Sample

```bash
py -3 backtest_imbalance_sample.py ^
  --data-dir "data/imbalance_sample" ^
  --horizon-bars 24 ^
  --bounce-pct 0.5 ^
  --touch-ticks 4 ^
  --breakout-ticks 2 ^
  --min-candidate-ratio 3 ^
  --min-candidate-notional 10000
```

Outputs:
- `data/imbalance_sample/backtest_summary.json`
- `data/imbalance_sample/backtest_events.csv`

## Important Limitation (Historical Backfill)

Official long-range historical book data is in requester-pays S3 buckets.
Without AWS credentials and `--request-payer requester`, full retrospective backtesting is limited.

The current setup can:
- scan all pairs in real-time
- log outcomes
- learn/retune thresholds from collected logs
