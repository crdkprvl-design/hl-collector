# Ironclad Density Protocol

## Goal
Collect enough real Hyperliquid order-book outcomes and lock a production screener that highlights only high-probability bounce walls (not market-maker noise, not passive grid liquidity).

## Phase 1: Collection (2-4 days)
- Keep collector running continuously.
- Avoid changing screener thresholds during this phase.
- Primary log: `data/signal_events_all_pairs.jsonl`

Commands:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_collector.ps1
powershell -ExecutionPolicy Bypass -File .\collector_status.ps1
```

## Phase 2: Daily Analysis
- Run daily report on last 24h.
- Track:
  - resolved cases
  - bounce win-rate
  - side split (`bid` / `ask`)
  - market split (`perp` / `spot`)
  - strict ironclad-filter performance
  - best rule candidates by Wilson lower bound + sample size

Command:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_daily_ironclad_report.ps1 -WindowHours 24
```

Outputs:
- `data/reports/ironclad_daily_YYYY-MM-DD.json`
- `data/reports/ironclad_daily_YYYY-MM-DD.md`

## Phase 2.5: Rebuild Anti-Fake Filter
- Recompute anti-noise (ironclad) thresholds from accumulated outcomes.
- This updates GUI runtime filtering via `data/ironclad_filters.json`.

Command:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_derive_ironclad_filters.ps1 -WindowHours 0 -MinResolved 80
```

## Phase 3: Lock Final Rules
- Only after enough out-of-sample data.
- Recommend minimum:
  - >= 72h of continuous data
  - >= 500 resolved cases overall
  - >= 120 resolved cases for the target profile
  - acceptable coin concentration (avoid 1-2 coin dominance)

Then:
- Rebuild profile rules from full collection:

```powershell
py -3 derive_quality_rules.py --log-path data/signal_events_all_pairs.jsonl --out-json data/quality_rules.json
```

- Freeze final production profile for the screener.

## Current Anti-Noise Baseline
Applied in GUI collector mode:
- `seen_count >= 2`
- `visible_age_sec >= 12`
- `wall_dominance_ratio >= 1.55`
- `wall_notional_stability_ratio >= 0.42`
- `wall_distance_from_spread_pct <= 3.0`
- `wall_ratio >= 3.0`
- `wall_notional_usd >= 10,000`

These are baseline constraints and can be tightened after multi-day validation.
