from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from derive_quality_rules import (
    INF_SENTINEL,
    ResolvedCase,
    apply_rule,
    build_cases,
    load_from_glob,
    load_jsonl,
    make_rule_grid,
    to_float,
    wilson_lower_bound,
)


def fmt_bound(value: float) -> str:
    if value >= INF_SENTINEL / 2:
        return "inf"
    return f"{value:.0f}"


def rule_to_human(rule: dict[str, Any]) -> str:
    return (
        f"market={rule['market']} "
        f"side={rule.get('side', 'all')} "
        f"ratio=[{fmt_bound(rule['min_ratio'])}, {fmt_bound(rule['max_ratio'])}) "
        f"wall=[{fmt_bound(rule['min_notional_usd'])}, {fmt_bound(rule['max_notional_usd'])}) "
        f"dayVol=[{fmt_bound(rule['min_day_volume_usd'])}, {fmt_bound(rule['max_day_volume_usd'])}) "
        f"dist<={rule.get('max_wall_distance_pct_from_spread', 3.0):.1f}%"
    )


def summarize_outcomes(rows: list[ResolvedCase]) -> dict[str, Any]:
    total = len(rows)
    bounced = sum(1 for r in rows if r.outcome == "bounced")
    failed = sum(1 for r in rows if r.outcome == "failed")
    wr = (bounced / total * 100.0) if total else 0.0

    by_side: dict[str, dict[str, float | int]] = {}
    for side in ("bid", "ask"):
        subset = [r for r in rows if r.side == side]
        wins = sum(1 for r in subset if r.outcome == "bounced")
        cnt = len(subset)
        by_side[side] = {
            "total": cnt,
            "bounced": wins,
            "win_rate_pct": (wins / cnt * 100.0) if cnt else 0.0,
        }

    by_market: dict[str, dict[str, float | int]] = {}
    for market in ("perp", "spot"):
        subset = [r for r in rows if r.market == market]
        wins = sum(1 for r in subset if r.outcome == "bounced")
        cnt = len(subset)
        by_market[market] = {
            "total": cnt,
            "bounced": wins,
            "win_rate_pct": (wins / cnt * 100.0) if cnt else 0.0,
        }

    return {
        "total": total,
        "bounced": bounced,
        "failed": failed,
        "win_rate_pct": wr,
        "by_side": by_side,
        "by_market": by_market,
    }


def ironclad_filter(rows: list[ResolvedCase]) -> list[ResolvedCase]:
    out: list[ResolvedCase] = []
    for r in rows:
        if r.seen_count < 2:
            continue
        if r.visible_age_sec < 12.0:
            continue
        if r.wall_dominance_ratio < 1.55:
            continue
        if r.wall_notional_stability_ratio < 0.42:
            continue
        if r.wall_distance_from_spread_pct > 3.0:
            continue
        if r.wall_ratio < 3.0:
            continue
        if r.wall_notional_usd < 10_000.0:
            continue
        out.append(r)
    return out


def score_rule_candidates(
    resolved: list[ResolvedCase],
    *,
    min_resolved: int,
    min_unique_coins: int,
    max_top_coin_share_pct: float,
) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for rule in make_rule_grid():
        subset = apply_rule(resolved, rule)
        n = len(subset)
        if n < min_resolved:
            continue

        wins = sum(1 for c in subset if c.outcome == "bounced")
        lb = wilson_lower_bound(wins, n)
        wr = wins / n
        coins = Counter(c.coin for c in subset)
        unique_coins = len(coins)
        top_coin_share_pct = (coins.most_common(1)[0][1] / n * 100.0) if coins else 0.0
        if unique_coins < min_unique_coins:
            continue
        if top_coin_share_pct > max_top_coin_share_pct:
            continue

        scored.append(
            {
                **rule,
                "resolved": n,
                "bounced": wins,
                "win_rate_pct": wr * 100.0,
                "wilson_lb_pct": lb * 100.0,
                "unique_coins": unique_coins,
                "top_coin_share_pct": top_coin_share_pct,
                "score": lb * math.log1p(n),
            }
        )

    scored.sort(
        key=lambda r: (
            r["score"],
            r["wilson_lb_pct"],
            r["resolved"],
        ),
        reverse=True,
    )
    return scored


def safe_name_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def build_report_md(summary: dict[str, Any]) -> str:
    head = summary["headline"]
    overall = summary["overall"]
    iron = summary["ironclad"]
    top_rules = summary["top_rules"]

    lines: list[str] = []
    lines.append("# Ironclad Daily Report")
    lines.append("")
    lines.append(f"- Source: `{head['source']}`")
    lines.append(f"- Window UTC: `{head['window_start_utc']}` -> `{head['window_end_utc']}` ({head['window_hours']}h)")
    lines.append(f"- Events loaded (window): `{head['events_in_window']}`")
    lines.append(f"- Cases resolved in window: `{overall['total']}`")
    lines.append("")
    lines.append("## Outcome Summary")
    lines.append("")
    lines.append(
        f"- Overall: resolved `{overall['total']}`, bounced `{overall['bounced']}`, "
        f"failed `{overall['failed']}`, win-rate `{overall['win_rate_pct']:.2f}%`"
    )
    lines.append(
        f"- Ironclad filter: resolved `{iron['total']}`, bounced `{iron['bounced']}`, "
        f"failed `{iron['failed']}`, win-rate `{iron['win_rate_pct']:.2f}%`"
    )
    lines.append("")
    lines.append("## By Side")
    lines.append("")
    for side, stats in overall["by_side"].items():
        lines.append(
            f"- {side}: resolved `{stats['total']}`, bounced `{stats['bounced']}`, "
            f"win-rate `{stats['win_rate_pct']:.2f}%`"
        )
    lines.append("")
    lines.append("## By Market")
    lines.append("")
    for market, stats in overall["by_market"].items():
        lines.append(
            f"- {market}: resolved `{stats['total']}`, bounced `{stats['bounced']}`, "
            f"win-rate `{stats['win_rate_pct']:.2f}%`"
        )
    lines.append("")
    lines.append("## Top Rule Candidates")
    lines.append("")
    if not top_rules:
        lines.append("- Not enough data for rule candidates with current minimum constraints.")
    else:
        for idx, rule in enumerate(top_rules[:10], start=1):
            lines.append(
                f"{idx}. `{rule_to_human(rule)}` | resolved `{rule['resolved']}` | "
                f"win-rate `{rule['win_rate_pct']:.2f}%` | lb `{rule['wilson_lb_pct']:.2f}%` | "
                f"coins `{rule['unique_coins']}` | top coin share `{rule['top_coin_share_pct']:.1f}%`"
            )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Keep collector running continuously for stable sample quality.")
    lines.append("- Avoid threshold changes during the collection phase (until enough out-of-sample data).")
    lines.append("- Final screener thresholds should be locked only after multi-day validation.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily quality report for ironclad density bounce setup")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help='Example: "cloud_data/events_*.jsonl"')
    parser.add_argument("--window-hours", type=int, default=24)
    parser.add_argument("--min-resolved-for-rule", type=int, default=40)
    parser.add_argument("--min-unique-coins", type=int, default=8)
    parser.add_argument("--max-top-coin-share-pct", type=float, default=45.0)
    parser.add_argument("--out-dir", default="data/reports")
    args = parser.parse_args()

    if args.log_glob:
        events, matched = load_from_glob(args.log_glob)
        source = f"{args.log_glob} ({len(matched)} files)"
    else:
        events = load_jsonl(Path(args.log_path))
        source = args.log_path

    if not events:
        raise SystemExit("No events found.")

    all_ts = [to_float(e.get("ts")) for e in events if to_float(e.get("ts")) > 0.0]
    if not all_ts:
        raise SystemExit("No valid timestamps in events.")
    end_ts = max(all_ts)
    start_ts = end_ts - (args.window_hours * 3600)
    window_events = [e for e in events if to_float(e.get("ts")) >= start_ts]
    if not window_events:
        raise SystemExit("No events in selected time window.")

    cases = build_cases(window_events)
    resolved = [c for c in cases if c.outcome in {"bounced", "failed"}]
    if not resolved:
        raise SystemExit("No resolved bounced/failed cases in selected time window.")

    overall = summarize_outcomes(resolved)
    iron_rows = ironclad_filter(resolved)
    iron_summary = summarize_outcomes(iron_rows)
    top_rules = score_rule_candidates(
        resolved,
        min_resolved=args.min_resolved_for_rule,
        min_unique_coins=args.min_unique_coins,
        max_top_coin_share_pct=args.max_top_coin_share_pct,
    )

    summary = {
        "headline": {
            "source": source,
            "generated_at_utc": iso_utc(time.time()),
            "window_start_utc": iso_utc(start_ts),
            "window_end_utc": iso_utc(end_ts),
            "window_hours": args.window_hours,
            "events_in_window": len(window_events),
        },
        "overall": overall,
        "ironclad": iron_summary,
        "top_rules": top_rules[:25],
        "samples": {
            "resolved_cases": len(resolved),
            "ironclad_cases": len(iron_rows),
            "resolved_preview": [asdict(c) for c in resolved[:5]],
        },
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = safe_name_ts(end_ts)
    out_json = out_dir / f"ironclad_daily_{stamp}.json"
    out_md = out_dir / f"ironclad_daily_{stamp}.md"

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(build_report_md(summary), encoding="utf-8")

    print(f"Saved JSON report: {out_json}")
    print(f"Saved MD report:   {out_md}")
    print(
        f"Resolved={overall['total']} bounced={overall['bounced']} "
        f"WR={overall['win_rate_pct']:.2f}% | "
        f"Ironclad resolved={iron_summary['total']} WR={iron_summary['win_rate_pct']:.2f}%"
    )
    if top_rules:
        best = top_rules[0]
        print(
            f"Top rule: {rule_to_human(best)} | resolved={best['resolved']} "
            f"WR={best['win_rate_pct']:.2f}% LB={best['wilson_lb_pct']:.2f}%"
        )
    else:
        print("Top rule: not enough samples with current constraints.")


if __name__ == "__main__":
    main()
