from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = int(round((len(values) - 1) * p))
    idx = max(0, min(idx, len(values) - 1))
    return values[idx]


def win_rate(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    bounced = sum(1 for r in rows if r.get("_outcome") == 1)
    return (bounced / len(rows)) * 100.0


def outcome_event_name(event: str) -> int | None:
    if event == "bounced":
        return 1
    if event in {"failed_breakdown", "failed_breakout", "expired"}:
        return 0
    return None


def wall_usd_bucket(v: float) -> str:
    if v < 10_000:
        return "<10k"
    if v < 25_000:
        return "10k-25k"
    if v < 50_000:
        return "25k-50k"
    if v < 100_000:
        return "50k-100k"
    if v < 250_000:
        return "100k-250k"
    if v < 500_000:
        return "250k-500k"
    return "500k+"


def ratio_bucket(v: float) -> str:
    if v < 3:
        return "<3"
    if v < 5:
        return "3-5"
    if v < 8:
        return "5-8"
    if v < 13:
        return "8-13"
    if v < 20:
        return "13-20"
    if v < 40:
        return "20-40"
    return "40+"


def dist_bucket(v: float) -> str:
    if v < 0.15:
        return "<0.15%"
    if v < 0.30:
        return "0.15-0.30%"
    if v < 0.60:
        return "0.30-0.60%"
    if v < 1.00:
        return "0.60-1.00%"
    if v < 1.50:
        return "1.00-1.50%"
    return "1.50%+"


def load_selected_filter(path: Path) -> dict[str, Any]:
    default = {
        "min_seen_count": 2,
        "min_visible_age_sec": 12.0,
        "min_dominance_ratio": 1.8,
        "min_stability_ratio": 0.55,
        "min_current_vs_peak_ratio": 0.55,
        "max_wall_level_index": 35,
        "max_wall_distance_pct": 3.0,
        "min_wall_ratio": 3.0,
        "min_wall_notional_usd": 20_000.0,
        "min_wall_to_day_volume_bps": 0.0,
        "min_wall_to_hour_volume_pct": 0.0,
        "min_round_level_score": 0.0,
        "max_wall_notional_volatility_ratio": 0.70,
        "min_pre_touch_decay_ratio": 0.62,
        "max_rebuild_count": 3,
    }
    if not path.exists():
        return default
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    if not isinstance(obj, dict):
        return default
    selected = obj.get("selected_filter")
    if not isinstance(selected, dict):
        return default
    out = dict(default)
    out.update(selected)
    return out


def passes_filter(row: dict[str, Any], f: dict[str, Any]) -> bool:
    seen_count = to_int(row.get("seen_count"), 0)
    visible_age = to_float(row.get("visible_age_sec"), 0.0)
    dominance = to_float(row.get("wall_dominance_ratio"), 0.0)
    stability = to_float(row.get("wall_notional_stability_ratio"), 0.0)
    cvp = to_float(row.get("wall_notional_current_vs_peak_ratio"), 0.0)
    level_idx = to_int(row.get("wall_level_index"), 99999)
    dist = to_float(row.get("wall_distance_from_spread_pct"), 9999.0)
    ratio = to_float(row.get("wall_ratio"), 0.0)
    wall_usd = to_float(row.get("wall_notional_usd"), 0.0)
    day_volume = to_float(row.get("day_volume_usd"), 0.0)
    round_score = to_float(row.get("round_level_score"), 0.0)
    volatility = to_float(row.get("wall_notional_volatility_ratio"), 0.0)
    pre_touch_decay = to_float(row.get("pre_touch_decay_ratio"), 1.0)
    rebuild_count = to_int(row.get("rebuild_count"), 0)
    wall_to_day_bps = 0.0
    if day_volume > 0:
        wall_to_day_bps = (wall_usd / day_volume) * 10_000.0
    wall_to_hour_pct = to_float(row.get("wall_to_hour_volume_pct"), 0.0)

    if seen_count < to_int(f.get("min_seen_count"), 0):
        return False
    if visible_age < to_float(f.get("min_visible_age_sec"), 0.0):
        return False
    if dominance < to_float(f.get("min_dominance_ratio"), 0.0):
        return False
    if stability < to_float(f.get("min_stability_ratio"), 0.0):
        return False
    if cvp < to_float(f.get("min_current_vs_peak_ratio"), 0.0):
        return False
    if level_idx > to_int(f.get("max_wall_level_index"), 99999):
        return False
    if dist > to_float(f.get("max_wall_distance_pct"), 9999.0):
        return False
    if ratio < to_float(f.get("min_wall_ratio"), 0.0):
        return False
    if wall_usd < to_float(f.get("min_wall_notional_usd"), 0.0):
        return False
    if wall_to_day_bps < to_float(f.get("min_wall_to_day_volume_bps"), 0.0):
        return False
    if wall_to_hour_pct < to_float(f.get("min_wall_to_hour_volume_pct"), 0.0):
        return False
    if round_score < to_float(f.get("min_round_level_score"), 0.0):
        return False
    if volatility > to_float(f.get("max_wall_notional_volatility_ratio"), 10.0):
        return False
    if pre_touch_decay < to_float(f.get("min_pre_touch_decay_ratio"), 0.0):
        return False
    if rebuild_count > to_int(f.get("max_rebuild_count"), 999999):
        return False
    return True


def build_bucket_table(rows: list[dict[str, Any]], key: str, bucket_fn: Any) -> list[dict[str, Any]]:
    totals: Counter[str] = Counter()
    bounced: Counter[str] = Counter()
    for r in rows:
        b = bucket_fn(to_float(r.get(key), 0.0))
        totals[b] += 1
        if r.get("_outcome") == 1:
            bounced[b] += 1
    out: list[dict[str, Any]] = []
    for bucket_name, total in totals.most_common():
        bcount = bounced[bucket_name]
        out.append(
            {
                "bucket": bucket_name,
                "resolved": total,
                "bounced": bcount,
                "win_rate_pct": (bcount / total) * 100.0 if total else 0.0,
            }
        )
    return out


def metric_summary(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    good = [to_float(r.get(key), 0.0) for r in rows if r.get("_outcome") == 1]
    bad = [to_float(r.get(key), 0.0) for r in rows if r.get("_outcome") == 0]
    return {
        "bounced_median": statistics.median(good) if good else 0.0,
        "bounced_p75": pct(good, 0.75),
        "bounced_p90": pct(good, 0.90),
        "failed_median": statistics.median(bad) if bad else 0.0,
        "failed_p75": pct(bad, 0.75),
        "failed_p90": pct(bad, 0.90),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze only real bounced ironclad walls vs failed walls")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--filters-json", default="data/ironclad_filters.json")
    parser.add_argument("--window-hours", type=float, default=24.0)
    parser.add_argument("--active-instance-only", action="store_true")
    parser.add_argument("--out-dir", default="data")
    args = parser.parse_args()

    log_path = Path(args.log_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    selected_filter = load_selected_filter(Path(args.filters_json))
    if not log_path.exists():
        raise SystemExit(f"log not found: {log_path}")

    cutoff_ts = 0.0
    if args.window_hours > 0:
        cutoff_ts = time.time() - (args.window_hours * 3600.0)

    active_instance_id = ""
    active_instance_start = 0.0
    if args.active_instance_only:
        lock_path = Path(str(log_path) + ".lock")
        if lock_path.exists():
            try:
                lock = json.loads(lock_path.read_text(encoding="utf-8"))
                active_instance_id = str(lock.get("collector_instance_id") or "")
                active_instance_start = to_float(lock.get("collector_started_at_ts"), 0.0)
            except Exception:
                active_instance_id = ""
                active_instance_start = 0.0

    resolved_rows: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            ts = to_float(row.get("ts"), 0.0)
            if ts <= 0:
                continue
            if cutoff_ts > 0 and ts < cutoff_ts:
                continue
            if active_instance_id:
                if str(row.get("collector_instance_id") or "") != active_instance_id:
                    continue
                if active_instance_start > 0 and ts < (active_instance_start - 1.0):
                    continue
            outcome = outcome_event_name(str(row.get("event", "")))
            if outcome is None:
                continue
            row["_outcome"] = outcome
            resolved_rows.append(row)

    ironclad_rows = [r for r in resolved_rows if passes_filter(r, selected_filter)]
    bounced_rows = [r for r in ironclad_rows if r.get("_outcome") == 1]

    per_coin_total: Counter[str] = Counter()
    per_coin_bounced: Counter[str] = Counter()
    for r in ironclad_rows:
        c = str(r.get("coin") or "")
        if not c:
            continue
        per_coin_total[c] += 1
        if r.get("_outcome") == 1:
            per_coin_bounced[c] += 1

    strong_coins: list[dict[str, Any]] = []
    for coin, total in per_coin_total.items():
        if total < 15:
            continue
        bounced = per_coin_bounced[coin]
        wr = (bounced / total) * 100.0 if total else 0.0
        if wr >= 35.0:
            strong_coins.append({"coin": coin, "resolved": total, "bounced": bounced, "win_rate_pct": wr})
    strong_coins.sort(key=lambda x: (x["win_rate_pct"], x["resolved"]), reverse=True)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "window_hours": args.window_hours,
        "active_instance_only": bool(args.active_instance_only),
        "active_instance_id": active_instance_id,
        "resolved_total": len(resolved_rows),
        "ironclad_resolved": len(ironclad_rows),
        "ironclad_bounced": len(bounced_rows),
        "ironclad_win_rate_pct": win_rate(ironclad_rows),
        "selected_filter": selected_filter,
        "metric_summary": {
            "wall_notional_usd": metric_summary(ironclad_rows, "wall_notional_usd"),
            "wall_ratio": metric_summary(ironclad_rows, "wall_ratio"),
            "wall_distance_from_spread_pct": metric_summary(ironclad_rows, "wall_distance_from_spread_pct"),
            "wall_dominance_ratio": metric_summary(ironclad_rows, "wall_dominance_ratio"),
            "visible_age_sec": metric_summary(ironclad_rows, "visible_age_sec"),
            "wall_notional_volatility_ratio": metric_summary(ironclad_rows, "wall_notional_volatility_ratio"),
            "pre_touch_decay_ratio": metric_summary(ironclad_rows, "pre_touch_decay_ratio"),
            "rebuild_count": metric_summary(ironclad_rows, "rebuild_count"),
        },
        "buckets": {
            "wall_notional_usd": build_bucket_table(ironclad_rows, "wall_notional_usd", wall_usd_bucket),
            "wall_ratio": build_bucket_table(ironclad_rows, "wall_ratio", ratio_bucket),
            "wall_distance_from_spread_pct": build_bucket_table(
                ironclad_rows, "wall_distance_from_spread_pct", dist_bucket
            ),
        },
        "strong_coins_min15_resolved": strong_coins[:25],
    }

    suffix = "_active" if args.active_instance_only else ""
    stamp = datetime.now().strftime("%Y-%m-%d")
    json_path = out_dir / f"ironclad_bounce_report_{stamp}{suffix}.json"
    md_path = out_dir / f"ironclad_bounce_report_{stamp}{suffix}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Ironclad Bounce Report")
    lines.append("")
    lines.append(f"- Generated UTC: `{report['generated_at_utc']}`")
    lines.append(f"- Window hours: `{args.window_hours}`")
    if args.active_instance_only:
        lines.append(f"- Active instance only: `{active_instance_id}`")
    lines.append(f"- Resolved total: `{len(resolved_rows)}`")
    lines.append(f"- Ironclad resolved: `{len(ironclad_rows)}`")
    lines.append(f"- Ironclad bounced: `{len(bounced_rows)}`")
    lines.append(f"- Ironclad win-rate: `{win_rate(ironclad_rows):.2f}%`")
    lines.append("")
    lines.append("## Selected Filter")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(selected_filter, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## Bounced-vs-Failed Metrics (Ironclad)")
    lines.append("")
    for key, metrics in report["metric_summary"].items():
        lines.append(
            f"- `{key}`: bounced med `{metrics['bounced_median']:.4f}` p75 `{metrics['bounced_p75']:.4f}` "
            f"p90 `{metrics['bounced_p90']:.4f}` | failed med `{metrics['failed_median']:.4f}` "
            f"p75 `{metrics['failed_p75']:.4f}` p90 `{metrics['failed_p90']:.4f}`"
        )
    lines.append("")
    lines.append("## Buckets (Ironclad)")
    lines.append("")
    for bucket_name, rows in report["buckets"].items():
        lines.append(f"### {bucket_name}")
        lines.append("")
        for row in rows[:12]:
            lines.append(
                f"- `{row['bucket']}`: resolved `{row['resolved']}`, bounced `{row['bounced']}`, "
                f"WR `{row['win_rate_pct']:.2f}%`"
            )
        lines.append("")
    lines.append("## Strong Coins (>=15 resolved, WR>=35%)")
    lines.append("")
    if strong_coins:
        for row in strong_coins[:25]:
            lines.append(
                f"- `{row['coin']}`: resolved `{row['resolved']}`, bounced `{row['bounced']}`, "
                f"WR `{row['win_rate_pct']:.2f}%`"
            )
    else:
        lines.append("- No symbols meet this threshold in the selected window.")
    lines.append("")

    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved JSON: {json_path}")
    print(f"Saved MD:   {md_path}")
    print(
        f"Ironclad resolved={len(ironclad_rows)} bounced={len(bounced_rows)} "
        f"win_rate={win_rate(ironclad_rows):.2f}%"
    )


if __name__ == "__main__":
    main()
