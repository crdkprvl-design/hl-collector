from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any


LEVELS = 10


@dataclass
class Bar:
    timestamp: datetime
    symbol: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    bid_cum: list[float]
    ask_cum: list[float]
    bid_dist_bps: list[float]
    ask_dist_bps: list[float]


@dataclass
class Event:
    symbol: str
    timestamp: datetime
    side: str
    level: int
    wall_price: float
    wall_notional: float
    wall_ratio: float
    wall_distance_bps: float
    l1_dist_bps: float
    outcome: str
    touched: bool
    resolved: bool


def to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def iter_sample_files(data_dir: Path) -> list[Path]:
    return sorted(data_dir.glob("*_sample_7d.csv"))


def read_bars(csv_path: Path) -> list[Bar]:
    bars: list[Bar] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bid_cum = [to_float(row[f"bid_volume_level_{i}"]) for i in range(1, LEVELS + 1)]
            ask_cum = [to_float(row[f"ask_volume_level_{i}"]) for i in range(1, LEVELS + 1)]
            bid_dist = [to_float(row[f"bid_distance_level_{i}"]) for i in range(1, LEVELS + 1)]
            ask_dist = [to_float(row[f"ask_distance_level_{i}"]) for i in range(1, LEVELS + 1)]
            bars.append(
                Bar(
                    timestamp=parse_ts(row["timestamp_utc"]),
                    symbol=row["instrument_symbol"],
                    open_price=to_float(row["open_price"]),
                    high_price=to_float(row["high_price"]),
                    low_price=to_float(row["low_price"]),
                    close_price=to_float(row["close_price"]),
                    bid_cum=bid_cum,
                    ask_cum=ask_cum,
                    bid_dist_bps=bid_dist,
                    ask_dist_bps=ask_dist,
                )
            )
    return bars


def marginal(cumulative: list[float]) -> list[float]:
    out = []
    prev = 0.0
    for x in cumulative:
        m = x - prev
        out.append(m if m > 0 else 0.0)
        prev = x
    return out


def simulate_outcome(
    future: list[Bar],
    side: str,
    wall_price: float,
    touch_tol_bps: float,
    breakout_tol_bps: float,
    bounce_pct: float,
) -> tuple[str, bool, bool]:
    touch_tol = touch_tol_bps / 10_000.0
    break_tol = breakout_tol_bps / 10_000.0
    bounce_up = wall_price * (1.0 + bounce_pct / 100.0)
    bounce_down = wall_price * (1.0 - bounce_pct / 100.0)

    touched = False
    for bar in future:
        if not touched:
            if side == "bid":
                if bar.low_price <= wall_price * (1.0 + touch_tol):
                    touched = True
            else:
                if bar.high_price >= wall_price * (1.0 - touch_tol):
                    touched = True

        if not touched:
            continue

        if side == "bid":
            hit_fail = bar.low_price <= wall_price * (1.0 - break_tol)
            hit_bounce = bar.high_price >= bounce_up
            if hit_fail and hit_bounce:
                return "ambiguous", True, False
            if hit_fail:
                return "failed", True, True
            if hit_bounce:
                return "bounced", True, True
        else:
            hit_fail = bar.high_price >= wall_price * (1.0 + break_tol)
            hit_bounce = bar.low_price <= bounce_down
            if hit_fail and hit_bounce:
                return "ambiguous", True, False
            if hit_fail:
                return "failed", True, True
            if hit_bounce:
                return "bounced", True, True

    if not touched:
        return "untouched", False, False
    return "unresolved", True, False


def ratio_bucket(value: float) -> str:
    if value < 2:
        return "<2"
    if value < 3:
        return "2-3"
    if value < 5:
        return "3-5"
    if value < 8:
        return "5-8"
    if value < 13:
        return "8-13"
    if value < 21:
        return "13-21"
    return "21+"


def usd_bucket(value: float) -> str:
    if value < 10_000:
        return "<10k"
    if value < 25_000:
        return "10k-25k"
    if value < 50_000:
        return "25k-50k"
    if value < 100_000:
        return "50k-100k"
    if value < 250_000:
        return "100k-250k"
    if value < 500_000:
        return "250k-500k"
    return "500k+"


def pct(x: int, y: int) -> float:
    if y <= 0:
        return 0.0
    return (x / y) * 100.0


def collect_events(
    bars: list[Bar],
    horizon_bars: int,
    bounce_pct: float,
    touch_ticks: float,
    breakout_ticks: float,
    min_candidate_ratio: float,
    min_candidate_notional: float,
) -> list[Event]:
    events: list[Event] = []
    if len(bars) < horizon_bars + 2:
        return events

    for i in range(len(bars) - horizon_bars - 1):
        bar = bars[i]
        future = bars[i + 1 : i + 1 + horizon_bars]

        for side in ("bid", "ask"):
            cums = bar.bid_cum if side == "bid" else bar.ask_cum
            dists = bar.bid_dist_bps if side == "bid" else bar.ask_dist_bps
            marg = marginal(cums)
            pos = [x for x in marg if x > 0]
            if not pos:
                continue
            med = median(pos)
            if med <= 0:
                continue

            l1_dist = max(dists[0], 0.0001)
            touch_tol_bps = max(0.0001, l1_dist * touch_ticks)
            breakout_tol_bps = max(0.0001, l1_dist * breakout_ticks)

            for level in range(1, LEVELS + 1):
                idx = level - 1
                level_vol = marg[idx]
                if level_vol <= 0:
                    continue
                wall_ratio = level_vol / med
                dist_bps = dists[idx]

                if side == "bid":
                    wall_price = bar.close_price * (1.0 - dist_bps / 10_000.0)
                else:
                    wall_price = bar.close_price * (1.0 + dist_bps / 10_000.0)
                wall_notional = wall_price * level_vol

                if wall_ratio < min_candidate_ratio:
                    continue
                if wall_notional < min_candidate_notional:
                    continue

                outcome, touched, resolved = simulate_outcome(
                    future=future,
                    side=side,
                    wall_price=wall_price,
                    touch_tol_bps=touch_tol_bps,
                    breakout_tol_bps=breakout_tol_bps,
                    bounce_pct=bounce_pct,
                )

                events.append(
                    Event(
                        symbol=bar.symbol,
                        timestamp=bar.timestamp,
                        side=side,
                        level=level,
                        wall_price=wall_price,
                        wall_notional=wall_notional,
                        wall_ratio=wall_ratio,
                        wall_distance_bps=dist_bps,
                        l1_dist_bps=l1_dist,
                        outcome=outcome,
                        touched=touched,
                        resolved=resolved,
                    )
                )
    return events


def summarize(events: list[Event]) -> dict[str, Any]:
    touched = [e for e in events if e.touched]
    resolved = [e for e in events if e.resolved]
    bounced = [e for e in resolved if e.outcome == "bounced"]
    failed = [e for e in resolved if e.outcome == "failed"]
    ambiguous = [e for e in events if e.outcome == "ambiguous"]
    unresolved = [e for e in events if e.outcome == "unresolved"]
    untouched = [e for e in events if e.outcome == "untouched"]

    return {
        "events_total": len(events),
        "touched": len(touched),
        "resolved": len(resolved),
        "bounced": len(bounced),
        "failed": len(failed),
        "ambiguous": len(ambiguous),
        "unresolved": len(unresolved),
        "untouched": len(untouched),
        "win_rate_resolved_pct": pct(len(bounced), len(resolved)),
        "touch_rate_pct": pct(len(touched), len(events)),
    }


def best_threshold_grid(events: list[Event]) -> list[dict[str, Any]]:
    ratio_grid = [2, 3, 5, 8, 13, 21]
    notional_grid = [10_000, 25_000, 50_000, 100_000, 250_000, 500_000]
    dist_grid = [0.25, 0.5, 1.0, 2.0, 5.0]  # bps

    rows: list[dict[str, Any]] = []
    for min_ratio in ratio_grid:
        for min_notional in notional_grid:
            for max_dist_bps in dist_grid:
                subset = [
                    e
                    for e in events
                    if e.wall_ratio >= min_ratio
                    and e.wall_notional >= min_notional
                    and e.wall_distance_bps <= max_dist_bps
                    and e.outcome in {"bounced", "failed"}
                ]
                if not subset:
                    continue
                wins = sum(1 for e in subset if e.outcome == "bounced")
                total = len(subset)
                wr = pct(wins, total)
                rows.append(
                    {
                        "min_ratio": min_ratio,
                        "min_notional": min_notional,
                        "max_dist_bps": max_dist_bps,
                        "resolved": total,
                        "bounced": wins,
                        "win_rate_pct": wr,
                    }
                )
    rows.sort(key=lambda r: (r["win_rate_pct"], r["resolved"]), reverse=True)
    return rows


def bucket_stats(events: list[Event]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, int]] = {}
    for e in events:
        if e.outcome not in {"bounced", "failed"}:
            continue
        key = (ratio_bucket(e.wall_ratio), usd_bucket(e.wall_notional))
        if key not in buckets:
            buckets[key] = {"resolved": 0, "bounced": 0}
        buckets[key]["resolved"] += 1
        if e.outcome == "bounced":
            buckets[key]["bounced"] += 1

    out = []
    for (rb, ub), v in buckets.items():
        out.append(
            {
                "ratio_bucket": rb,
                "notional_bucket": ub,
                "resolved": v["resolved"],
                "bounced": v["bounced"],
                "win_rate_pct": pct(v["bounced"], v["resolved"]),
            }
        )
    out.sort(key=lambda x: (x["win_rate_pct"], x["resolved"]), reverse=True)
    return out


def symbol_stats(events: list[Event]) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, int]] = {}
    for e in events:
        if e.outcome not in {"bounced", "failed"}:
            continue
        sym = e.symbol
        if sym not in stats:
            stats[sym] = {"resolved": 0, "bounced": 0}
        stats[sym]["resolved"] += 1
        if e.outcome == "bounced":
            stats[sym]["bounced"] += 1

    out = []
    for sym, v in stats.items():
        out.append(
            {
                "symbol": sym,
                "resolved": v["resolved"],
                "bounced": v["bounced"],
                "win_rate_pct": pct(v["bounced"], v["resolved"]),
            }
        )
    out.sort(key=lambda x: (x["win_rate_pct"], x["resolved"]), reverse=True)
    return out


def top_success_examples(events: list[Event], limit: int = 20) -> list[dict[str, Any]]:
    bounced = [e for e in events if e.outcome == "bounced"]
    bounced.sort(key=lambda e: e.wall_notional, reverse=True)
    out = []
    for e in bounced[:limit]:
        out.append(
            {
                "timestamp_utc": e.timestamp.isoformat(),
                "symbol": e.symbol,
                "side": e.side,
                "level": e.level,
                "wall_price": round(e.wall_price, 8),
                "wall_notional": round(e.wall_notional, 2),
                "wall_ratio": round(e.wall_ratio, 4),
                "distance_bps": round(e.wall_distance_bps, 4),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest density-bounce logic on Imbalance Labs sample")
    parser.add_argument("--data-dir", default="data/imbalance_sample")
    parser.add_argument("--horizon-bars", type=int, default=24, help="5m bars; 24 = 2 hours")
    parser.add_argument("--bounce-pct", type=float, default=0.5)
    parser.add_argument("--touch-ticks", type=float, default=4.0)
    parser.add_argument("--breakout-ticks", type=float, default=2.0)
    parser.add_argument("--min-candidate-ratio", type=float, default=3.0)
    parser.add_argument("--min-candidate-notional", type=float, default=10_000.0)
    parser.add_argument("--out-json", default="data/imbalance_sample/backtest_summary.json")
    parser.add_argument("--out-events-csv", default="data/imbalance_sample/backtest_events.csv")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    files = iter_sample_files(data_dir)
    if not files:
        raise SystemExit(f"No *_sample_7d.csv files found in {data_dir}")

    all_events: list[Event] = []
    processed_files = 0
    for csv_path in files:
        bars = read_bars(csv_path)
        events = collect_events(
            bars=bars,
            horizon_bars=args.horizon_bars,
            bounce_pct=args.bounce_pct,
            touch_ticks=args.touch_ticks,
            breakout_ticks=args.breakout_ticks,
            min_candidate_ratio=args.min_candidate_ratio,
            min_candidate_notional=args.min_candidate_notional,
        )
        all_events.extend(events)
        processed_files += 1

    summary = summarize(all_events)
    bucket = bucket_stats(all_events)
    by_symbol = symbol_stats(all_events)
    grid = best_threshold_grid(all_events)
    top_examples = top_success_examples(all_events, limit=30)

    result = {
        "config": {
            "data_dir": str(data_dir),
            "horizon_bars": args.horizon_bars,
            "bounce_pct": args.bounce_pct,
            "touch_ticks": args.touch_ticks,
            "breakout_ticks": args.breakout_ticks,
            "min_candidate_ratio": args.min_candidate_ratio,
            "min_candidate_notional": args.min_candidate_notional,
        },
        "files_processed": processed_files,
        "summary": summary,
        "best_threshold_grid_top20": grid[:20],
        "bucket_stats_top40": bucket[:40],
        "symbol_stats": by_symbol,
        "top_success_examples": top_examples,
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    events_path = Path(args.out_events_csv)
    events_path.parent.mkdir(parents=True, exist_ok=True)
    with events_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_utc",
                "symbol",
                "side",
                "level",
                "wall_price",
                "wall_notional",
                "wall_ratio",
                "wall_distance_bps",
                "l1_dist_bps",
                "outcome",
                "touched",
                "resolved",
            ]
        )
        for e in all_events:
            writer.writerow(
                [
                    e.timestamp.isoformat(),
                    e.symbol,
                    e.side,
                    e.level,
                    f"{e.wall_price:.10f}",
                    f"{e.wall_notional:.4f}",
                    f"{e.wall_ratio:.6f}",
                    f"{e.wall_distance_bps:.6f}",
                    f"{e.l1_dist_bps:.6f}",
                    e.outcome,
                    int(e.touched),
                    int(e.resolved),
                ]
            )

    print(f"Files processed: {processed_files}")
    print("Summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    print("\nTop threshold combos (min 20 shown):")
    for row in grid[:20]:
        print(
            f"  ratio>={row['min_ratio']:<4} notional>={int(row['min_notional']):<7} "
            f"dist<={row['max_dist_bps']:<4}bps resolved={row['resolved']:<6} "
            f"bounced={row['bounced']:<6} wr={row['win_rate_pct']:.2f}%"
        )

    print("\nTop buckets:")
    for row in bucket[:20]:
        print(
            f"  ratio={row['ratio_bucket']:<5} notional={row['notional_bucket']:<10} "
            f"resolved={row['resolved']:<6} bounced={row['bounced']:<6} wr={row['win_rate_pct']:.2f}%"
        )

    print(f"\nSaved: {out_path}")
    print(f"Saved: {events_path}")


if __name__ == "__main__":
    main()
