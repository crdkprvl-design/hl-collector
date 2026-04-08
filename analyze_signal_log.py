from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def ratio_bin(value: float) -> str:
    if value < 10:
        return "<10"
    if value < 15:
        return "10-15"
    if value < 20:
        return "15-20"
    if value < 30:
        return "20-30"
    return "30+"


def usd_bin(value: float) -> str:
    if value < 50_000:
        return "<50k"
    if value < 100_000:
        return "50k-100k"
    if value < 250_000:
        return "100k-250k"
    if value < 500_000:
        return "250k-500k"
    return "500k+"


def load_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists():
        return events
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    events.append(item)
            except json.JSONDecodeError:
                continue
    return events


def load_events_from_glob(glob_pattern: str) -> tuple[list[dict[str, Any]], list[Path]]:
    events: list[dict[str, Any]] = []
    matched = sorted(Path(".").glob(glob_pattern))
    for path in matched:
        if path.is_file():
            events.extend(load_events(path))
    return events, matched


def event_key(event: dict[str, Any]) -> str:
    market = str(event.get("market", ""))
    coin = str(event.get("coin", ""))
    side = str(event.get("side", ""))
    wall_price = str(event.get("wall_price", ""))
    touched = str(event.get("touch_price", ""))
    return f"{market}|{coin}|{side}|{wall_price}|{touched}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Hyperliquid density screener event log")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help="Example: cloud_data/events_*.jsonl")
    args = parser.parse_args()

    source_info = args.log_path
    if args.log_glob:
        events, matched = load_events_from_glob(args.log_glob)
        source_info = f"{args.log_glob} ({len(matched)} files)"
    else:
        events = load_events(Path(args.log_path))

    if not events:
        print("No events found.")
        return

    print(f"Loaded events: {len(events)} from {source_info}")

    touched_events: dict[str, dict[str, Any]] = {}
    outcomes: dict[str, str] = {}

    for event in events:
        kind = str(event.get("event", ""))
        key = event_key(event)
        if kind == "touched":
            touched_events[key] = event
        elif kind in {"bounced", "failed_breakout", "failed_breakdown", "expired"}:
            outcomes[key] = kind

    totals = defaultdict(int)
    wins = defaultdict(int)
    by_market = defaultdict(lambda: {"total": 0, "win": 0})

    for key, touched in touched_events.items():
        outcome = outcomes.get(key)
        if outcome is None:
            continue
        ratio = float(touched.get("wall_ratio", 0.0) or 0.0)
        wall_usd = float(touched.get("wall_notional_usd", 0.0) or 0.0)
        market = str(touched.get("market", "unknown"))

        rb = ratio_bin(ratio)
        ub = usd_bin(wall_usd)
        bucket = f"{rb} | {ub}"

        totals[bucket] += 1
        by_market[market]["total"] += 1
        if outcome == "bounced":
            wins[bucket] += 1
            by_market[market]["win"] += 1

    if not totals:
        armed_count = sum(1 for e in events if str(e.get("event", "")) == "armed")
        touched_count = sum(1 for e in events if str(e.get("event", "")) == "touched")
        print(f"No completed outcomes yet. armed={armed_count}, touched={touched_count}.")
        return

    print("\nBy market:")
    for market, stats in sorted(by_market.items()):
        total = stats["total"]
        win = stats["win"]
        wr = (win / total * 100.0) if total else 0.0
        print(f"{market:6s} total={total:5d} bounced={win:5d} win_rate={wr:6.2f}%")

    print("\nBy density bucket:")
    rows = []
    for bucket, total in totals.items():
        win = wins.get(bucket, 0)
        wr = (win / total * 100.0) if total else 0.0
        rows.append((wr, total, win, bucket))
    rows.sort(reverse=True)
    for wr, total, win, bucket in rows:
        print(f"{bucket:24s} total={total:5d} bounced={win:5d} win_rate={wr:6.2f}%")


if __name__ == "__main__":
    main()
