from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ResolvedCase:
    market: str
    coin: str
    side: str
    wall_ratio: float
    wall_notional_usd: float
    day_volume_usd: float
    outcome: str  # bounced | failed | expired
    touched_ts: float
    resolved_ts: float
    lifetime_sec: float


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
            except json.JSONDecodeError:
                continue
    return rows


def load_from_glob(glob_pattern: str) -> tuple[list[dict[str, Any]], list[Path]]:
    matched = sorted(Path(".").glob(glob_pattern))
    events: list[dict[str, Any]] = []
    for path in matched:
        if path.is_file():
            events.extend(load_jsonl(path))
    return events, matched


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def key(event: dict[str, Any], include_touch_price: bool = True) -> str:
    market = str(event.get("market", ""))
    coin = str(event.get("coin", ""))
    side = str(event.get("side", ""))
    wall_price = str(event.get("wall_price", ""))
    if include_touch_price:
        touch_price = str(event.get("touch_price", ""))
        return f"{market}|{coin}|{side}|{wall_price}|{touch_price}"
    return f"{market}|{coin}|{side}|{wall_price}"


def wilson_lower_bound(successes: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = successes / total
    den = 1 + z * z / total
    center = p + z * z / (2 * total)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total)
    return (center - margin) / den


def build_cases(events: list[dict[str, Any]]) -> list[ResolvedCase]:
    touched_by_key: dict[str, dict[str, Any]] = {}
    touched_fallback: dict[str, dict[str, Any]] = {}

    for event in events:
        if str(event.get("event", "")) != "touched":
            continue
        k = key(event, include_touch_price=True)
        touched_by_key[k] = event
        touched_fallback[key(event, include_touch_price=False)] = event

    cases: list[ResolvedCase] = []
    for event in events:
        kind = str(event.get("event", ""))
        if kind not in {"bounced", "failed_breakout", "failed_breakdown", "expired"}:
            continue

        t = touched_by_key.get(key(event, include_touch_price=True))
        if t is None:
            t = touched_fallback.get(key(event, include_touch_price=False))
        if t is None:
            continue

        touched_ts = to_float(t.get("ts"))
        resolved_ts = to_float(event.get("ts"))
        lifetime = max(0.0, resolved_ts - touched_ts)

        outcome = "bounced" if kind == "bounced" else "failed"
        if kind == "expired":
            outcome = "expired"

        cases.append(
            ResolvedCase(
                market=str(t.get("market", "unknown")),
                coin=str(t.get("coin", "")),
                side=str(t.get("side", "")),
                wall_ratio=to_float(t.get("wall_ratio")),
                wall_notional_usd=to_float(t.get("wall_notional_usd")),
                day_volume_usd=to_float(t.get("day_volume_usd")),
                outcome=outcome,
                touched_ts=touched_ts,
                resolved_ts=resolved_ts,
                lifetime_sec=lifetime,
            )
        )
    return cases


def make_rule_grid() -> list[dict[str, float | str]]:
    markets = ["all", "perp", "spot"]
    min_ratio = [3, 5, 8, 10, 13, 20]
    min_notional = [10_000, 25_000, 50_000, 100_000, 250_000]
    min_day_volume = [0, 50_000, 200_000, 1_000_000, 5_000_000]

    grid: list[dict[str, float | str]] = []
    for market in markets:
        for r in min_ratio:
            for n in min_notional:
                for v in min_day_volume:
                    grid.append(
                        {
                            "market": market,
                            "min_ratio": float(r),
                            "min_notional_usd": float(n),
                            "min_day_volume_usd": float(v),
                        }
                    )
    return grid


def apply_rule(cases: list[ResolvedCase], rule: dict[str, float | str]) -> list[ResolvedCase]:
    market = str(rule["market"])
    min_ratio = float(rule["min_ratio"])
    min_notional = float(rule["min_notional_usd"])
    min_day_volume = float(rule["min_day_volume_usd"])

    out: list[ResolvedCase] = []
    for c in cases:
        if market != "all" and c.market != market:
            continue
        if c.wall_ratio < min_ratio:
            continue
        if c.wall_notional_usd < min_notional:
            continue
        if c.day_volume_usd < min_day_volume:
            continue
        out.append(c)
    return out


def pick_profiles(rules_scored: list[dict[str, Any]]) -> dict[str, Any]:
    strict_pool = [r for r in rules_scored if r["resolved"] >= 80]
    balanced_pool = [r for r in rules_scored if r["resolved"] >= 300]
    flow_pool = [r for r in rules_scored if r["resolved"] >= 600]

    strict = strict_pool[0] if strict_pool else None
    balanced = max(balanced_pool, key=lambda r: (r["bounced"], r["win_rate"])) if balanced_pool else None
    flow = max(flow_pool, key=lambda r: (r["bounced"], r["wilson_lb"])) if flow_pool else None

    return {
        "strict": strict,
        "balanced": balanced,
        "flow": flow,
    }


def summarize_cases(cases: list[ResolvedCase]) -> dict[str, Any]:
    resolved = [c for c in cases if c.outcome in {"bounced", "failed"}]
    bounced = [c for c in resolved if c.outcome == "bounced"]
    expired = [c for c in cases if c.outcome == "expired"]
    by_market: dict[str, dict[str, int]] = {}
    for market in ("perp", "spot"):
        m = [c for c in resolved if c.market == market]
        w = [c for c in m if c.outcome == "bounced"]
        by_market[market] = {"resolved": len(m), "bounced": len(w)}

    return {
        "cases_total": len(cases),
        "resolved_total": len(resolved),
        "bounced_total": len(bounced),
        "expired_total": len(expired),
        "win_rate_pct": (len(bounced) / len(resolved) * 100.0) if resolved else 0.0,
        "by_market": by_market,
    }


def rule_to_human(rule: dict[str, Any]) -> str:
    return (
        f"market={rule['market']} ratio>={rule['min_ratio']:.0f} "
        f"wall>={rule['min_notional_usd']:.0f} dayVol>={rule['min_day_volume_usd']:.0f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Derive robust quality rules from collector logs")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help='Example: "cloud_data/events_*.jsonl"')
    parser.add_argument("--out-json", default="data/quality_rules.json")
    parser.add_argument("--min-resolved", type=int, default=30)
    args = parser.parse_args()

    if args.log_glob:
        events, matched = load_from_glob(args.log_glob)
        source = f"{args.log_glob} ({len(matched)} files)"
    else:
        events = load_jsonl(Path(args.log_path))
        source = args.log_path

    if not events:
        raise SystemExit("No events found.")

    cases = build_cases(events)
    if not cases:
        raise SystemExit("No resolved/touched matched cases found yet.")

    resolved = [c for c in cases if c.outcome in {"bounced", "failed"}]
    if not resolved:
        raise SystemExit("No resolved bounced/failed cases yet.")

    scored: list[dict[str, Any]] = []
    for rule in make_rule_grid():
        subset = apply_rule(resolved, rule)
        n = len(subset)
        if n < args.min_resolved:
            continue
        wins = sum(1 for c in subset if c.outcome == "bounced")
        wr = wins / n
        lb = wilson_lower_bound(wins, n)
        avg_life = sum(c.lifetime_sec for c in subset) / n
        row = {
            **rule,
            "resolved": n,
            "bounced": wins,
            "win_rate": wr,
            "win_rate_pct": wr * 100.0,
            "wilson_lb": lb,
            "wilson_lb_pct": lb * 100.0,
            "avg_lifetime_sec": avg_life,
        }
        scored.append(row)

    if not scored:
        raise SystemExit("No rule bucket met minimum resolved threshold.")

    scored.sort(key=lambda r: (r["wilson_lb"], r["resolved"]), reverse=True)
    profiles = pick_profiles(scored)
    overall = summarize_cases(cases)

    output = {
        "source": source,
        "events_loaded": len(events),
        "overall": overall,
        "profiles": profiles,
        "top_rules": scored[:60],
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Loaded events: {len(events)} from {source}")
    print("Overall:")
    print(
        f"  resolved={overall['resolved_total']} bounced={overall['bounced_total']} "
        f"win_rate={overall['win_rate_pct']:.2f}%"
    )
    print("Profiles:")
    for name in ("strict", "balanced", "flow"):
        p = profiles.get(name)
        if not p:
            print(f"  {name}: not enough data")
            continue
        print(
            f"  {name}: {rule_to_human(p)} | resolved={p['resolved']} "
            f"wr={p['win_rate_pct']:.2f}% lb={p['wilson_lb_pct']:.2f}%"
        )

    print("\nTop 10 by Wilson lower bound:")
    for row in scored[:10]:
        print(
            f"  {rule_to_human(row)} | resolved={row['resolved']} bounced={row['bounced']} "
            f"wr={row['win_rate_pct']:.2f}% lb={row['wilson_lb_pct']:.2f}%"
        )
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
