from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional


@dataclass
class ResolvedCase:
    symbol_key: str
    market: str
    coin: str
    outcome: str  # bounced | failed
    seen_count: int
    visible_age_sec: float
    wall_ratio: float
    wall_notional_usd: float
    day_volume_usd: float
    wall_dominance_ratio: float
    wall_notional_stability_ratio: float
    wall_notional_current_vs_peak_ratio: float
    wall_level_index: int
    wall_distance_from_spread_pct: float
    wall_to_day_volume_bps: float
    wall_to_hour_volume_pct: float
    round_level_score: float
    wall_notional_volatility_ratio: float
    pre_touch_decay_ratio: float
    rebuild_count: int
    resolved_ts: float


def build_suppress_filter(
    rows: list[ResolvedCase],
    *,
    min_wall_usd_floor: float,
) -> dict[str, Any]:
    resolved = len(rows)
    bounced = sum(1 for r in rows if r.outcome == "bounced")
    failed = sum(1 for r in rows if r.outcome == "failed")
    win_rate_pct = (bounced / resolved * 100.0) if resolved else 0.0
    last_ts = max((r.resolved_ts for r in rows), default=0.0)

    return {
        "min_seen_count": 4,
        "min_visible_age_sec": 90.0,
        "min_dominance_ratio": 8.0,
        "min_stability_ratio": 0.97,
        "min_current_vs_peak_ratio": 0.97,
        "max_wall_level_index": 10,
        "max_wall_distance_pct": 0.6,
        "min_wall_ratio": 35.0,
        "min_wall_notional_usd": max(min_wall_usd_floor, 50_000.0),
        "min_wall_to_day_volume_bps": 20.0,
        "min_wall_to_hour_volume_pct": 2.0,
        "min_round_level_score": 0.0,
        "max_wall_notional_volatility_ratio": 0.35,
        "min_pre_touch_decay_ratio": 0.90,
        "max_rebuild_count": 1,
        "resolved": resolved,
        "bounced": bounced,
        "failed": failed,
        "win_rate_pct": win_rate_pct,
        "last_resolved_ts": last_ts,
        "suppress_symbol": True,
    }


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def quantile(values: list[float], p: float, default: float) -> float:
    if not values:
        return default
    data = sorted(values)
    idx = (len(data) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(data) - 1)
    frac = idx - lo
    return data[lo] * (1.0 - frac) + data[hi] * frac


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
    return rows


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                yield item


def iter_events(log_path: str, log_glob: str) -> tuple[Iterable[dict[str, Any]], str]:
    if log_glob:
        paths = sorted(Path(".").glob(log_glob))

        def _iter() -> Iterator[dict[str, Any]]:
            for p in paths:
                if p.is_file():
                    yield from iter_jsonl(p)

        return _iter(), f"{log_glob} ({len(paths)} files)"

    return iter_jsonl(Path(log_path)), log_path


def base_key(event: dict[str, Any]) -> str:
    cid = str(event.get("candidate_id", "")).strip()
    if cid:
        return cid
    return (
        f"{event.get('market', '')}|{event.get('coin', '')}|"
        f"{event.get('side', '')}|{event.get('wall_price', '')}"
    )


def build_resolved_cases(events: Iterable[dict[str, Any]]) -> list[ResolvedCase]:
    touched_queues: dict[str, list[dict[str, Any]]] = {}
    out: list[ResolvedCase] = []

    for event in events:
        kind = str(event.get("event", ""))
        key = base_key(event)

        if kind == "touched":
            touched_queues.setdefault(key, []).append(event)
            continue

        if kind not in {"bounced", "failed_breakout", "failed_breakdown"}:
            continue

        queue = touched_queues.get(key)
        if not queue:
            continue

        touch_price = str(event.get("touch_price", ""))
        idx = 0
        if touch_price:
            for i, touched in enumerate(queue):
                if str(touched.get("touch_price", "")) == touch_price:
                    idx = i
                    break

        touched = queue.pop(idx)
        if not queue:
            touched_queues.pop(key, None)

        market = str(touched.get("market", "")).lower()
        coin = str(touched.get("coin", ""))
        if not market or not coin:
            continue

        wall_notional_usd = to_float(touched.get("wall_notional_usd"), 0.0)
        day_volume_usd = to_float(touched.get("day_volume_usd"), 0.0)
        wall_to_day_volume_bps = 0.0
        if day_volume_usd > 0:
            wall_to_day_volume_bps = (wall_notional_usd / day_volume_usd) * 10_000.0

        wall_to_hour_volume_pct = to_float(touched.get("wall_to_hour_volume_pct"), -1.0)
        if wall_to_hour_volume_pct < 0:
            if day_volume_usd > 0:
                hour_volume_usd = day_volume_usd / 24.0
                if hour_volume_usd > 0:
                    wall_to_hour_volume_pct = (wall_notional_usd / hour_volume_usd) * 100.0
                else:
                    wall_to_hour_volume_pct = 0.0
            else:
                wall_to_hour_volume_pct = 0.0

        out.append(
            ResolvedCase(
                symbol_key=f"{market}|{coin}",
                market=market,
                coin=coin,
                outcome="bounced" if kind == "bounced" else "failed",
                seen_count=max(1, int(to_float(touched.get("seen_count"), 1.0))),
                visible_age_sec=to_float(touched.get("visible_age_sec"), 0.0),
                wall_ratio=to_float(touched.get("wall_ratio"), 0.0),
                wall_notional_usd=wall_notional_usd,
                day_volume_usd=day_volume_usd,
                wall_dominance_ratio=to_float(touched.get("wall_dominance_ratio"), 1.0),
                wall_notional_stability_ratio=to_float(touched.get("wall_notional_stability_ratio"), 0.0),
                wall_notional_current_vs_peak_ratio=to_float(
                    touched.get("wall_notional_current_vs_peak_ratio"), 1.0
                ),
                wall_level_index=int(to_float(touched.get("wall_level_index"), 0.0)),
                wall_distance_from_spread_pct=to_float(touched.get("wall_distance_from_spread_pct"), 0.0),
                wall_to_day_volume_bps=wall_to_day_volume_bps,
                wall_to_hour_volume_pct=wall_to_hour_volume_pct,
                round_level_score=to_float(touched.get("round_level_score"), 0.0),
                wall_notional_volatility_ratio=to_float(touched.get("wall_notional_volatility_ratio"), 0.0),
                pre_touch_decay_ratio=to_float(touched.get("pre_touch_decay_ratio"), 1.0),
                rebuild_count=int(to_float(touched.get("rebuild_count"), 0.0)),
                resolved_ts=to_float(event.get("ts"), 0.0),
            )
        )

    return out


def max_ts_from_events(events: Iterable[dict[str, Any]]) -> float:
    max_ts = 0.0
    for event in events:
        ts = to_float(event.get("ts"))
        if ts > max_ts:
            max_ts = ts
    return max_ts


def derive_symbol_filter(
    rows: list[ResolvedCase],
    *,
    min_failed_for_contrast: int,
    min_wall_usd_floor: float,
) -> dict[str, Any]:
    bounced_rows = [r for r in rows if r.outcome == "bounced"]
    failed_rows = [r for r in rows if r.outcome == "failed"]

    b_notional = [r.wall_notional_usd for r in bounced_rows]
    f_notional = [r.wall_notional_usd for r in failed_rows]
    b_ratio = [r.wall_ratio for r in bounced_rows]
    f_ratio = [r.wall_ratio for r in failed_rows]
    b_dom = [r.wall_dominance_ratio for r in bounced_rows]
    f_dom = [r.wall_dominance_ratio for r in failed_rows]
    b_stab = [r.wall_notional_stability_ratio for r in bounced_rows]
    f_stab = [r.wall_notional_stability_ratio for r in failed_rows]
    b_cvp = [r.wall_notional_current_vs_peak_ratio for r in bounced_rows]
    b_seen = [float(r.seen_count) for r in bounced_rows]
    b_age = [r.visible_age_sec for r in bounced_rows]
    b_day = [r.wall_to_day_volume_bps for r in bounced_rows]
    f_day = [r.wall_to_day_volume_bps for r in failed_rows]
    b_hour = [r.wall_to_hour_volume_pct for r in bounced_rows]
    f_hour = [r.wall_to_hour_volume_pct for r in failed_rows]
    b_lvl = [float(r.wall_level_index) for r in bounced_rows]
    b_dist = [r.wall_distance_from_spread_pct for r in bounced_rows]
    b_round = [r.round_level_score for r in bounced_rows]
    b_volatility = [r.wall_notional_volatility_ratio for r in bounced_rows]
    f_volatility = [r.wall_notional_volatility_ratio for r in failed_rows]
    b_decay = [r.pre_touch_decay_ratio for r in bounced_rows]
    f_decay = [r.pre_touch_decay_ratio for r in failed_rows]
    b_rebuild = [float(r.rebuild_count) for r in bounced_rows]

    min_wall_notional_usd = max(
        min_wall_usd_floor,
        quantile(b_notional, 0.30, min_wall_usd_floor),
    )
    if len(f_notional) >= min_failed_for_contrast:
        min_wall_notional_usd = max(min_wall_notional_usd, quantile(f_notional, 0.80, min_wall_usd_floor))
    min_wall_notional_usd = max(
        min_wall_usd_floor,
        min(min_wall_notional_usd, quantile(b_notional, 0.90, min_wall_notional_usd)),
    )

    min_wall_ratio = max(3.0, quantile(b_ratio, 0.25, 3.0))
    if len(f_ratio) >= min_failed_for_contrast:
        min_wall_ratio = max(min_wall_ratio, quantile(f_ratio, 0.75, 0.0))

    min_dominance_ratio = max(1.6, quantile(b_dom, 0.25, 1.6))
    if len(f_dom) >= min_failed_for_contrast:
        min_dominance_ratio = max(min_dominance_ratio, quantile(f_dom, 0.75, 0.0))

    min_stability_ratio = max(0.5, quantile(b_stab, 0.25, 0.5))
    if len(f_stab) >= min_failed_for_contrast:
        min_stability_ratio = max(min_stability_ratio, quantile(f_stab, 0.75, 0.0))

    min_current_vs_peak_ratio = min(0.95, max(0.55, quantile(b_cvp, 0.25, 0.55)))
    min_visible_age_sec = max(12.0, quantile(b_age, 0.25, 12.0))
    min_seen_count = max(2, int(round(quantile(b_seen, 0.25, 2.0))))
    max_wall_level_index = min(35, max(8, int(round(quantile(b_lvl, 0.85, 35.0)))))
    max_wall_distance_pct = min(3.0, max(0.8, quantile(b_dist, 0.85, 3.0)))
    min_wall_to_day_volume_bps = max(0.3, quantile(b_day, 0.25, 0.3))
    if len(f_day) >= min_failed_for_contrast:
        min_wall_to_day_volume_bps = max(min_wall_to_day_volume_bps, quantile(f_day, 0.75, 0.0))
    min_wall_to_day_volume_bps = min(min_wall_to_day_volume_bps, 120.0)

    min_wall_to_hour_volume_pct = max(0.08, quantile(b_hour, 0.25, 0.08))
    if len(f_hour) >= min_failed_for_contrast:
        min_wall_to_hour_volume_pct = max(min_wall_to_hour_volume_pct, quantile(f_hour, 0.75, 0.0))
    min_wall_to_hour_volume_pct = min(min_wall_to_hour_volume_pct, 12.0)
    min_round_level_score = max(0.0, quantile(b_round, 0.25, 0.0))
    max_wall_notional_volatility_ratio = max(0.15, min(1.5, quantile(b_volatility, 0.80, 0.55)))
    if len(f_volatility) >= min_failed_for_contrast:
        max_wall_notional_volatility_ratio = min(
            max_wall_notional_volatility_ratio,
            max(0.15, quantile(f_volatility, 0.35, max_wall_notional_volatility_ratio)),
        )
    min_pre_touch_decay_ratio = max(0.55, min(0.98, quantile(b_decay, 0.25, 0.62)))
    if len(f_decay) >= min_failed_for_contrast:
        min_pre_touch_decay_ratio = max(min_pre_touch_decay_ratio, quantile(f_decay, 0.85, min_pre_touch_decay_ratio))
    max_rebuild_count = max(1, min(6, int(round(quantile(b_rebuild, 0.85, 3.0)))))

    resolved = len(rows)
    bounced = len(bounced_rows)
    win_rate_pct = (bounced / resolved * 100.0) if resolved else 0.0

    return {
        "min_seen_count": min_seen_count,
        "min_visible_age_sec": min_visible_age_sec,
        "min_dominance_ratio": min_dominance_ratio,
        "min_stability_ratio": min_stability_ratio,
        "min_current_vs_peak_ratio": min_current_vs_peak_ratio,
        "max_wall_level_index": max_wall_level_index,
        "max_wall_distance_pct": max_wall_distance_pct,
        "min_wall_ratio": min_wall_ratio,
        "min_wall_notional_usd": min_wall_notional_usd,
        "min_wall_to_day_volume_bps": min_wall_to_day_volume_bps,
        "min_wall_to_hour_volume_pct": min_wall_to_hour_volume_pct,
        "min_round_level_score": min_round_level_score,
        "max_wall_notional_volatility_ratio": max_wall_notional_volatility_ratio,
        "min_pre_touch_decay_ratio": min_pre_touch_decay_ratio,
        "max_rebuild_count": max_rebuild_count,
        "resolved": resolved,
        "bounced": bounced,
        "failed": len(failed_rows),
        "win_rate_pct": win_rate_pct,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Derive per-symbol ironclad thresholds from resolved outcomes")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help='Example: "cloud_data/events_*.jsonl"')
    parser.add_argument("--window-hours", type=int, default=72, help="0 = all history")
    parser.add_argument("--min-resolved-per-symbol", type=int, default=30)
    parser.add_argument("--min-bounced-per-symbol", type=int, default=8)
    parser.add_argument("--min-failed-for-contrast", type=int, default=6)
    parser.add_argument("--min-wall-usd-floor", type=float, default=20_000.0)
    parser.add_argument("--max-symbol-staleness-hours", type=int, default=24)
    parser.add_argument(
        "--suppress-if-winrate-lt-pct",
        type=float,
        default=8.0,
        help="Create suppress_symbol profile when resolved is enough but WR is below this threshold",
    )
    parser.add_argument(
        "--suppress-min-resolved",
        type=int,
        default=25,
        help="Minimum resolved cases to allow suppress_symbol profile",
    )
    parser.add_argument(
        "--suppress-min-failed",
        type=int,
        default=20,
        help="Minimum failed cases to allow suppress_symbol profile",
    )
    parser.add_argument("--out-json", default="data/symbol_ironclad_filters.json")
    args = parser.parse_args()

    def new_events_iter() -> Iterable[dict[str, Any]]:
        it, _ = iter_events(args.log_path, args.log_glob)
        return it

    _, source = iter_events(args.log_path, args.log_glob)

    cutoff: Optional[float] = None
    if args.window_hours > 0:
        max_ts = max_ts_from_events(new_events_iter())
        if max_ts <= 0:
            raise SystemExit("No events found.")
        cutoff = max_ts - args.window_hours * 3600

    events_loaded = 0

    def filtered_events() -> Iterator[dict[str, Any]]:
        nonlocal events_loaded
        for event in new_events_iter():
            if cutoff is not None and to_float(event.get("ts")) < cutoff:
                continue
            events_loaded += 1
            yield event

    cases = build_resolved_cases(filtered_events())
    if not cases:
        raise SystemExit("No resolved touched->outcome cases found.")

    by_symbol: dict[str, list[ResolvedCase]] = defaultdict(list)
    for c in cases:
        by_symbol[c.symbol_key].append(c)

    symbols: dict[str, dict[str, Any]] = {}
    for symbol_key, rows in by_symbol.items():
        resolved = len(rows)
        bounced = sum(1 for r in rows if r.outcome == "bounced")
        failed = resolved - bounced
        win_rate_pct = (bounced / resolved * 100.0) if resolved else 0.0
        last_ts = max((r.resolved_ts for r in rows), default=0.0)
        if args.max_symbol_staleness_hours > 0:
            if (time.time() - last_ts) > (args.max_symbol_staleness_hours * 3600):
                continue
        if resolved < args.min_resolved_per_symbol:
            continue
        should_suppress = (
            resolved >= args.suppress_min_resolved
            and failed >= args.suppress_min_failed
            and bounced == 0
            and win_rate_pct < args.suppress_if_winrate_lt_pct
        )
        if should_suppress:
            symbols[symbol_key] = build_suppress_filter(
                rows,
                min_wall_usd_floor=args.min_wall_usd_floor,
            )
            continue
        if bounced < args.min_bounced_per_symbol:
            continue
        item = derive_symbol_filter(
            rows,
            min_failed_for_contrast=args.min_failed_for_contrast,
            min_wall_usd_floor=args.min_wall_usd_floor,
        )
        item["last_resolved_ts"] = last_ts
        symbols[symbol_key] = item

    result = {
        "source": source,
        "generated_at_ts": time.time(),
        "window_hours": args.window_hours,
        "events_loaded": events_loaded,
        "resolved_cases": len(cases),
        "min_resolved_per_symbol": args.min_resolved_per_symbol,
        "min_bounced_per_symbol": args.min_bounced_per_symbol,
        "min_wall_usd_floor": args.min_wall_usd_floor,
        "max_symbol_staleness_hours": args.max_symbol_staleness_hours,
        "symbols": symbols,
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Loaded events: {events_loaded} from {source}")
    print(f"Resolved cases: {len(cases)}")
    print(f"Derived symbol filters: {len(symbols)}")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
