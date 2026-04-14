from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional


@dataclass
class ResolvedFeatureCase:
    market: str
    coin: str
    side: str
    outcome: str  # bounced | failed
    seen_count: int
    visible_age_sec: float
    dominance_ratio: float
    stability_ratio: float
    current_vs_peak_ratio: float
    wall_level_index: int
    wall_distance_pct: float
    wall_ratio: float
    wall_notional_usd: float
    day_volume_usd: float
    wall_to_hour_volume_pct: float
    round_level_score: float
    wall_notional_volatility_ratio: float
    pre_touch_decay_ratio: float
    rebuild_count: int


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
    return rows


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
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
        matched = sorted(Path(".").glob(log_glob))

        def _iter() -> Iterator[dict[str, Any]]:
            for path in matched:
                if path.is_file():
                    yield from iter_jsonl(path)

        return _iter(), f"{log_glob} ({len(matched)} files)"

    return iter_jsonl(Path(log_path)), log_path


def base_key(event: dict[str, Any]) -> str:
    cid = str(event.get("candidate_id", "")).strip()
    if cid:
        return cid
    return (
        f"{event.get('market', '')}|{event.get('coin', '')}|"
        f"{event.get('side', '')}|{event.get('wall_price', '')}"
    )


def wilson_lower_bound(successes: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = successes / total
    den = 1 + z * z / total
    center = p + z * z / (2 * total)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total)
    return (center - margin) / den


def build_resolved_feature_cases(events: Iterable[dict[str, Any]]) -> list[ResolvedFeatureCase]:
    touched_queues: dict[str, list[dict[str, Any]]] = {}
    out: list[ResolvedFeatureCase] = []

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
            for i, t in enumerate(queue):
                if str(t.get("touch_price", "")) == touch_price:
                    idx = i
                    break

        touched = queue.pop(idx)
        if not queue:
            touched_queues.pop(key, None)

        side = str(touched.get("side", "")).lower()
        if side not in {"bid", "ask"}:
            continue

        outcome = "bounced" if kind == "bounced" else "failed"
        wall_notional_usd = to_float(touched.get("wall_notional_usd"), 0.0)
        day_volume_usd = to_float(touched.get("day_volume_usd"), 0.0)
        wall_to_hour_volume_pct = to_float(touched.get("wall_to_hour_volume_pct"), -1.0)
        if wall_to_hour_volume_pct < 0:
            if day_volume_usd > 0:
                hour_volume_usd_est = day_volume_usd / 24.0
                if hour_volume_usd_est > 0:
                    wall_to_hour_volume_pct = (wall_notional_usd / hour_volume_usd_est) * 100.0
                else:
                    wall_to_hour_volume_pct = 0.0
            else:
                wall_to_hour_volume_pct = 0.0
        out.append(
            ResolvedFeatureCase(
                market=str(touched.get("market", "perp")).lower(),
                coin=str(touched.get("coin", "")),
                side=side,
                outcome=outcome,
                seen_count=int(to_float(touched.get("seen_count"), 1.0)),
                visible_age_sec=to_float(touched.get("visible_age_sec"), 0.0),
                dominance_ratio=to_float(touched.get("wall_dominance_ratio"), 1.0),
                stability_ratio=to_float(touched.get("wall_notional_stability_ratio"), 0.0),
                current_vs_peak_ratio=to_float(touched.get("wall_notional_current_vs_peak_ratio"), 1.0),
                wall_level_index=int(to_float(touched.get("wall_level_index"), 0.0)),
                wall_distance_pct=to_float(touched.get("wall_distance_from_spread_pct"), 3.0),
                wall_ratio=to_float(touched.get("wall_ratio"), 0.0),
                wall_notional_usd=wall_notional_usd,
                day_volume_usd=day_volume_usd,
                wall_to_hour_volume_pct=wall_to_hour_volume_pct,
                round_level_score=to_float(touched.get("round_level_score"), 0.0),
                wall_notional_volatility_ratio=to_float(touched.get("wall_notional_volatility_ratio"), 0.0),
                pre_touch_decay_ratio=to_float(touched.get("pre_touch_decay_ratio"), 1.0),
                rebuild_count=int(to_float(touched.get("rebuild_count"), 0.0)),
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


def passes_filter(case: ResolvedFeatureCase, f: dict[str, float]) -> bool:
    if case.seen_count < int(f["min_seen_count"]):
        return False
    if case.visible_age_sec < float(f["min_visible_age_sec"]):
        return False
    if case.dominance_ratio < float(f["min_dominance_ratio"]):
        return False
    if case.stability_ratio < float(f["min_stability_ratio"]):
        return False
    if case.current_vs_peak_ratio < float(f["min_current_vs_peak_ratio"]):
        return False
    if case.wall_level_index > int(f["max_wall_level_index"]):
        return False
    if case.wall_distance_pct > float(f["max_wall_distance_pct"]):
        return False
    if case.wall_ratio < float(f["min_wall_ratio"]):
        return False
    if case.wall_notional_usd < float(f["min_wall_notional_usd"]):
        return False
    if case.round_level_score < float(f["min_round_level_score"]):
        return False
    wall_to_day_volume_bps = 0.0
    if case.day_volume_usd > 0:
        wall_to_day_volume_bps = (case.wall_notional_usd / case.day_volume_usd) * 10_000.0
    if wall_to_day_volume_bps < float(f["min_wall_to_day_volume_bps"]):
        return False
    if case.wall_to_hour_volume_pct < float(f.get("min_wall_to_hour_volume_pct", 0.0)):
        return False
    if case.wall_notional_volatility_ratio > float(f.get("max_wall_notional_volatility_ratio", 10.0)):
        return False
    if case.pre_touch_decay_ratio < float(f.get("min_pre_touch_decay_ratio", 0.0)):
        return False
    if case.rebuild_count > int(f.get("max_rebuild_count", 999999)):
        return False
    return True


def score_filters(
    cases: list[ResolvedFeatureCase],
    *,
    min_resolved: int,
    min_unique_coins: int,
    max_top_coin_share_pct: float,
    grid_mode: str = "fast",
) -> list[dict[str, Any]]:
    if grid_mode == "full":
        min_seen_options = [2, 3, 4]
        min_visible_age_options = [12.0, 20.0, 30.0, 45.0]
        min_dominance_options = [1.4, 1.6, 1.8, 2.2]
        min_stability_options = [0.35, 0.45, 0.55, 0.7]
        min_current_vs_peak_options = [0.5, 0.6, 0.7, 0.8]
        max_level_index_options = [24, 40, 60, 80]
        max_distance_options = [1.5, 2.0, 2.5, 3.0]
        min_wall_to_day_volume_bps_options = [0.0, 0.35, 0.7]
        min_wall_to_hour_volume_pct_options = [0.0, 0.08, 0.16, 0.24, 0.36]
        min_round_level_score_options = [0.0, 0.15, 0.45]
        max_volatility_ratio_options = [0.55, 0.70, 0.90, 1.20]
        min_pre_touch_decay_ratio_options = [0.55, 0.62, 0.72]
        max_rebuild_count_options = [2, 3, 4]
    else:
        # Fast mode keeps broad behavior coverage, but avoids multi-hour brute force.
        min_seen_options = [2, 3]
        min_visible_age_options = [12.0, 30.0]
        min_dominance_options = [1.4, 1.8]
        min_stability_options = [0.35, 0.55]
        min_current_vs_peak_options = [0.5, 0.7]
        max_level_index_options = [40, 80]
        max_distance_options = [1.5, 2.5]
        min_wall_to_day_volume_bps_options = [0.0, 0.7]
        min_wall_to_hour_volume_pct_options = [0.08, 0.24]
        min_round_level_score_options = [0.0, 0.45]
        max_volatility_ratio_options = [0.70, 1.20]
        min_pre_touch_decay_ratio_options = [0.55, 0.72]
        max_rebuild_count_options = [2, 4]

    fixed_min_wall_ratio = 3.0
    fixed_min_wall_notional_usd = 20_000.0

    scored: list[dict[str, Any]] = []

    for min_seen in min_seen_options:
        for min_age in min_visible_age_options:
            for min_dom in min_dominance_options:
                for min_stab in min_stability_options:
                    for min_cvp in min_current_vs_peak_options:
                        for max_lvl in max_level_index_options:
                            for max_dist in max_distance_options:
                                for min_wall_dv_bps in min_wall_to_day_volume_bps_options:
                                    for min_wall_hv_pct in min_wall_to_hour_volume_pct_options:
                                        for min_round_score in min_round_level_score_options:
                                            for max_vol in max_volatility_ratio_options:
                                                for min_decay in min_pre_touch_decay_ratio_options:
                                                    for max_rebuild in max_rebuild_count_options:
                                                        f = {
                                                            "min_seen_count": int(min_seen),
                                                            "min_visible_age_sec": float(min_age),
                                                            "min_dominance_ratio": float(min_dom),
                                                            "min_stability_ratio": float(min_stab),
                                                            "min_current_vs_peak_ratio": float(min_cvp),
                                                            "max_wall_level_index": int(max_lvl),
                                                            "max_wall_distance_pct": float(max_dist),
                                                            "min_wall_ratio": float(fixed_min_wall_ratio),
                                                            "min_wall_notional_usd": float(fixed_min_wall_notional_usd),
                                                            "min_wall_to_day_volume_bps": float(min_wall_dv_bps),
                                                            "min_wall_to_hour_volume_pct": float(min_wall_hv_pct),
                                                            "min_round_level_score": float(min_round_score),
                                                            "max_wall_notional_volatility_ratio": float(max_vol),
                                                            "min_pre_touch_decay_ratio": float(min_decay),
                                                            "max_rebuild_count": int(max_rebuild),
                                                        }
                                                        subset = [c for c in cases if passes_filter(c, f)]
                                                        n = len(subset)
                                                        if n < min_resolved:
                                                            continue

                                                        wins = sum(1 for c in subset if c.outcome == "bounced")
                                                        lb = wilson_lower_bound(wins, n)
                                                        wr = wins / n
                                                        coin_counts = Counter(c.coin for c in subset)
                                                        unique_coins = len(coin_counts)
                                                        top_coin_share_pct = (
                                                            coin_counts.most_common(1)[0][1] / n * 100.0
                                                            if coin_counts
                                                            else 0.0
                                                        )
                                                        if unique_coins < min_unique_coins:
                                                            continue
                                                        if top_coin_share_pct > max_top_coin_share_pct:
                                                            continue

                                                        score = lb * math.log1p(n)
                                                        scored.append(
                                                            {
                                                                **f,
                                                                "resolved": n,
                                                                "bounced": wins,
                                                                "win_rate_pct": wr * 100.0,
                                                                "wilson_lb_pct": lb * 100.0,
                                                                "unique_coins": unique_coins,
                                                                "top_coin_share_pct": top_coin_share_pct,
                                                                "score": score,
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


def summarize(cases: list[ResolvedFeatureCase]) -> dict[str, Any]:
    total = len(cases)
    bounced = sum(1 for c in cases if c.outcome == "bounced")
    wr = (bounced / total * 100.0) if total else 0.0
    by_side: dict[str, dict[str, Any]] = {}
    for side in ("bid", "ask"):
        subset = [c for c in cases if c.side == side]
        wins = sum(1 for c in subset if c.outcome == "bounced")
        cnt = len(subset)
        by_side[side] = {
            "total": cnt,
            "bounced": wins,
            "win_rate_pct": (wins / cnt * 100.0) if cnt else 0.0,
        }
    return {
        "resolved_total": total,
        "bounced_total": bounced,
        "win_rate_pct": wr,
        "by_side": by_side,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Derive anti-fake ironclad filters from real online outcomes")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help='Example: "cloud_data/events_*.jsonl"')
    parser.add_argument("--window-hours", type=int, default=0, help="0 = all history")
    parser.add_argument("--min-resolved", type=int, default=80)
    parser.add_argument("--min-unique-coins", type=int, default=10)
    parser.add_argument("--max-top-coin-share-pct", type=float, default=45.0)
    parser.add_argument(
        "--grid-mode",
        choices=("fast", "full"),
        default="fast",
        help="fast = quick recalculation, full = exhaustive (very slow)",
    )
    parser.add_argument("--out-json", default="data/ironclad_filters.json")
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

    cases = build_resolved_feature_cases(filtered_events())
    if not cases:
        raise SystemExit("No resolved touched->outcome cases found.")

    baseline_filter = {
        "min_seen_count": 2,
        "min_visible_age_sec": 12.0,
        "min_dominance_ratio": 1.55,
        "min_stability_ratio": 0.42,
        "min_current_vs_peak_ratio": 0.55,
        "max_wall_level_index": 65,
        "max_wall_distance_pct": 3.0,
        "min_wall_ratio": 3.0,
        "min_wall_notional_usd": 20_000.0,
        "min_wall_to_day_volume_bps": 0.35,
        "min_wall_to_hour_volume_pct": 0.08,
        "min_round_level_score": 0.0,
        "max_wall_notional_volatility_ratio": 0.70,
        "min_pre_touch_decay_ratio": 0.62,
        "max_rebuild_count": 3,
    }
    baseline_subset = [c for c in cases if passes_filter(c, baseline_filter)]
    scored = score_filters(
        cases,
        min_resolved=args.min_resolved,
        min_unique_coins=args.min_unique_coins,
        max_top_coin_share_pct=args.max_top_coin_share_pct,
        grid_mode=args.grid_mode,
    )
    selected = scored[0] if scored else None

    result = {
        "source": source,
        "generated_at_ts": time.time(),
        "window_hours": args.window_hours,
        "grid_mode": args.grid_mode,
        "events_loaded": events_loaded,
        "overall": summarize(cases),
        "baseline_filter": {
            **baseline_filter,
            **summarize(baseline_subset),
        },
        "selected_filter": selected,
        "top_filters": scored[:100],
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Loaded events: {events_loaded} from {source}")
    print(
        f"Overall resolved={result['overall']['resolved_total']} "
        f"win_rate={result['overall']['win_rate_pct']:.2f}%"
    )
    print(
        f"Baseline resolved={result['baseline_filter']['resolved_total']} "
        f"win_rate={result['baseline_filter']['win_rate_pct']:.2f}%"
    )
    if selected:
        print(
            f"Selected resolved={selected['resolved']} win_rate={selected['win_rate_pct']:.2f}% "
            f"lb={selected['wilson_lb_pct']:.2f}% coins={selected['unique_coins']} "
            f"top_coin_share={selected['top_coin_share_pct']:.1f}%"
        )
    else:
        print("Selected filter: no candidate met constraints.")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
