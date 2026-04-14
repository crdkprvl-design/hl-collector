from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

INF_SENTINEL = 1e30


@dataclass
class ResolvedCase:
    candidate_id: str
    market: str
    coin: str
    side: str
    wall_ratio: float
    wall_dominance_ratio: float
    wall_notional_usd: float
    wall_distance_from_spread_pct: float
    day_volume_usd: float
    outcome: str  # bounced | failed | expired
    touched_ts: float
    resolved_ts: float
    lifetime_sec: float
    visible_age_sec: float = 0.0
    seen_count: int = 0
    wall_notional_peak_usd: float = 0.0
    wall_notional_floor_usd: float = 0.0
    wall_notional_mean_usd: float = 0.0
    wall_notional_stability_ratio: float = 0.0
    round_level_score: float = 0.0
    wall_notional_volatility_ratio: float = 0.0
    distance_volatility_ratio: float = 0.0
    pre_touch_decay_ratio: float = 1.0
    distance_compression_ratio: float = 1.0
    rebuild_count: int = 0
    missing_episodes: int = 0
    touch_survival_sec: float = 0.0
    favorable_move_pct: float = 0.0
    loss_proxy_pct: float = 0.0
    touch_mfe_pct: float = 0.0
    touch_mae_pct: float = 0.0
    time_to_outcome_sec: float = 0.0
    touch_attempt_count: int = 0
    updates_before_touch: int = 0
    approach_speed_pct_per_sec: float = 0.0
    entry_score: float = 0.0
    liquidity_bucket: str = "trash"


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


def base_key(event: dict[str, Any]) -> str:
    cid = str(event.get("candidate_id", "")).strip()
    if cid:
        return cid
    market = str(event.get("market", ""))
    coin = str(event.get("coin", ""))
    side = str(event.get("side", ""))
    wall_price = str(event.get("wall_price", ""))
    return f"{market}|{coin}|{side}|{wall_price}"


def wilson_lower_bound(successes: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = successes / total
    den = 1 + z * z / total
    center = p + z * z / (2 * total)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total)
    return (center - margin) / den


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def compute_bounce_score_case(c: ResolvedCase) -> float:
    ratio_term = math.log1p(max(c.wall_ratio, 0.0))
    dom_term = math.log1p(max(c.wall_dominance_ratio, 0.0))
    visible_age_norm = clamp(c.visible_age_sec / 60.0, 0.0, 1.5)
    wall_to_hour_pct = 0.0
    if c.day_volume_usd > 0:
        hour_volume_usd = c.day_volume_usd / 24.0
        if hour_volume_usd > 0:
            wall_to_hour_pct = (c.wall_notional_usd / hour_volume_usd) * 100.0
    wall_to_hour_term = min(math.log1p(max(wall_to_hour_pct, 0.0)) / math.log1p(80.0), 1.5)
    near_spread_penalty = 0.0
    if c.wall_distance_from_spread_pct < 0.20:
        near_spread_penalty = clamp((0.20 - c.wall_distance_from_spread_pct) / 0.20, 0.0, 1.0)
    volatility_penalty = max(0.0, c.wall_notional_volatility_ratio)
    decay_penalty = max(0.0, 1.0 - c.pre_touch_decay_ratio)
    rebuild_penalty = min(max(0, c.rebuild_count) / 3.0, 1.5)
    compression_bonus = clamp(1.0 - abs(1.0 - c.distance_compression_ratio), 0.0, 1.0)
    approach_penalty = clamp(max(0.0, c.approach_speed_pct_per_sec) / 0.08, 0.0, 1.0)
    current_vs_peak = 0.0
    if c.wall_notional_peak_usd > 0:
        current_vs_peak = c.wall_notional_usd / c.wall_notional_peak_usd

    score = (
        1.15 * ratio_term
        + 0.95 * dom_term
        + 0.90 * wall_to_hour_term
        + 0.75 * max(0.0, c.wall_notional_stability_ratio)
        + 0.70 * max(0.0, current_vs_peak)
        + 0.55 * visible_age_norm
        + 0.30 * max(0.0, c.round_level_score)
        + 0.20 * compression_bonus
        - 1.10 * volatility_penalty
        - 1.35 * decay_penalty
        - 0.70 * rebuild_penalty
        - 0.65 * near_spread_penalty
        - 0.15 * approach_penalty
    )
    return score


def liquidity_bucket_for_volume(day_volume_usd: float) -> str:
    if day_volume_usd >= 25_000_000:
        return "majors"
    if day_volume_usd >= 2_500_000:
        return "mids"
    return "trash"


def summarize_trade_quality(subset: list[ResolvedCase]) -> dict[str, Any]:
    n = len(subset)
    if n == 0:
        return {
            "count": 0,
            "win_rate_pct": 0.0,
            "expectancy_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "avg_touch_mfe_pct": 0.0,
            "avg_touch_mae_pct": 0.0,
        }
    bounced = [c for c in subset if c.outcome == "bounced"]
    failed = [c for c in subset if c.outcome == "failed"]
    wins = len(bounced)
    wr = wins / n
    loss_rate = 1.0 - wr
    avg_win_pct = sum(max(0.0, c.favorable_move_pct) for c in bounced) / len(bounced) if bounced else 0.0
    avg_loss_pct = sum(max(0.0, c.loss_proxy_pct) for c in failed) / len(failed) if failed else 0.0
    expectancy_pct = (wr * avg_win_pct) - (loss_rate * avg_loss_pct)
    avg_touch_mfe_pct = sum(max(0.0, c.touch_mfe_pct) for c in subset) / n
    avg_touch_mae_pct = sum(max(0.0, c.touch_mae_pct) for c in subset) / n
    return {
        "count": n,
        "win_rate_pct": wr * 100.0,
        "expectancy_pct": expectancy_pct,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "avg_touch_mfe_pct": avg_touch_mfe_pct,
        "avg_touch_mae_pct": avg_touch_mae_pct,
    }


def build_ranking_eval(resolved: list[ResolvedCase]) -> dict[str, Any]:
    ranked: list[tuple[float, ResolvedCase]] = [(compute_bounce_score_case(c), c) for c in resolved]
    ranked.sort(key=lambda x: x[0], reverse=True)
    if not ranked:
        return {"deciles": [], "top_slices": []}

    deciles: list[dict[str, Any]] = []
    n = len(ranked)
    bucket = max(1, n // 10)
    for idx in range(10):
        lo = idx * bucket
        hi = n if idx == 9 else min(n, (idx + 1) * bucket)
        part = [c for _, c in ranked[lo:hi]]
        label = f"D{10 - idx}"
        summary = summarize_trade_quality(part)
        deciles.append({"label": label, **summary})

    top_slices: list[dict[str, Any]] = []
    for pct in (50, 20, 10, 5):
        k = max(1, int(n * (pct / 100.0)))
        part = [c for _, c in ranked[:k]]
        summary = summarize_trade_quality(part)
        top_slices.append({"slice": f"top_{pct}pct", **summary})

    by_bucket: list[dict[str, Any]] = []
    for bucket in ("majors", "mids", "trash"):
        subset = [c for c in resolved if c.liquidity_bucket == bucket]
        summary = summarize_trade_quality(subset)
        by_bucket.append({"bucket": bucket, **summary})

    return {"deciles": deciles, "top_slices": top_slices, "by_liquidity_bucket": by_bucket}


def build_cycle_top_eval(events: list[dict[str, Any]], resolved: list[ResolvedCase]) -> dict[str, Any]:
    pre_touch_snapshots = [
        e
        for e in events
        if str(e.get("event", "")).lower() == "selection_snapshot_pre_touch" and isinstance(e.get("selected"), list)
    ]
    snapshots = sorted(
        [
            e
            for e in events
            if str(e.get("event", "")).lower() == "selection_snapshot" and isinstance(e.get("selected"), list)
        ],
        key=lambda e: to_float(e.get("ts"), 0.0),
    )
    if pre_touch_snapshots:
        snapshots = sorted(pre_touch_snapshots, key=lambda e: to_float(e.get("ts"), 0.0))
    if not snapshots:
        return {"top_1_per_cycle": {"cycles": 0, **summarize_trade_quality([])}, "top_3_per_cycle": {"cycles": 0, **summarize_trade_quality([])}}

    cases_by_candidate: dict[str, list[ResolvedCase]] = {}
    for case in sorted(resolved, key=lambda c: c.touched_ts):
        if not case.candidate_id:
            continue
        cases_by_candidate.setdefault(case.candidate_id, []).append(case)

    consumed_keys: set[tuple[str, float]] = set()

    def pick_case(candidate_id: str, snapshot_ts: float) -> ResolvedCase | None:
        if not candidate_id:
            return None
        seq = cases_by_candidate.get(candidate_id, [])
        if not seq:
            return None
        for case in seq:
            case_key = (case.candidate_id, case.touched_ts)
            if case_key in consumed_keys:
                continue
            if case.touched_ts < snapshot_ts - 1.0:
                continue
            if case.touched_ts > snapshot_ts + 1800.0:
                break
            consumed_keys.add(case_key)
            return case
        return None

    top1_cases: list[ResolvedCase] = []
    top3_cases: list[ResolvedCase] = []
    top1_cycles = 0
    top3_cycles = 0

    for snap in snapshots:
        snap_ts = to_float(snap.get("ts"), 0.0)
        selected = [
            row
            for row in snap.get("selected", [])
            if isinstance(row, dict) and str(row.get("status", "")).lower() == "armed"
        ]
        if not selected:
            continue
        selected.sort(key=lambda row: int(to_float(row.get("rank"), 9999.0)))

        first = selected[0]
        top1 = pick_case(str(first.get("candidate_id", "")).strip(), snap_ts)
        if top1 is not None:
            top1_cycles += 1
            top1_cases.append(top1)

        picked_any = False
        for row in selected[:3]:
            case = pick_case(str(row.get("candidate_id", "")).strip(), snap_ts)
            if case is None:
                continue
            top3_cases.append(case)
            picked_any = True
        if picked_any:
            top3_cycles += 1

    return {
        "top_1_per_cycle": {"cycles": top1_cycles, **summarize_trade_quality(top1_cases)},
        "top_3_per_cycle": {"cycles": top3_cycles, **summarize_trade_quality(top3_cases)},
    }


def build_cases(events: list[dict[str, Any]]) -> list[ResolvedCase]:
    sorted_events = sorted(events, key=lambda e: to_float(e.get("ts")))
    touched_queues: dict[str, list[dict[str, Any]]] = {}
    cases: list[ResolvedCase] = []

    for event in sorted_events:
        kind = str(event.get("event", ""))
        bkey = base_key(event)

        if kind == "touched":
            touched_queues.setdefault(bkey, []).append(event)
            continue

        if kind not in {"bounced", "failed_breakout", "failed_breakdown", "expired"}:
            continue

        queue = touched_queues.get(bkey)
        if not queue:
            continue

        touch_price = str(event.get("touch_price", ""))
        pick_index = 0
        if touch_price:
            for i, touched in enumerate(queue):
                if str(touched.get("touch_price", "")) == touch_price:
                    pick_index = i
                    break

        touched = queue.pop(pick_index)
        if not queue:
            touched_queues.pop(bkey, None)

        touched_ts = to_float(touched.get("ts"))
        resolved_ts = to_float(event.get("ts"))
        lifetime = max(0.0, resolved_ts - touched_ts)

        outcome = "bounced" if kind == "bounced" else "failed"
        if kind == "expired":
            outcome = "expired"
        side = str(touched.get("side", "")).lower()
        touch_price = to_float(touched.get("touch_price"), 0.0)
        if touch_price <= 0:
            touch_price = to_float(event.get("touch_price"), 0.0)
        resolved_mid = to_float(event.get("exit_mid"), to_float(event.get("last_mid"), 0.0))
        direction = 1.0 if side == "bid" else -1.0 if side == "ask" else 0.0
        favorable_move_pct = 0.0
        loss_proxy_pct = 0.0
        if touch_price > 0 and resolved_mid > 0 and direction != 0:
            raw_move_pct = ((resolved_mid - touch_price) / touch_price) * 100.0
            favorable_move_pct = raw_move_pct * direction
            loss_proxy_pct = max(0.0, -favorable_move_pct)
        touch_mfe_pct = to_float(
            event.get("touch_mfe_pct"),
            to_float(touched.get("touch_mfe_pct"), max(0.0, favorable_move_pct)),
        )
        touch_mae_pct = to_float(
            event.get("touch_mae_pct"),
            to_float(touched.get("touch_mae_pct"), max(0.0, loss_proxy_pct)),
        )
        time_to_outcome_sec = max(0.0, resolved_ts - touched_ts)

        cases.append(
            ResolvedCase(
                candidate_id=str(touched.get("candidate_id", "")),
                market=str(touched.get("market", "unknown")),
                coin=str(touched.get("coin", "")),
                side=side,
                wall_ratio=to_float(touched.get("wall_ratio")),
                wall_dominance_ratio=to_float(touched.get("wall_dominance_ratio"), 1.0),
                wall_notional_usd=to_float(touched.get("wall_notional_usd")),
                # Backward-compatible default for older logs without this field.
                wall_distance_from_spread_pct=to_float(touched.get("wall_distance_from_spread_pct"), 3.0),
                day_volume_usd=to_float(touched.get("day_volume_usd")),
                outcome=outcome,
                touched_ts=touched_ts,
                resolved_ts=resolved_ts,
                lifetime_sec=lifetime,
                visible_age_sec=to_float(touched.get("visible_age_sec")),
                seen_count=int(to_float(touched.get("seen_count"), 0.0)),
                wall_notional_peak_usd=to_float(touched.get("wall_notional_peak_usd")),
                wall_notional_floor_usd=to_float(touched.get("wall_notional_floor_usd")),
                wall_notional_mean_usd=to_float(touched.get("wall_notional_mean_usd")),
                wall_notional_stability_ratio=to_float(touched.get("wall_notional_stability_ratio")),
                round_level_score=to_float(touched.get("round_level_score")),
                wall_notional_volatility_ratio=to_float(touched.get("wall_notional_volatility_ratio")),
                distance_volatility_ratio=to_float(touched.get("distance_volatility_ratio")),
                pre_touch_decay_ratio=to_float(touched.get("pre_touch_decay_ratio"), 1.0),
                distance_compression_ratio=to_float(touched.get("distance_compression_ratio"), 1.0),
                rebuild_count=int(to_float(touched.get("rebuild_count"), 0.0)),
                missing_episodes=int(to_float(touched.get("missing_episodes"), 0.0)),
                touch_survival_sec=to_float(touched.get("touch_survival_sec"), 0.0),
                favorable_move_pct=favorable_move_pct,
                loss_proxy_pct=max(loss_proxy_pct, touch_mae_pct),
                touch_mfe_pct=touch_mfe_pct,
                touch_mae_pct=touch_mae_pct,
                time_to_outcome_sec=time_to_outcome_sec,
                touch_attempt_count=int(to_float(touched.get("touch_attempt_count"), 0.0)),
                updates_before_touch=int(to_float(touched.get("updates_before_touch"), 0.0)),
                approach_speed_pct_per_sec=to_float(touched.get("approach_speed_pct_per_sec"), 0.0),
                entry_score=to_float(touched.get("entry_score"), 0.0),
                liquidity_bucket=liquidity_bucket_for_volume(to_float(touched.get("day_volume_usd"), 0.0)),
            )
        )

    return cases


def make_rule_grid() -> list[dict[str, float | str]]:
    markets = ["all", "perp", "spot"]
    sides = ["all", "bid", "ask"]
    min_ratio = [3, 5, 8, 10, 13]
    max_ratio = [8, 13, 20, 40, INF_SENTINEL]
    min_notional = [10_000, 25_000, 50_000, 100_000, 150_000]
    max_notional = [50_000, 100_000, 250_000, 500_000, 1_000_000, INF_SENTINEL]
    min_day_volume = [0, 50_000, 200_000, 1_000_000, 5_000_000, 10_000_000]
    max_day_volume = [200_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000, INF_SENTINEL]
    max_distance_from_spread = [1.0, 1.5, 2.0, 2.5, 3.0]

    grid: list[dict[str, float | str]] = []
    for market in markets:
        for side in sides:
            for min_r in min_ratio:
                for max_r in max_ratio:
                    if max_r <= min_r:
                        continue
                    for min_n in min_notional:
                        for max_n in max_notional:
                            if max_n <= min_n:
                                continue
                            for min_v in min_day_volume:
                                for max_v in max_day_volume:
                                    if max_v <= min_v:
                                        continue
                                    for max_dist in max_distance_from_spread:
                                        grid.append(
                                            {
                                                "market": market,
                                                "side": side,
                                                "min_ratio": float(min_r),
                                                "max_ratio": float(max_r),
                                                "min_notional_usd": float(min_n),
                                                "max_notional_usd": float(max_n),
                                                "min_day_volume_usd": float(min_v),
                                                "max_day_volume_usd": float(max_v),
                                                "max_wall_distance_pct_from_spread": float(max_dist),
                                            }
                                        )
    return grid


def make_rule_grid_quick() -> list[dict[str, float | str]]:
    ratio_pairs = [
        (3.0, 13.0),
        (3.0, 20.0),
        (5.0, 20.0),
        (8.0, INF_SENTINEL),
    ]
    notional_pairs = [
        (20_000.0, 100_000.0),
        (20_000.0, 250_000.0),
        (50_000.0, 250_000.0),
        (100_000.0, INF_SENTINEL),
    ]
    volume_pairs = [
        (0.0, 1_000_000.0),
        (200_000.0, 10_000_000.0),
        (1_000_000.0, 50_000_000.0),
        (5_000_000.0, INF_SENTINEL),
    ]
    max_distance_from_spread = [1.5, 2.5, 3.0]
    markets = ["all", "perp", "spot"]
    sides = ["all", "bid", "ask"]

    grid: list[dict[str, float | str]] = []
    for market in markets:
        for side in sides:
            for min_ratio, max_ratio in ratio_pairs:
                for min_notional, max_notional in notional_pairs:
                    for min_day_volume, max_day_volume in volume_pairs:
                        for max_dist in max_distance_from_spread:
                            grid.append(
                                {
                                    "market": market,
                                    "side": side,
                                    "min_ratio": min_ratio,
                                    "max_ratio": max_ratio,
                                    "min_notional_usd": min_notional,
                                    "max_notional_usd": max_notional,
                                    "min_day_volume_usd": min_day_volume,
                                    "max_day_volume_usd": max_day_volume,
                                    "max_wall_distance_pct_from_spread": max_dist,
                                }
                            )
    return grid


def build_rule_pool_lookup(cases: list[ResolvedCase]) -> dict[tuple[str, str], list[ResolvedCase]]:
    by_market: dict[str, list[ResolvedCase]] = {"perp": [], "spot": []}
    by_side: dict[str, list[ResolvedCase]] = {"bid": [], "ask": []}
    by_pair: dict[tuple[str, str], list[ResolvedCase]] = {}
    for case in cases:
        by_market.setdefault(case.market, []).append(case)
        by_side.setdefault(case.side, []).append(case)
        by_pair.setdefault((case.market, case.side), []).append(case)

    lookup: dict[tuple[str, str], list[ResolvedCase]] = {
        ("all", "all"): cases,
        ("perp", "all"): by_market.get("perp", []),
        ("spot", "all"): by_market.get("spot", []),
        ("all", "bid"): by_side.get("bid", []),
        ("all", "ask"): by_side.get("ask", []),
        ("perp", "bid"): by_pair.get(("perp", "bid"), []),
        ("perp", "ask"): by_pair.get(("perp", "ask"), []),
        ("spot", "bid"): by_pair.get(("spot", "bid"), []),
        ("spot", "ask"): by_pair.get(("spot", "ask"), []),
    }
    return lookup


def load_existing_quality_rules(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def apply_rule(cases: list[ResolvedCase], rule: dict[str, float | str]) -> list[ResolvedCase]:
    market = str(rule["market"])
    side = str(rule.get("side", "all"))
    min_ratio = float(rule["min_ratio"])
    max_ratio = float(rule["max_ratio"])
    min_notional = float(rule["min_notional_usd"])
    max_notional = float(rule["max_notional_usd"])
    min_day_volume = float(rule["min_day_volume_usd"])
    max_day_volume = float(rule["max_day_volume_usd"])
    max_distance = float(rule.get("max_wall_distance_pct_from_spread", 3.0))

    out: list[ResolvedCase] = []
    for c in cases:
        if market != "all" and c.market != market:
            continue
        if side != "all" and c.side != side:
            continue
        if c.wall_ratio < min_ratio or c.wall_ratio >= max_ratio:
            continue
        if c.wall_notional_usd < min_notional or c.wall_notional_usd >= max_notional:
            continue
        if c.day_volume_usd < min_day_volume or c.day_volume_usd >= max_day_volume:
            continue
        if c.wall_distance_from_spread_pct > max_distance:
            continue
        out.append(c)
    return out


def evaluate_rule_stats(
    cases: list[ResolvedCase],
    rule: dict[str, float | str],
    *,
    min_resolved: int,
) -> dict[str, Any] | None:
    min_ratio = float(rule["min_ratio"])
    max_ratio = float(rule["max_ratio"])
    min_notional = float(rule["min_notional_usd"])
    max_notional = float(rule["max_notional_usd"])
    min_day_volume = float(rule["min_day_volume_usd"])
    max_day_volume = float(rule["max_day_volume_usd"])
    max_distance = float(rule.get("max_wall_distance_pct_from_spread", 3.0))

    n = 0
    wins = 0
    failed_count = 0
    sum_life = 0.0
    sum_dom = 0.0
    sum_volatility = 0.0
    sum_pre_touch_decay = 0.0
    sum_rebuild = 0.0
    sum_win_pct = 0.0
    sum_loss_pct = 0.0
    sum_touch_mfe_pct = 0.0
    sum_touch_mae_pct = 0.0
    fast_10 = 0
    fast_20 = 0
    fast_30 = 0
    coin_counts: Counter[str] = Counter()

    for c in cases:
        if c.wall_ratio < min_ratio or c.wall_ratio >= max_ratio:
            continue
        if c.wall_notional_usd < min_notional or c.wall_notional_usd >= max_notional:
            continue
        if c.day_volume_usd < min_day_volume or c.day_volume_usd >= max_day_volume:
            continue
        if c.wall_distance_from_spread_pct > max_distance:
            continue

        n += 1
        sum_life += c.lifetime_sec
        sum_dom += c.wall_dominance_ratio
        sum_volatility += c.wall_notional_volatility_ratio
        sum_pre_touch_decay += c.pre_touch_decay_ratio
        sum_rebuild += c.rebuild_count
        sum_touch_mae_pct += max(0.0, c.touch_mae_pct)
        coin_counts[c.coin] += 1

        if c.outcome == "bounced":
            wins += 1
            sum_win_pct += max(0.0, c.favorable_move_pct)
            sum_touch_mfe_pct += max(0.0, c.touch_mfe_pct)
            if c.time_to_outcome_sec <= 10.0:
                fast_10 += 1
            if c.time_to_outcome_sec <= 20.0:
                fast_20 += 1
            if c.time_to_outcome_sec <= 30.0:
                fast_30 += 1
        else:
            failed_count += 1
            sum_loss_pct += max(0.0, c.loss_proxy_pct)

    if n < min_resolved:
        return None

    wr = wins / n
    loss_rate = 1.0 - wr
    lb = wilson_lower_bound(wins, n)
    avg_win_pct = sum_win_pct / wins if wins else 0.0
    avg_loss_pct = sum_loss_pct / failed_count if failed_count else 0.0
    expectancy_pct = (wr * avg_win_pct) - (loss_rate * avg_loss_pct)
    top_coin_count = coin_counts.most_common(1)[0][1] if coin_counts else 0

    return {
        **rule,
        "resolved": n,
        "bounced": wins,
        "win_rate": wr,
        "win_rate_pct": wr * 100.0,
        "wilson_lb": lb,
        "wilson_lb_pct": lb * 100.0,
        "avg_lifetime_sec": (sum_life / n) if n else 0.0,
        "avg_wall_dominance_ratio": (sum_dom / n) if n else 0.0,
        "avg_wall_notional_volatility_ratio": (sum_volatility / n) if n else 0.0,
        "avg_pre_touch_decay_ratio": (sum_pre_touch_decay / n) if n else 0.0,
        "avg_rebuild_count": (sum_rebuild / n) if n else 0.0,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "expectancy_pct": expectancy_pct,
        "avg_touch_mfe_pct": (sum_touch_mfe_pct / wins) if wins else 0.0,
        "avg_touch_mae_pct": (sum_touch_mae_pct / n) if n else 0.0,
        "fast_bounce_10s_pct": (fast_10 / wins * 100.0) if wins else 0.0,
        "fast_bounce_20s_pct": (fast_20 / wins * 100.0) if wins else 0.0,
        "fast_bounce_30s_pct": (fast_30 / wins * 100.0) if wins else 0.0,
        "unique_coins": len(coin_counts),
        "top_coin_share_pct": (top_coin_count / n * 100.0) if n else 0.0,
    }


def choose_profile(
    rules_scored: list[dict[str, Any]],
    *,
    stages: list[dict[str, Any]],
    key_func: Any,
) -> dict[str, Any] | None:
    for stage in stages:
        pool = [r for r in rules_scored if r["resolved"] >= int(stage["min_resolved"])]
        pool = [r for r in pool if r["unique_coins"] >= int(stage["min_unique_coins"])]
        pool = [r for r in pool if r["top_coin_share_pct"] <= float(stage["max_top_coin_share_pct"])]

        market = stage.get("market")
        if market:
            pool = [r for r in pool if r["market"] == market]

        side = stage.get("side")
        if side:
            pool = [r for r in pool if r["side"] == side]

        min_day_volume = stage.get("min_day_volume_usd")
        if min_day_volume is not None:
            pool = [r for r in pool if r["min_day_volume_usd"] >= float(min_day_volume)]

        min_notional = stage.get("min_notional_usd")
        if min_notional is not None:
            pool = [r for r in pool if r["min_notional_usd"] >= float(min_notional)]

        min_expectancy = stage.get("min_expectancy_pct")
        if min_expectancy is not None:
            pool = [r for r in pool if r.get("expectancy_pct", 0.0) >= float(min_expectancy)]

        if pool:
            return max(pool, key=key_func)

    return None


def pick_profiles(rules_scored: list[dict[str, Any]], min_resolved: int) -> dict[str, Any]:
    strict = choose_profile(
        rules_scored,
        stages=[
            {
                "market": "all",
                "side": "all",
                "min_notional_usd": 20_000,
                "min_day_volume_usd": 200_000,
                "min_resolved": max(120, min_resolved),
                "min_unique_coins": 12,
                "max_top_coin_share_pct": 35.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "market": "all",
                "side": "all",
                "min_notional_usd": 20_000,
                "min_day_volume_usd": 200_000,
                "min_resolved": max(100, min_resolved),
                "min_unique_coins": 8,
                "max_top_coin_share_pct": 50.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "market": "all",
                "side": "all",
                "min_notional_usd": 20_000,
                "min_day_volume_usd": 200_000,
                "min_resolved": min_resolved,
                "min_unique_coins": 4,
                "max_top_coin_share_pct": 75.0,
                "min_expectancy_pct": -0.02,
            },
        ],
        key_func=lambda r: (r["wilson_lb"], r["expectancy_pct"], r["win_rate"], r["resolved"]),
    )

    balanced = choose_profile(
        rules_scored,
        stages=[
            {
                "min_resolved": max(220, min_resolved),
                "min_unique_coins": 18,
                "max_top_coin_share_pct": 25.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "min_resolved": max(160, min_resolved),
                "min_unique_coins": 12,
                "max_top_coin_share_pct": 35.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "min_resolved": max(120, min_resolved),
                "min_unique_coins": 8,
                "max_top_coin_share_pct": 50.0,
                "min_expectancy_pct": -0.02,
            },
        ],
        key_func=lambda r: (
            r["wilson_lb"] * math.log1p(r["resolved"]),
            r["expectancy_pct"],
            r["wilson_lb"],
            r["resolved"],
        ),
    )

    flow = choose_profile(
        rules_scored,
        stages=[
            {
                "min_resolved": max(700, min_resolved),
                "min_unique_coins": 35,
                "max_top_coin_share_pct": 15.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "min_resolved": max(500, min_resolved),
                "min_unique_coins": 25,
                "max_top_coin_share_pct": 20.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "min_resolved": max(300, min_resolved),
                "min_unique_coins": 18,
                "max_top_coin_share_pct": 30.0,
                "min_expectancy_pct": -0.02,
            },
        ],
        key_func=lambda r: (r["resolved"] * r["wilson_lb"], r["expectancy_pct"], r["wilson_lb"], r["resolved"]),
    )

    actionable = choose_profile(
        rules_scored,
        stages=[
            {
                "market": "perp",
                "side": "ask",
                "min_day_volume_usd": 50_000,
                "min_resolved": max(80, min_resolved),
                "min_unique_coins": 20,
                "max_top_coin_share_pct": 35.0,
                "min_expectancy_pct": 0.0,
            },
            {
                "market": "perp",
                "side": "ask",
                "min_day_volume_usd": 50_000,
                "min_resolved": max(60, min_resolved),
                "min_unique_coins": 12,
                "max_top_coin_share_pct": 45.0,
                "min_expectancy_pct": -0.02,
            },
        ],
        key_func=lambda r: (r["wilson_lb"], r["expectancy_pct"], r["win_rate"], r["resolved"]),
    )

    return {
        "strict": strict,
        "balanced": balanced,
        "flow": flow,
        "actionable": actionable or strict,
    }


def summarize_cases(cases: list[ResolvedCase]) -> dict[str, Any]:
    resolved = [c for c in cases if c.outcome in {"bounced", "failed"}]
    bounced = [c for c in resolved if c.outcome == "bounced"]
    expired = [c for c in cases if c.outcome == "expired"]
    by_market: dict[str, dict[str, int]] = {}
    by_side: dict[str, dict[str, int]] = {}

    for market in ("perp", "spot"):
        subset = [c for c in resolved if c.market == market]
        wins = [c for c in subset if c.outcome == "bounced"]
        by_market[market] = {"resolved": len(subset), "bounced": len(wins)}

    for side in ("bid", "ask"):
        subset = [c for c in resolved if c.side == side]
        wins = [c for c in subset if c.outcome == "bounced"]
        by_side[side] = {"resolved": len(subset), "bounced": len(wins)}

    return {
        "cases_total": len(cases),
        "resolved_total": len(resolved),
        "bounced_total": len(bounced),
        "expired_total": len(expired),
        "win_rate_pct": (len(bounced) / len(resolved) * 100.0) if resolved else 0.0,
        "by_market": by_market,
        "by_side": by_side,
    }


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Derive robust quality rules from collector logs")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help='Example: "cloud_data/events_*.jsonl"')
    parser.add_argument("--out-json", default="data/quality_rules.json")
    parser.add_argument("--min-resolved", type=int, default=60)
    parser.add_argument("--mode", choices=["quick", "full", "ranking-only"], default="quick")
    args = parser.parse_args()

    t0 = time.perf_counter()
    print(f"[derive] mode={args.mode}")

    if args.log_glob:
        events, matched = load_from_glob(args.log_glob)
        source = f"{args.log_glob} ({len(matched)} files)"
    else:
        events = load_jsonl(Path(args.log_path))
        source = args.log_path
    t1 = time.perf_counter()
    print(f"[derive] loaded events={len(events)} in {t1 - t0:.2f}s")

    if not events:
        raise SystemExit("No events found.")

    cases = build_cases(events)
    t2 = time.perf_counter()
    print(f"[derive] built cases={len(cases)} in {t2 - t1:.2f}s")
    if not cases:
        raise SystemExit("No resolved/touched matched cases found yet.")

    resolved = [c for c in cases if c.outcome in {"bounced", "failed"}]
    t3 = time.perf_counter()
    print(f"[derive] resolved cases={len(resolved)} in {t3 - t2:.2f}s")
    if not resolved:
        raise SystemExit("No resolved bounced/failed cases yet.")

    overall = summarize_cases(cases)
    ranking_eval = build_ranking_eval(resolved)
    ranking_eval["top_per_cycle"] = build_cycle_top_eval(events, resolved)
    t4 = time.perf_counter()
    print(f"[derive] ranking/top-cycle ready in {t4 - t3:.2f}s")

    scored: list[dict[str, Any]] = []
    profiles: dict[str, Any] = {}
    existing_output = load_existing_quality_rules(Path(args.out_json))
    if args.mode == "ranking-only":
        profiles = dict(existing_output.get("profiles", {}) or {})
        scored = list(existing_output.get("top_rules", []) or [])
        print("[derive] rules search skipped (ranking-only mode)")
    else:
        rule_grid = make_rule_grid_quick() if args.mode == "quick" else make_rule_grid()
        lookup = build_rule_pool_lookup(resolved)
        print(f"[derive] rules search start: rules={len(rule_grid)}")
        rules_t0 = time.perf_counter()
        for idx, rule in enumerate(rule_grid, start=1):
            if idx % 500 == 0 or idx == len(rule_grid):
                print(f"[derive] rules progress {idx}/{len(rule_grid)}")
            pool = lookup.get((str(rule["market"]), str(rule.get("side", "all"))), resolved)
            scored_row = evaluate_rule_stats(pool, rule, min_resolved=args.min_resolved)
            if scored_row is None:
                continue
            scored.append(scored_row)
        rules_t1 = time.perf_counter()
        print(f"[derive] rules search done in {rules_t1 - rules_t0:.2f}s, buckets={len(scored)}")
        if scored:
            scored.sort(key=lambda r: (r["wilson_lb"], r["expectancy_pct"], r["resolved"]), reverse=True)
            profiles = pick_profiles(scored, args.min_resolved)
        else:
            profiles = dict(existing_output.get("profiles", {}) or {})
            scored = list(existing_output.get("top_rules", []) or [])
            print("[derive] no rule bucket met threshold; reusing existing profiles/top_rules")

    output = {
        "source": source,
        "generated_at_ts": time.time(),
        "derive_mode": args.mode,
        "events_loaded": len(events),
        "overall": overall,
        "ranking_eval": ranking_eval,
        "profiles": profiles,
        "top_rules": scored[:100],
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    t5 = time.perf_counter()
    print(f"[derive] saved {out_path} in {t5 - t4:.2f}s (total {t5 - t0:.2f}s)")

    print(f"Loaded events: {len(events)} from {source}")
    print("Overall:")
    print(
        f"  resolved={overall['resolved_total']} bounced={overall['bounced_total']} "
        f"win_rate={overall['win_rate_pct']:.2f}%"
    )
    print("Profiles:")
    for name in ("strict", "balanced", "flow", "actionable"):
        profile = profiles.get(name)
        if not profile:
            print(f"  {name}: not enough data")
            continue
        print(
            f"  {name}: {rule_to_human(profile)} | resolved={profile['resolved']} "
            f"wr={profile['win_rate_pct']:.2f}% lb={profile['wilson_lb_pct']:.2f}% "
            f"exp={profile.get('expectancy_pct', 0.0):.4f}% "
            f"coins={profile['unique_coins']} top_coin_share={profile['top_coin_share_pct']:.1f}%"
        )

    print("\nTop 10 by Wilson lower bound:")
    for row in scored[:10]:
        print(
            f"  {rule_to_human(row)} | resolved={row['resolved']} bounced={row['bounced']} "
            f"wr={row['win_rate_pct']:.2f}% lb={row['wilson_lb_pct']:.2f}% "
            f"exp={row.get('expectancy_pct', 0.0):.4f}% "
            f"coins={row['unique_coins']} top_coin_share={row['top_coin_share_pct']:.1f}%"
        )
    print("\nRanking top-slices:")
    for row in ranking_eval.get("top_slices", []):
        print(
            f"  {row['slice']}: count={row['count']} wr={row['win_rate_pct']:.2f}% "
            f"exp={row['expectancy_pct']:.4f}% mfe={row['avg_touch_mfe_pct']:.4f}% mae={row['avg_touch_mae_pct']:.4f}%"
        )
    cycle_eval = ranking_eval.get("top_per_cycle", {})
    top1 = cycle_eval.get("top_1_per_cycle", {})
    top3 = cycle_eval.get("top_3_per_cycle", {})
    print(
        f"  top_1_per_cycle: cycles={top1.get('cycles', 0)} count={top1.get('count', 0)} "
        f"wr={top1.get('win_rate_pct', 0.0):.2f}% exp={top1.get('expectancy_pct', 0.0):.4f}% "
        f"mfe={top1.get('avg_touch_mfe_pct', 0.0):.4f}% mae={top1.get('avg_touch_mae_pct', 0.0):.4f}%"
    )
    print(
        f"  top_3_per_cycle: cycles={top3.get('cycles', 0)} count={top3.get('count', 0)} "
        f"wr={top3.get('win_rate_pct', 0.0):.2f}% exp={top3.get('expectancy_pct', 0.0):.4f}% "
        f"mfe={top3.get('avg_touch_mfe_pct', 0.0):.4f}% mae={top3.get('avg_touch_mae_pct', 0.0):.4f}%"
    )
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
