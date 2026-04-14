from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


def ratio_bin(value: float) -> str:
    if value < 5:
        return "3-5"
    if value < 8:
        return "5-8"
    if value < 13:
        return "8-13"
    if value < 20:
        return "13-20"
    if value < 40:
        return "20-40"
    return "40+"


def usd_bin(value: float) -> str:
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


def vol_bin(value: float) -> str:
    if value < 50_000:
        return "<50k"
    if value < 200_000:
        return "50k-200k"
    if value < 1_000_000:
        return "200k-1m"
    if value < 5_000_000:
        return "1m-5m"
    return "5m+"


def liquidity_bucket(value: float) -> str:
    if value >= 25_000_000:
        return "majors"
    if value >= 2_500_000:
        return "mids"
    return "trash"


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


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def base_key(event: dict[str, Any]) -> str:
    cid = str(event.get("candidate_id", "")).strip()
    if cid:
        return cid
    return (
        f"{event.get('market', '')}|{event.get('coin', '')}|"
        f"{event.get('side', '')}|{event.get('wall_price', '')}"
    )


def build_resolved_cases(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_events = sorted(events, key=lambda e: to_float(e.get("ts")))
    touched_queues: dict[str, list[dict[str, Any]]] = {}
    resolved: list[dict[str, Any]] = []

    for event in sorted_events:
        kind = str(event.get("event", ""))
        key = base_key(event)

        if kind == "touched":
            touched_queues.setdefault(key, []).append(event)
            continue

        if kind not in {"bounced", "failed_breakout", "failed_breakdown", "expired"}:
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

        outcome = "bounced" if kind == "bounced" else "failed"
        if kind == "expired":
            outcome = "expired"

        side = str(touched.get("side", "")).lower()
        touch_ts = to_float(touched.get("ts"), 0.0)
        resolved_ts = to_float(event.get("ts"), touch_ts)
        time_to_outcome_sec = max(0.0, resolved_ts - touch_ts)

        touch_price = to_float(touched.get("touch_price"), 0.0)
        if touch_price <= 0:
            touch_price = to_float(event.get("touch_price"), 0.0)
        resolved_mid = to_float(event.get("exit_mid"), to_float(event.get("last_mid"), 0.0))
        direction = 1.0 if side == "bid" else -1.0 if side == "ask" else 0.0
        favorable_move_pct = 0.0
        adverse_move_pct = 0.0
        if touch_price > 0 and resolved_mid > 0 and direction != 0:
            raw_move_pct = ((resolved_mid - touch_price) / touch_price) * 100.0
            favorable_move_pct = raw_move_pct * direction
            adverse_move_pct = max(0.0, -favorable_move_pct)
        touch_mfe_pct = to_float(
            event.get("touch_mfe_pct"),
            to_float(touched.get("touch_mfe_pct"), max(0.0, favorable_move_pct)),
        )
        touch_mae_pct = to_float(
            event.get("touch_mae_pct"),
            to_float(touched.get("touch_mae_pct"), max(0.0, adverse_move_pct)),
        )
        loss_proxy_pct = max(0.0, touch_mae_pct, adverse_move_pct)
        trade_pnl_pct = favorable_move_pct if outcome == "bounced" else -loss_proxy_pct

        resolved.append(
            {
                "candidate_id": str(touched.get("candidate_id", "")).strip(),
                "market": str(touched.get("market", "unknown")),
                "coin": str(touched.get("coin", "")),
                "side": side,
                "ratio": to_float(touched.get("wall_ratio")),
                "dominance": to_float(touched.get("wall_dominance_ratio"), 1.0),
                "wall_usd": to_float(touched.get("wall_notional_usd")),
                "distance_pct": to_float(touched.get("wall_distance_from_spread_pct"), 3.0),
                "day_volume_usd": to_float(touched.get("day_volume_usd")),
                "visible_age_sec": to_float(touched.get("visible_age_sec"), 0.0),
                "stability_ratio": to_float(touched.get("wall_notional_stability_ratio"), 0.0),
                "current_vs_peak_ratio": to_float(touched.get("wall_notional_current_vs_peak_ratio"), 0.0),
                "round_level_score": to_float(touched.get("round_level_score"), 0.0),
                "wall_to_hour_volume_pct": to_float(touched.get("wall_to_hour_volume_pct"), 0.0),
                "volatility_ratio": to_float(touched.get("wall_notional_volatility_ratio"), 0.0),
                "pre_touch_decay_ratio": to_float(touched.get("pre_touch_decay_ratio"), 1.0),
                "rebuild_count": int(to_float(touched.get("rebuild_count"), 0.0)),
                "touch_attempt_count": int(to_float(touched.get("touch_attempt_count"), 0.0)),
                "updates_before_touch": int(to_float(touched.get("updates_before_touch"), 0.0)),
                "approach_speed_pct_per_sec": to_float(touched.get("approach_speed_pct_per_sec"), 0.0),
                "entry_score": to_float(touched.get("entry_score"), 0.0),
                "time_to_outcome_sec": time_to_outcome_sec,
                "favorable_move_pct": favorable_move_pct,
                "adverse_move_pct": adverse_move_pct,
                "touch_mfe_pct": touch_mfe_pct,
                "touch_mae_pct": touch_mae_pct,
                "loss_proxy_pct": loss_proxy_pct,
                "trade_pnl_pct": trade_pnl_pct,
                "fast_bounce_10s": bool(outcome == "bounced" and time_to_outcome_sec <= 10.0),
                "fast_bounce_20s": bool(outcome == "bounced" and time_to_outcome_sec <= 20.0),
                "fast_bounce_30s": bool(outcome == "bounced" and time_to_outcome_sec <= 30.0),
                "dirty_touch": bool(outcome == "bounced" and touch_mae_pct >= 0.10),
                "outcome": outcome,
                "touched_ts": touch_ts,
                "resolved_ts": resolved_ts,
                "liquidity_bucket": liquidity_bucket(to_float(touched.get("day_volume_usd"), 0.0)),
            }
        )

    return resolved


def print_group(rows: list[tuple[float, int, int, str]], title: str) -> None:
    print(f"\n{title}:")
    for wr, total, win, bucket in rows:
        print(f"{bucket:28s} total={total:5d} bounced={win:5d} win_rate={wr:6.2f}%")


def compute_behavior_score(case: dict[str, Any]) -> float:
    ratio = max(0.0, to_float(case.get("ratio"), 0.0))
    dom = max(0.0, to_float(case.get("dominance"), 0.0))
    w2h = max(0.0, to_float(case.get("wall_to_hour_volume_pct"), 0.0))
    age_norm = min(max(0.0, to_float(case.get("visible_age_sec"), 0.0)) / 60.0, 1.0)
    stability = max(0.0, to_float(case.get("stability_ratio"), 0.0))
    current_vs_peak = max(0.0, to_float(case.get("current_vs_peak_ratio"), 0.0))
    round_level = max(0.0, to_float(case.get("round_level_score"), 0.0))
    dist = max(0.0, to_float(case.get("distance_pct"), 0.0))
    volatility = max(0.0, to_float(case.get("volatility_ratio"), 0.0))
    pre_touch_decay = max(0.0, to_float(case.get("pre_touch_decay_ratio"), 1.0))
    rebuild_count = max(0.0, to_float(case.get("rebuild_count"), 0.0))
    approach_speed = max(0.0, to_float(case.get("approach_speed_pct_per_sec"), 0.0))

    near_spread_penalty = max(0.0, (0.20 - dist) / 0.20) if dist < 0.20 else 0.0
    decay_penalty = max(0.0, 1.0 - pre_touch_decay)
    rebuild_penalty = min(rebuild_count / 3.0, 1.5)

    return (
        1.00 * math.log1p(ratio)
        + 0.90 * math.log1p(dom)
        + 0.80 * math.log1p(max(w2h, 0.01))
        + 0.70 * age_norm
        + 0.80 * stability
        + 0.70 * current_vs_peak
        + 0.35 * round_level
        - 0.90 * near_spread_penalty
        - 0.80 * volatility
        - 1.10 * decay_penalty
        - 0.70 * rebuild_penalty
        - 0.15 * min(approach_speed / 0.08, 1.0)
    )


def summarize_subset(cases: list[dict[str, Any]]) -> dict[str, float]:
    if not cases:
        return {
            "count": 0.0,
            "wr_pct": 0.0,
            "expectancy_pct": 0.0,
            "avg_mfe_pct": 0.0,
            "avg_mae_pct": 0.0,
        }
    n = len(cases)
    wins = [c for c in cases if c.get("outcome") == "bounced"]
    fails = [c for c in cases if c.get("outcome") == "failed"]
    wr = len(wins) / n
    avg_win = mean([max(0.0, to_float(c.get("favorable_move_pct"))) for c in wins])
    avg_loss = mean([max(0.0, to_float(c.get("loss_proxy_pct"))) for c in fails])
    expectancy = (wr * avg_win) - ((1.0 - wr) * avg_loss)
    avg_mfe = mean([max(0.0, to_float(c.get("touch_mfe_pct"))) for c in cases])
    avg_mae = mean([max(0.0, to_float(c.get("touch_mae_pct"))) for c in cases])
    return {
        "count": float(n),
        "wr_pct": wr * 100.0,
        "expectancy_pct": expectancy,
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
    }


def build_top_per_cycle_eval(events: list[dict[str, Any]], resolved: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
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
        return {
            "top_1_per_cycle": {"cycles": 0.0, **summarize_subset([])},
            "top_3_per_cycle": {"cycles": 0.0, **summarize_subset([])},
        }

    cases_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case in sorted(resolved, key=lambda c: to_float(c.get("touched_ts"), 0.0)):
        cid = str(case.get("candidate_id", "")).strip()
        if cid:
            cases_by_candidate[cid].append(case)

    consumed: set[tuple[str, float]] = set()

    def pick_case(candidate_id: str, snap_ts: float) -> dict[str, Any] | None:
        if not candidate_id:
            return None
        for case in cases_by_candidate.get(candidate_id, []):
            case_key = (candidate_id, to_float(case.get("touched_ts"), 0.0))
            if case_key in consumed:
                continue
            touched_ts = to_float(case.get("touched_ts"), 0.0)
            if touched_ts < snap_ts - 1.0:
                continue
            if touched_ts > snap_ts + 1800.0:
                break
            consumed.add(case_key)
            return case
        return None

    top1_cases: list[dict[str, Any]] = []
    top3_cases: list[dict[str, Any]] = []
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
        first_case = pick_case(str(first.get("candidate_id", "")).strip(), snap_ts)
        if first_case is not None:
            top1_cycles += 1
            top1_cases.append(first_case)

        picked_any = False
        for row in selected[:3]:
            c = pick_case(str(row.get("candidate_id", "")).strip(), snap_ts)
            if c is None:
                continue
            top3_cases.append(c)
            picked_any = True
        if picked_any:
            top3_cycles += 1

    return {
        "top_1_per_cycle": {"cycles": float(top1_cycles), **summarize_subset(top1_cases)},
        "top_3_per_cycle": {"cycles": float(top3_cycles), **summarize_subset(top3_cases)},
    }


def decile_cutoffs(scores: list[float]) -> list[float]:
    if not scores:
        return []
    ordered = sorted(scores)
    cuts: list[float] = []
    n = len(ordered)
    for p in range(1, 10):
        idx = int(round((n - 1) * p / 10.0))
        cuts.append(ordered[idx])
    return cuts


def decile_label(score: float, cuts: list[float]) -> str:
    if not cuts:
        return "D10"
    for i, c in enumerate(cuts, start=1):
        if score <= c:
            return f"D{i}"
    return "D10"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Hyperliquid density screener event log")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--log-glob", default="", help="Example: cloud_data/events_*.jsonl")
    parser.add_argument("--min-bucket-samples", type=int, default=20)
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
    cases = build_resolved_cases(events)
    if not cases:
        print("No resolved/touched matched cases yet.")
        return

    resolved = [c for c in cases if c["outcome"] in {"bounced", "failed"}]
    expired = [c for c in cases if c["outcome"] == "expired"]
    if not resolved:
        print("No resolved bounced/failed cases yet.")
        return

    bounced_total = sum(1 for c in resolved if c["outcome"] == "bounced")
    win_rate = bounced_total / len(resolved)
    loss_rate = 1.0 - win_rate
    win_moves = [max(0.0, to_float(c.get("favorable_move_pct"))) for c in resolved if c["outcome"] == "bounced"]
    loss_moves = [max(0.0, to_float(c.get("loss_proxy_pct"))) for c in resolved if c["outcome"] == "failed"]
    avg_win_pct = mean(win_moves)
    avg_loss_pct = mean(loss_moves)
    expectancy_pct = (win_rate * avg_win_pct) - (loss_rate * avg_loss_pct)
    bounced_rows = [c for c in resolved if c["outcome"] == "bounced"]
    fast10 = sum(1 for c in bounced_rows if bool(c.get("fast_bounce_10s")))
    fast20 = sum(1 for c in bounced_rows if bool(c.get("fast_bounce_20s")))
    fast30 = sum(1 for c in bounced_rows if bool(c.get("fast_bounce_30s")))
    dirty = sum(1 for c in bounced_rows if bool(c.get("dirty_touch")))
    median_mfe = sorted([to_float(c.get("touch_mfe_pct"), 0.0) for c in bounced_rows])[len(bounced_rows) // 2] if bounced_rows else 0.0
    median_mae = sorted([to_float(c.get("touch_mae_pct"), 0.0) for c in resolved])[len(resolved) // 2] if resolved else 0.0
    print(
        f"Resolved: {len(resolved)} | bounced: {bounced_total} | "
        f"win_rate={bounced_total / len(resolved) * 100.0:.2f}% | expired={len(expired)}"
    )
    print(
        "Expectancy: "
        f"{expectancy_pct:.4f}% per signal | avg_win={avg_win_pct:.4f}% avg_loss={avg_loss_pct:.4f}% "
        f"| median_MFE={median_mfe:.4f}% median_MAE={median_mae:.4f}%"
    )
    if bounced_total > 0:
        print(
            "Fast bounce share: "
            f"<=10s {fast10 / bounced_total * 100.0:.2f}% | "
            f"<=20s {fast20 / bounced_total * 100.0:.2f}% | "
            f"<=30s {fast30 / bounced_total * 100.0:.2f}% | "
            f"dirty-touch {dirty / bounced_total * 100.0:.2f}%"
        )

    by_market = defaultdict(lambda: {"total": 0, "win": 0})
    by_side = defaultdict(lambda: {"total": 0, "win": 0})
    ratio_totals = defaultdict(int)
    ratio_wins = defaultdict(int)
    usd_totals = defaultdict(int)
    usd_wins = defaultdict(int)
    vol_totals = defaultdict(int)
    vol_wins = defaultdict(int)

    for c in resolved:
        market = c["market"]
        side = c["side"]
        won = c["outcome"] == "bounced"
        rb = ratio_bin(c["ratio"])
        ub = usd_bin(c["wall_usd"])
        vb = vol_bin(c["day_volume_usd"])

        by_market[market]["total"] += 1
        by_side[side]["total"] += 1
        if won:
            by_market[market]["win"] += 1
            by_side[side]["win"] += 1
            ratio_wins[rb] += 1
            usd_wins[ub] += 1
            vol_wins[vb] += 1
        ratio_totals[rb] += 1
        usd_totals[ub] += 1
        vol_totals[vb] += 1

    print("\nBy market:")
    for market, stats in sorted(by_market.items()):
        total = stats["total"]
        win = stats["win"]
        wr = (win / total * 100.0) if total else 0.0
        print(f"{market:6s} total={total:5d} bounced={win:5d} win_rate={wr:6.2f}%")

    print("\nBy side:")
    for side, stats in sorted(by_side.items()):
        total = stats["total"]
        win = stats["win"]
        wr = (win / total * 100.0) if total else 0.0
        print(f"{side:6s} total={total:5d} bounced={win:5d} win_rate={wr:6.2f}%")

    ratio_rows: list[tuple[float, int, int, str]] = []
    for bucket, total in ratio_totals.items():
        if total < args.min_bucket_samples:
            continue
        win = ratio_wins.get(bucket, 0)
        ratio_rows.append(((win / total * 100.0), total, win, bucket))
    ratio_rows.sort(reverse=True)

    usd_rows: list[tuple[float, int, int, str]] = []
    for bucket, total in usd_totals.items():
        if total < args.min_bucket_samples:
            continue
        win = usd_wins.get(bucket, 0)
        usd_rows.append(((win / total * 100.0), total, win, bucket))
    usd_rows.sort(reverse=True)

    vol_rows: list[tuple[float, int, int, str]] = []
    for bucket, total in vol_totals.items():
        if total < args.min_bucket_samples:
            continue
        win = vol_wins.get(bucket, 0)
        vol_rows.append(((win / total * 100.0), total, win, bucket))
    vol_rows.sort(reverse=True)

    print_group(ratio_rows, "By ratio bucket")
    print_group(usd_rows, "By wall USD bucket")
    print_group(vol_rows, "By day volume bucket")

    print("\nBy liquidity bucket:")
    for bucket in ("majors", "mids", "trash"):
        subset = [c for c in resolved if str(c.get("liquidity_bucket", "")) == bucket]
        summary = summarize_subset(subset)
        print(
            f"{bucket:28s} total={int(summary['count']):5d} wr={summary['wr_pct']:6.2f}% "
            f"exp={summary['expectancy_pct']:8.4f}% mfe={summary['avg_mfe_pct']:7.4f}% mae={summary['avg_mae_pct']:7.4f}%"
        )

    scored_cases: list[dict[str, Any]] = []
    for c in resolved:
        cc = dict(c)
        cc["behavior_score"] = compute_behavior_score(cc)
        scored_cases.append(cc)

    scores = [to_float(c.get("behavior_score")) for c in scored_cases]
    cuts = decile_cutoffs(scores)
    by_decile = defaultdict(lambda: {"total": 0, "win": 0, "pnl_sum": 0.0})
    for c in scored_cases:
        label = decile_label(to_float(c.get("behavior_score")), cuts)
        by_decile[label]["total"] += 1
        if c.get("outcome") == "bounced":
            by_decile[label]["win"] += 1
        by_decile[label]["pnl_sum"] += to_float(c.get("trade_pnl_pct"), 0.0)

    decile_rows: list[tuple[float, int, int, str]] = []
    for d in [f"D{i}" for i in range(10, 0, -1)]:
        total = by_decile[d]["total"]
        if total < args.min_bucket_samples:
            continue
        win = by_decile[d]["win"]
        decile_rows.append(((win / total * 100.0), total, win, d))
    print_group(decile_rows, "By behavior-score decile (D10 best)")
    print("\nBehavior deciles with expectancy:")
    for d in [f"D{i}" for i in range(10, 0, -1)]:
        total = by_decile[d]["total"]
        if total < args.min_bucket_samples:
            continue
        win = by_decile[d]["win"]
        wr = (win / total * 100.0) if total else 0.0
        exp_pct = by_decile[d]["pnl_sum"] / total if total else 0.0
        print(f"{d:4s} total={total:5d} WR={wr:6.2f}% expectancy={exp_pct:8.4f}%")

    top_cycle = build_top_per_cycle_eval(events, resolved)
    top1 = top_cycle["top_1_per_cycle"]
    top3 = top_cycle["top_3_per_cycle"]
    print("\nTop per cycle:")
    print(
        f"top_1_per_cycle cycles={int(top1['cycles'])} count={int(top1['count'])} "
        f"WR={top1['wr_pct']:.2f}% exp={top1['expectancy_pct']:.4f}% "
        f"MFE={top1['avg_mfe_pct']:.4f}% MAE={top1['avg_mae_pct']:.4f}%"
    )
    print(
        f"top_3_per_cycle cycles={int(top3['cycles'])} count={int(top3['count'])} "
        f"WR={top3['wr_pct']:.2f}% exp={top3['expectancy_pct']:.4f}% "
        f"MFE={top3['avg_mfe_pct']:.4f}% MAE={top3['avg_mae_pct']:.4f}%"
    )


if __name__ == "__main__":
    main()
