from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from density_screener import entry_score_from_metrics
from paper_trade_runtime import (
    PendingPaperTrade,
    RESOLVED_EVENTS,
    profile_matches,
    update_post_touch_metrics,
    favorable_move_pct as paper_favorable_move_pct,
    loss_proxy_pct as paper_loss_proxy_pct,
)
from rolling_stability_report import read_last_ts


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def metrics_from_armed_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "seen_count": to_float(event.get("seen_count"), 0.0),
        "wall_notional_stability_ratio": to_float(event.get("wall_notional_stability_ratio"), 0.0),
        "wall_notional_current_vs_peak_ratio": to_float(event.get("wall_notional_current_vs_peak_ratio"), 0.0),
        "pre_touch_decay_ratio": to_float(event.get("pre_touch_decay_ratio"), 1.0),
        "distance_compression_ratio": to_float(event.get("distance_compression_ratio"), 1.0),
        "approach_speed_pct_per_sec": to_float(event.get("approach_speed_pct_per_sec"), 0.0),
        "touch_attempt_count": to_float(event.get("touch_attempt_count"), 0.0),
        "updates_before_touch": to_float(event.get("updates_before_touch"), 0.0),
        "touch_survival_sec": to_float(event.get("touch_survival_sec"), 0.0),
    }


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, float]:
    if not rows:
        return {
            "count": 0.0,
            "wr_pct": 0.0,
            "expectancy_pct": 0.0,
            "avg_mfe_pct": 0.0,
            "avg_mae_pct": 0.0,
        }
    wins = [r for r in rows if str(r.get("outcome", "")).lower() == "bounced"]
    fails = [r for r in rows if str(r.get("outcome", "")).lower() != "bounced"]
    total = len(rows)
    wr = len(wins) / total
    avg_win = sum(max(0.0, to_float(r.get("favorable_move_pct"), 0.0)) for r in wins) / len(wins) if wins else 0.0
    avg_loss = sum(max(0.0, to_float(r.get("loss_proxy_pct"), 0.0)) for r in fails) / len(fails) if fails else 0.0
    expectancy = (wr * avg_win) - ((1.0 - wr) * avg_loss)
    avg_mfe = sum(max(0.0, to_float(r.get("touch_mfe_pct"), 0.0)) for r in rows) / total
    avg_mae = sum(max(0.0, to_float(r.get("touch_mae_pct"), 0.0)) for r in rows) / total
    return {
        "count": float(total),
        "wr_pct": wr * 100.0,
        "expectancy_pct": expectancy,
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
    }


def finalize_trade(trade: PendingPaperTrade, event: dict[str, Any]) -> dict[str, Any]:
    update_post_touch_metrics(trade, event)
    favorable_pct = max(
        paper_favorable_move_pct(event),
        trade.best_favorable_pct,
        trade.touch_mfe_pct,
    )
    loss_pct = max(
        paper_loss_proxy_pct(event, favorable_pct),
        trade.best_loss_proxy_pct,
        trade.touch_mae_pct,
    )
    return {
        "ts": trade.selected_ts,
        "candidate_id": trade.candidate_id,
        "coin": trade.coin,
        "side": trade.side,
        "liquidity_bucket": trade.liquidity_bucket,
        "side_weighting_profile": trade.side_weighting_profile,
        "handcrafted_score": trade.handcrafted_score,
        "ml_proba": trade.ml_proba,
        "final_score": trade.final_score,
        "behavior_tag": trade.behavior_tag,
        "touched_ts": trade.touched_ts,
        "outcome": str(event.get("event", "")).lower(),
        "favorable_move_pct": favorable_pct,
        "loss_proxy_pct": loss_pct,
        "touch_mfe_pct": max(to_float(event.get("touch_mfe_pct"), 0.0), trade.touch_mfe_pct),
        "touch_mae_pct": max(to_float(event.get("touch_mae_pct"), 0.0), trade.touch_mae_pct),
    }


def pending_key(mode_name: str, lane: str, candidate_id: str) -> str:
    return f"{mode_name}|{lane}|{candidate_id}"


def run_report(
    *,
    log_path: Path,
    profile: str,
    fresh_hours: float,
    min_score: float,
    entry_weight: float,
    out_json: Path | None,
) -> dict[str, Any]:
    last_ts = read_last_ts(log_path)
    cutoff_ts = last_ts - (fresh_hours * 3600.0)

    latest_armed: dict[str, dict[str, Any]] = {}
    pending: dict[str, PendingPaperTrade] = {}
    resolved_rows: dict[str, list[dict[str, Any]]] = {
        "baseline:overall": [],
        "baseline:top1": [],
        "baseline:top3": [],
        "entry:overall": [],
        "entry:top1": [],
        "entry:top3": [],
    }
    cycle_counts: dict[str, int] = {
        "baseline:top1": 0,
        "baseline:top3": 0,
        "entry:top1": 0,
        "entry:top3": 0,
    }

    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue

            kind = str(event.get("event", "")).lower()
            ts = to_float(event.get("ts"), 0.0)

            if kind == "heartbeat" and str(event.get("candidate_status", "")).lower() == "armed":
                candidate_id = str(event.get("candidate_id", "")).strip()
                if candidate_id:
                    latest_armed[candidate_id] = event
                continue

            if kind == "selection_snapshot_pre_touch":
                if ts < cutoff_ts:
                    continue
                selected = event.get("selected")
                if not isinstance(selected, list):
                    continue
                baseline_rows: list[dict[str, Any]] = []
                entry_rows: list[dict[str, Any]] = []
                for row in selected:
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("status", "")).lower() != "armed":
                        continue
                    if not profile_matches(row, profile):
                        continue
                    current_final = to_float(row.get("final_score"), 0.0)
                    if current_final < min_score:
                        continue
                    candidate_id = str(row.get("candidate_id", "")).strip()
                    if not candidate_id:
                        continue
                    armed = latest_armed.get(candidate_id)
                    if armed is not None:
                        entry_score = entry_score_from_metrics(metrics_from_armed_event(armed))
                    else:
                        entry_score = to_float(row.get("entry_score"), 0.0)
                    enriched = dict(row)
                    enriched["entry_score"] = entry_score
                    enriched["entry_augmented_score"] = current_final + (entry_weight * entry_score)
                    baseline_rows.append(enriched)
                    entry_rows.append(enriched)

                baseline_rows.sort(key=lambda r: to_float(r.get("final_score"), 0.0), reverse=True)
                entry_rows.sort(key=lambda r: to_float(r.get("entry_augmented_score"), 0.0), reverse=True)

                cycle_counts["baseline:top1"] += 1
                cycle_counts["baseline:top3"] += 1
                cycle_counts["entry:top1"] += 1
                cycle_counts["entry:top3"] += 1

                lane_map = {
                    "baseline:overall": baseline_rows,
                    "baseline:top1": baseline_rows[:1],
                    "baseline:top3": baseline_rows[:3],
                    "entry:overall": entry_rows,
                    "entry:top1": entry_rows[:1],
                    "entry:top3": entry_rows[:3],
                }
                for bucket_key, rows in lane_map.items():
                    mode_name, lane = bucket_key.split(":")
                    for row in rows:
                        candidate_id = str(row.get("candidate_id", "")).strip()
                        if not candidate_id:
                            continue
                        key = pending_key(mode_name, lane, candidate_id)
                        if key in pending:
                            continue
                        pending[key] = PendingPaperTrade(
                            selected_ts=ts,
                            candidate_id=candidate_id,
                            coin=str(row.get("coin", "")),
                            side=str(row.get("side", "")).lower(),
                            liquidity_bucket=str(row.get("liquidity_bucket", "")),
                            side_weighting_profile=str(row.get("side_weighting_profile", "")),
                            handcrafted_score=to_float(row.get("handcrafted_score"), to_float(row.get("bounce_score"), 0.0)),
                            ml_proba=to_float(row.get("ml_proba"), 0.0),
                            final_score=to_float(
                                row.get("final_score") if mode_name == "baseline" else row.get("entry_augmented_score"),
                                0.0,
                            ),
                            behavior_tag=str(row.get("behavior_tag", "")),
                        )
                continue

            candidate_id = str(event.get("candidate_id", "")).strip()
            if not candidate_id:
                continue
            candidate_keys = [key for key in pending.keys() if key.endswith(f"|{candidate_id}")]
            if not candidate_keys:
                continue

            if kind == "touched":
                for key in candidate_keys:
                    update_post_touch_metrics(pending[key], event)
                continue
            if kind == "heartbeat" and str(event.get("candidate_status", "")).lower() == "touched":
                for key in candidate_keys:
                    update_post_touch_metrics(pending[key], event)
                continue
            if kind not in RESOLVED_EVENTS:
                continue
            for key in candidate_keys:
                trade = pending.pop(key, None)
                if trade is None:
                    continue
                mode_name, lane, _ = key.split("|", 2)
                resolved_rows[f"{mode_name}:{lane}"].append(finalize_trade(trade, event))

    report: dict[str, Any] = {
        "profile": profile,
        "fresh_hours": fresh_hours,
        "min_score": min_score,
        "entry_weight": entry_weight,
        "modes": {
            "baseline": {
                "overall": summarize_rows(resolved_rows["baseline:overall"]),
                "top_1_per_cycle": {
                    "cycles": cycle_counts["baseline:top1"],
                    **summarize_rows(resolved_rows["baseline:top1"]),
                },
                "top_3_per_cycle": {
                    "cycles": cycle_counts["baseline:top3"],
                    **summarize_rows(resolved_rows["baseline:top3"]),
                },
            },
            "entry_augmented": {
                "overall": summarize_rows(resolved_rows["entry:overall"]),
                "top_1_per_cycle": {
                    "cycles": cycle_counts["entry:top1"],
                    **summarize_rows(resolved_rows["entry:top1"]),
                },
                "top_3_per_cycle": {
                    "cycles": cycle_counts["entry:top3"],
                    **summarize_rows(resolved_rows["entry:top3"]),
                },
            },
        },
    }

    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate entry timing score as a separate layer on pre-touch snapshots")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--profile", default="trash_ask", choices=("trash_ask", "trash_bid"))
    parser.add_argument("--fresh-hours", type=float, default=24.0)
    parser.add_argument("--min-score", type=float, default=160.0)
    parser.add_argument("--entry-weight", type=float, default=0.12)
    parser.add_argument("--out-json", default="data/reports/entry_timing_report_trash_ask.json")
    args = parser.parse_args()

    report = run_report(
        log_path=Path(args.log_path),
        profile=args.profile,
        fresh_hours=args.fresh_hours,
        min_score=args.min_score,
        entry_weight=args.entry_weight,
        out_json=Path(args.out_json),
    )
    for mode_name in ("baseline", "entry_augmented"):
        mode = report["modes"][mode_name]
        overall = mode["overall"]
        top1 = mode["top_1_per_cycle"]
        top3 = mode["top_3_per_cycle"]
        print(
            f"[{mode_name}] overall count={int(overall['count'])} "
            f"WR={overall['wr_pct']:.2f}% exp={overall['expectancy_pct']:.4f}% "
            f"avg_MFE={overall['avg_mfe_pct']:.4f}% avg_MAE={overall['avg_mae_pct']:.4f}%"
        )
        print(
            f"[{mode_name}] top1 cycles={int(top1['cycles'])} count={int(top1['count'])} "
            f"WR={top1['wr_pct']:.2f}% exp={top1['expectancy_pct']:.4f}% "
            f"avg_MFE={top1['avg_mfe_pct']:.4f}% avg_MAE={top1['avg_mae_pct']:.4f}%"
        )
        print(
            f"[{mode_name}] top3 cycles={int(top3['cycles'])} count={int(top3['count'])} "
            f"WR={top3['wr_pct']:.2f}% exp={top3['expectancy_pct']:.4f}% "
            f"avg_MFE={top3['avg_mfe_pct']:.4f}% avg_MAE={top3['avg_mae_pct']:.4f}%"
        )


if __name__ == "__main__":
    main()
