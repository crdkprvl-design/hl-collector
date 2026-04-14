from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import joblib
except Exception as exc:  # noqa: BLE001
    raise SystemExit(
        "Missing ML deps. Install: pip install joblib scikit-learn\n"
        f"Import error: {exc}"
    )

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


@dataclass
class ModeConfig:
    name: str
    score_key: str
    blend_weight_current: float
    blend_weight_tradable: float


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_feature_row(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "market": str(event.get("market", "")),
        "side": str(event.get("side", "")),
        "liquidity_bucket": str(event.get("liquidity_bucket", "")),
        "wall_ratio": max(0.0, to_float(event.get("wall_ratio"), 0.0)),
        "wall_dominance_ratio": max(0.0, to_float(event.get("wall_dominance_ratio"), 0.0)),
        "wall_notional_usd": max(0.0, to_float(event.get("wall_notional_usd"), 0.0)),
        "wall_distance_from_spread_pct": max(0.0, to_float(event.get("wall_distance_from_spread_pct"), 0.0)),
        "day_volume_usd": max(0.0, to_float(event.get("day_volume_usd"), 0.0)),
        "visible_age_sec": max(0.0, to_float(event.get("visible_age_sec"), 0.0)),
        "seen_count": max(0.0, to_float(event.get("seen_count"), 0.0)),
        "wall_notional_stability_ratio": max(0.0, to_float(event.get("wall_notional_stability_ratio"), 0.0)),
        "round_level_score": max(0.0, to_float(event.get("round_level_score"), 0.0)),
        "wall_notional_volatility_ratio": max(0.0, to_float(event.get("wall_notional_volatility_ratio"), 0.0)),
        "distance_volatility_ratio": max(0.0, to_float(event.get("distance_volatility_ratio"), 0.0)),
        "pre_touch_decay_ratio": max(0.0, to_float(event.get("pre_touch_decay_ratio"), 1.0)),
        "distance_compression_ratio": max(0.0, to_float(event.get("distance_compression_ratio"), 1.0)),
        "rebuild_count": max(0.0, to_float(event.get("rebuild_count"), 0.0)),
        "touch_survival_sec": max(0.0, to_float(event.get("touch_survival_sec"), 0.0)),
        "touch_attempt_count": max(0.0, to_float(event.get("touch_attempt_count"), 0.0)),
        "updates_before_touch": max(0.0, to_float(event.get("updates_before_touch"), 0.0)),
        "approach_speed_pct_per_sec": max(0.0, to_float(event.get("approach_speed_pct_per_sec"), 0.0)),
    }


def predict_tradable_proba(model: Any, feature_row: dict[str, Any]) -> float:
    try:
        value = model.predict_proba([feature_row])[0][1]
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return 0.0


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


def select_rows_for_lane(
    snapshot: dict[str, Any],
    *,
    profile: str,
    min_score: float,
    latest_armed: dict[str, dict[str, Any]],
    tradable_model: Any,
    mode: ModeConfig,
    lane: str,
) -> list[dict[str, Any]]:
    selected = snapshot.get("selected")
    if not isinstance(selected, list):
        return []
    eligible: list[dict[str, Any]] = []
    for row in selected:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "armed":
            continue
        if not profile_matches(row, profile):
            continue
        candidate_id = str(row.get("candidate_id", "")).strip()
        if not candidate_id:
            continue
        current_final = to_float(row.get("final_score"), 0.0)
        if current_final < min_score:
            continue
        tradable_proba = 0.0
        armed = latest_armed.get(candidate_id)
        if armed is not None:
            tradable_proba = predict_tradable_proba(tradable_model, build_feature_row(armed))
        ab_score = (
            mode.blend_weight_current * current_final
            + mode.blend_weight_tradable * (tradable_proba * 100.0)
        )
        scored = dict(row)
        scored["current_final_score"] = current_final
        scored["tradable_proba"] = tradable_proba
        scored["ab_final_score"] = ab_score
        score_value = current_final if mode.score_key == "current_final_score" else ab_score
        scored["mode_score"] = score_value
        eligible.append(scored)
    eligible.sort(key=lambda item: to_float(item.get("mode_score"), 0.0), reverse=True)
    if lane == "top1":
        return eligible[:1]
    if lane == "top3":
        return eligible[:3]
    return eligible


def run_compare(
    *,
    log_path: Path,
    tradable_model_path: Path,
    profile: str,
    fresh_hours: float,
    min_score: float,
    out_json: Path | None,
) -> dict[str, Any]:
    tradable_model = joblib.load(tradable_model_path)
    last_ts = read_last_ts(log_path)
    cutoff_ts = last_ts - (fresh_hours * 3600.0)

    modes = [
        ModeConfig(name="A", score_key="current_final_score", blend_weight_current=1.0, blend_weight_tradable=0.0),
        ModeConfig(name="B", score_key="ab_final_score", blend_weight_current=0.80, blend_weight_tradable=0.20),
    ]
    lanes = ("overall", "top1", "top3")

    latest_armed: dict[str, dict[str, Any]] = {}
    pending: dict[str, PendingPaperTrade] = {}
    resolved_rows: dict[str, list[dict[str, Any]]] = {f"{mode.name}:{lane}": [] for mode in modes for lane in lanes}
    cycle_counts: dict[str, int] = {f"{mode.name}:{lane}": 0 for mode in modes for lane in ("top1", "top3")}

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
                for mode in modes:
                    overall_rows = select_rows_for_lane(
                        event,
                        profile=profile,
                        min_score=min_score,
                        latest_armed=latest_armed,
                        tradable_model=tradable_model,
                        mode=mode,
                        lane="overall",
                    )
                    top1_rows = overall_rows[:1]
                    top3_rows = overall_rows[:3]
                    cycle_counts[f"{mode.name}:top1"] += 1
                    cycle_counts[f"{mode.name}:top3"] += 1
                    for lane, rows in (("overall", overall_rows), ("top1", top1_rows), ("top3", top3_rows)):
                        for row in rows:
                            candidate_id = str(row.get("candidate_id", "")).strip()
                            if not candidate_id:
                                continue
                            key = pending_key(mode.name, lane, candidate_id)
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
                                final_score=to_float(row.get("mode_score"), 0.0),
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
        "mode_b_blend": {
            "current_live_blend_weight": 0.80,
            "tradable_soft_rank_weight": 0.20,
        },
        "modes": {},
    }
    for mode in modes:
        overall = summarize_rows(resolved_rows[f"{mode.name}:overall"])
        top1 = summarize_rows(resolved_rows[f"{mode.name}:top1"])
        top3 = summarize_rows(resolved_rows[f"{mode.name}:top3"])
        report["modes"][mode.name] = {
            "overall": overall,
            "top_1_per_cycle": {
                "cycles": cycle_counts[f"{mode.name}:top1"],
                **top1,
            },
            "top_3_per_cycle": {
                "cycles": cycle_counts[f"{mode.name}:top3"],
                **top3,
            },
        }

    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="A/B paper-trade compare current live-blend vs tradable-assisted blend")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--tradable-model-path", default="data/models/tradable_bounce_model_075_020.joblib")
    parser.add_argument("--profile", default="trash_ask", choices=("trash_ask", "trash_bid"))
    parser.add_argument("--fresh-hours", type=float, default=24.0)
    parser.add_argument("--min-score", type=float, default=160.0)
    parser.add_argument("--out-json", default="data/reports/ab_paper_trade_compare_trash_ask.json")
    args = parser.parse_args()

    report = run_compare(
        log_path=Path(args.log_path),
        tradable_model_path=Path(args.tradable_model_path),
        profile=args.profile,
        fresh_hours=args.fresh_hours,
        min_score=args.min_score,
        out_json=Path(args.out_json),
    )
    for mode_name in ("A", "B"):
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
