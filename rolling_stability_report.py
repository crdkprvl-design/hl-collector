from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from analyze_signal_log import (
    build_resolved_cases,
    build_top_per_cycle_eval,
    compute_behavior_score,
    summarize_subset,
    to_float,
)
from paper_trade_runtime import (
    PendingPaperTrade,
    RESOLVED_EVENTS,
    profile_matches,
    select_snapshot_rows,
    update_post_touch_metrics,
    favorable_move_pct as paper_favorable_move_pct,
    loss_proxy_pct as paper_loss_proxy_pct,
)


def read_last_ts(path: Path) -> float:
    with path.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        block = 65536
        buf = b""
        pos = size
        while pos > 0:
            read = min(block, pos)
            pos -= read
            f.seek(pos)
            buf = f.read(read) + buf
            lines = buf.splitlines()
            if len(lines) >= 2:
                last = lines[-1]
                if not last.strip():
                    last = lines[-2]
                break
        else:
            last = buf.strip().splitlines()[-1]
    obj = json.loads(last.decode("utf-8", errors="ignore"))
    return to_float(obj.get("ts"), 0.0)


def compute_deciles(cases: list[dict[str, Any]]) -> dict[str, float]:
    if not cases:
        return {"D10_expectancy_pct": 0.0, "count": 0.0}
    scored: list[dict[str, Any]] = []
    for case in cases:
        row = dict(case)
        row["behavior_score"] = compute_behavior_score(row)
        scored.append(row)
    scored.sort(key=lambda x: to_float(x.get("behavior_score"), 0.0), reverse=True)
    bucket = max(1, len(scored) // 10)
    d10 = scored[:bucket]
    summary = summarize_subset(d10)
    return {"D10_expectancy_pct": summary["expectancy_pct"], "count": summary["count"]}


def summarize_paper_rows(rows: list[dict[str, Any]]) -> dict[str, float]:
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
    n = len(rows)
    wr = len(wins) / n
    avg_win = sum(max(0.0, to_float(r.get("favorable_move_pct"), 0.0)) for r in wins) / len(wins) if wins else 0.0
    avg_loss = sum(max(0.0, to_float(r.get("loss_proxy_pct"), 0.0)) for r in fails) / len(fails) if fails else 0.0
    expectancy = (wr * avg_win) - ((1.0 - wr) * avg_loss)
    avg_mfe = sum(max(0.0, to_float(r.get("touch_mfe_pct"), 0.0)) for r in rows) / n
    avg_mae = sum(max(0.0, to_float(r.get("touch_mae_pct"), 0.0)) for r in rows) / n
    return {
        "count": float(n),
        "wr_pct": wr * 100.0,
        "expectancy_pct": expectancy,
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
    }


def finalize_paper_trade(trade: PendingPaperTrade, event: dict[str, Any]) -> dict[str, Any]:
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


def build_paper_rows(
    path: Path,
    *,
    profile: str,
    entry_mode: str,
    min_final_score: float,
    cutoff_ts: float,
) -> list[dict[str, Any]]:
    pending: dict[str, PendingPaperTrade] = {}
    rows: list[dict[str, Any]] = []
    emitted: set[tuple[str, float, str]] = set()
    with path.open("r", encoding="utf-8", errors="ignore") as f:
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
            ts = to_float(event.get("ts"), 0.0)
            kind = str(event.get("event", "")).lower()
            if kind == "selection_snapshot_pre_touch":
                if ts < cutoff_ts:
                    continue
                selected_rows = select_snapshot_rows(event, profile, entry_mode, min_final_score)
                for row in selected_rows:
                    candidate_id = str(row.get("candidate_id", "")).strip()
                    if not candidate_id or candidate_id in pending:
                        continue
                    pending[candidate_id] = PendingPaperTrade(
                        selected_ts=ts,
                        candidate_id=candidate_id,
                        coin=str(row.get("coin", "")),
                        side=str(row.get("side", "")).lower(),
                        liquidity_bucket=str(row.get("liquidity_bucket", "")),
                        side_weighting_profile=str(row.get("side_weighting_profile") or f"{row.get('side','')}_{row.get('liquidity_bucket','')}"),
                        handcrafted_score=to_float(row.get("handcrafted_score"), to_float(row.get("bounce_score"), 0.0)),
                        ml_proba=to_float(row.get("ml_proba"), 0.0),
                        final_score=to_float(row.get("final_score"), 0.0),
                        behavior_tag=str(row.get("behavior_tag", "")),
                    )
                continue

            candidate_id = str(event.get("candidate_id", "")).strip()
            if not candidate_id or candidate_id not in pending:
                continue
            trade = pending[candidate_id]
            if kind == "touched":
                update_post_touch_metrics(trade, event)
                continue
            if kind == "heartbeat" and str(event.get("candidate_status", "")).lower() == "touched":
                update_post_touch_metrics(trade, event)
                continue
            if kind not in RESOLVED_EVENTS:
                continue
            emit_key = (trade.candidate_id, trade.selected_ts, kind)
            if emit_key in emitted:
                pending.pop(candidate_id, None)
                continue
            rows.append(finalize_paper_trade(trade, event))
            emitted.add(emit_key)
            pending.pop(candidate_id, None)
    return rows


def filter_window_cases(cases: list[dict[str, Any]], cutoff_ts: float) -> list[dict[str, Any]]:
    return [c for c in cases if to_float(c.get("resolved_ts"), 0.0) >= cutoff_ts]


def filter_window_snapshots(events: list[dict[str, Any]], cutoff_ts: float) -> list[dict[str, Any]]:
    result = []
    for e in events:
        kind = str(e.get("event", "")).lower()
        if kind not in {"selection_snapshot_pre_touch", "selection_snapshot"}:
            continue
        if to_float(e.get("ts"), 0.0) >= cutoff_ts:
            result.append(e)
    return result


def fmt_pct(value: float) -> str:
    return f"{value:.4f}%"


def run_report(path: Path, paper_threshold: float, paper_entry_mode: str) -> dict[str, Any]:
    last_ts = read_last_ts(path)
    windows = {
        "24h": last_ts - 86400.0,
        "3d": last_ts - (3 * 86400.0),
        "7d": last_ts - (7 * 86400.0),
    }

    reduced_events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
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
            if kind in {"touched", "bounced", "failed_breakout", "failed_breakdown", "expired", "selection_snapshot_pre_touch", "selection_snapshot"}:
                reduced_events.append(event)

    cases = build_resolved_cases(reduced_events)
    resolved = [c for c in cases if c.get("outcome") in {"bounced", "failed"}]

    ask_rows = build_paper_rows(
        path,
        profile="trash_ask",
        entry_mode=paper_entry_mode,
        min_final_score=paper_threshold,
        cutoff_ts=min(windows.values()),
    )
    bid_rows = build_paper_rows(
        path,
        profile="trash_bid",
        entry_mode=paper_entry_mode,
        min_final_score=paper_threshold,
        cutoff_ts=min(windows.values()),
    )

    report: dict[str, Any] = {"last_ts": last_ts, "windows": {}}
    for label, cutoff in windows.items():
        window_cases = filter_window_cases(resolved, cutoff)
        top_eval = build_top_per_cycle_eval(filter_window_snapshots(reduced_events, cutoff), window_cases)
        d10 = compute_deciles(window_cases)
        paper_ask = summarize_paper_rows([r for r in ask_rows if to_float(r.get("ts"), 0.0) >= cutoff])
        paper_bid = summarize_paper_rows([r for r in bid_rows if to_float(r.get("ts"), 0.0) >= cutoff])
        report["windows"][label] = {
            "overall": summarize_subset(window_cases),
            "top_1_per_cycle": top_eval["top_1_per_cycle"],
            "top_3_per_cycle": top_eval["top_3_per_cycle"],
            "D10_expectancy_pct": d10["D10_expectancy_pct"],
            "paper_trade_trash_ask": paper_ask,
            "paper_trade_trash_bid": paper_bid,
        }
    return report


def print_report(report: dict[str, Any]) -> None:
    for label in ("24h", "3d", "7d"):
        row = report["windows"][label]
        overall = row["overall"]
        top1 = row["top_1_per_cycle"]
        top3 = row["top_3_per_cycle"]
        ask = row["paper_trade_trash_ask"]
        bid = row["paper_trade_trash_bid"]
        print(f"\n[{label}]")
        print(
            f"overall: WR={overall['wr_pct']:.2f}% expectancy={fmt_pct(overall['expectancy_pct'])} "
            f"count={int(overall['count'])}"
        )
        print(
            f"top_1_per_cycle: WR={top1['wr_pct']:.2f}% expectancy={fmt_pct(top1['expectancy_pct'])} "
            f"count={int(top1['count'])} cycles={int(top1['cycles'])}"
        )
        print(
            f"top_3_per_cycle: WR={top3['wr_pct']:.2f}% expectancy={fmt_pct(top3['expectancy_pct'])} "
            f"count={int(top3['count'])} cycles={int(top3['cycles'])}"
        )
        print(f"D10 expectancy: {fmt_pct(row['D10_expectancy_pct'])}")
        print(
            f"paper_trade trash_ask: WR={ask['wr_pct']:.2f}% expectancy={fmt_pct(ask['expectancy_pct'])} "
            f"avg_MFE={fmt_pct(ask['avg_mfe_pct'])} avg_MAE={fmt_pct(ask['avg_mae_pct'])} count={int(ask['count'])}"
        )
        print(
            f"paper_trade trash_bid: WR={bid['wr_pct']:.2f}% expectancy={fmt_pct(bid['expectancy_pct'])} "
            f"avg_MFE={fmt_pct(bid['avg_mfe_pct'])} avg_MAE={fmt_pct(bid['avg_mae_pct'])} count={int(bid['count'])}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Rolling stability report for density bounce project")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--paper-threshold", type=float, default=160.0)
    parser.add_argument("--paper-entry-mode", choices=["top1", "top3"], default="top1")
    parser.add_argument("--out-json", default="")
    args = parser.parse_args()

    report = run_report(Path(args.log_path), args.paper_threshold, args.paper_entry_mode)
    print_report(report)
    if args.out_json:
        out = Path(args.out_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nSaved JSON: {out}")


if __name__ == "__main__":
    main()
