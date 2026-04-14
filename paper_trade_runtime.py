from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


VALID_PROFILES = {"trash_ask": "ask", "trash_bid": "bid"}
RESOLVED_EVENTS = {"bounced", "failed_breakout", "failed_breakdown", "expired"}


@dataclass
class PendingPaperTrade:
    selected_ts: float
    candidate_id: str
    coin: str
    side: str
    liquidity_bucket: str
    side_weighting_profile: str
    handcrafted_score: float
    ml_proba: float
    final_score: float
    behavior_tag: str
    touched_ts: float | None = None
    touch_price: float = 0.0
    last_mid: float = 0.0
    touch_mfe_pct: float = 0.0
    touch_mae_pct: float = 0.0
    best_favorable_pct: float = 0.0
    best_loss_proxy_pct: float = 0.0
    touch_best_mid: float = 0.0
    touch_worst_mid: float = 0.0


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_json_lines_from_bytes(blob: bytes) -> list[dict[str, Any]]:
    if not blob:
        return []
    text = blob.decode("utf-8", errors="ignore")
    lines = text.splitlines()
    if lines and not lines[0].lstrip().startswith("{"):
        lines = lines[1:]
    rows: list[dict[str, Any]] = []
    for line in lines:
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


def read_tail_events(path: Path, max_bytes: int) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], 0
    with path.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        start = max(0, size - max(0, max_bytes))
        f.seek(start)
        blob = f.read()
    return safe_json_lines_from_bytes(blob), size


def follow_new_events(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], offset
    with path.open("rb") as f:
        f.seek(0, 2)
        end = f.tell()
        if end < offset:
            offset = 0
        f.seek(offset)
        blob = f.read()
        offset = f.tell()
    return safe_json_lines_from_bytes(blob), offset


def profile_matches(row: dict[str, Any], profile: str) -> bool:
    expected_side = VALID_PROFILES[profile]
    side = str(row.get("side", "")).lower()
    if side != expected_side:
        return False
    bucket = str(row.get("liquidity_bucket", "")).lower()
    if bucket != "trash":
        return False
    return True


def weighting_profile_for_row(row: dict[str, Any]) -> str:
    side = str(row.get("side", "")).lower()
    bucket = str(row.get("liquidity_bucket", "trash")).lower() or "trash"
    return str(row.get("side_weighting_profile") or f"{side}_{bucket}")


def handcrafted_for_row(row: dict[str, Any]) -> float:
    return to_float(row.get("handcrafted_score"), to_float(row.get("bounce_score"), 0.0))


def favorable_move_pct(row: dict[str, Any]) -> float:
    side = str(row.get("side", "")).lower()
    direction = 1.0 if side == "bid" else -1.0 if side == "ask" else 0.0
    touch_price = to_float(row.get("touch_price"), 0.0)
    exit_mid = to_float(row.get("exit_mid"), to_float(row.get("last_mid"), 0.0))
    if touch_price <= 0 or exit_mid <= 0 or direction == 0.0:
        return 0.0
    raw_move_pct = ((exit_mid - touch_price) / touch_price) * 100.0
    return raw_move_pct * direction


def raw_adverse_move_pct(row: dict[str, Any], favorable_pct: float) -> float:
    return max(0.0, -favorable_pct)


def directional_move_pct(side: str, touch_price: float, mid_price: float) -> float:
    direction = 1.0 if side == "bid" else -1.0 if side == "ask" else 0.0
    if touch_price <= 0 or mid_price <= 0 or direction == 0.0:
        return 0.0
    raw_move_pct = ((mid_price - touch_price) / touch_price) * 100.0
    return raw_move_pct * direction


def loss_proxy_pct(row: dict[str, Any], favorable_pct: float) -> float:
    adverse_pct = raw_adverse_move_pct(row, favorable_pct)
    return max(
        0.0,
        to_float(row.get("touch_mae_pct"), 0.0),
        adverse_pct,
    )


def update_post_touch_metrics(trade: PendingPaperTrade, event: dict[str, Any]) -> None:
    if trade.touched_ts is None and str(event.get("event", "")).lower() == "touched":
        trade.touched_ts = to_float(event.get("ts"), 0.0)
    if trade.touch_price <= 0:
        trade.touch_price = to_float(event.get("touch_price"), 0.0)
    trade.last_mid = to_float(event.get("last_mid"), trade.last_mid)
    trade.touch_best_mid = max(trade.touch_best_mid, to_float(event.get("touch_best_mid"), 0.0))
    worst_mid = to_float(event.get("touch_worst_mid"), 0.0)
    if worst_mid > 0.0:
        trade.touch_worst_mid = worst_mid if trade.touch_worst_mid <= 0.0 else min(trade.touch_worst_mid, worst_mid)
    trade.touch_mfe_pct = max(trade.touch_mfe_pct, to_float(event.get("touch_mfe_pct"), 0.0))
    trade.touch_mae_pct = max(trade.touch_mae_pct, to_float(event.get("touch_mae_pct"), 0.0))
    favorable_pct = favorable_move_pct(event)
    best_mid_favorable = directional_move_pct(trade.side, trade.touch_price, trade.touch_best_mid)
    worst_mid_favorable = directional_move_pct(trade.side, trade.touch_price, trade.touch_worst_mid)
    trade.best_favorable_pct = max(trade.best_favorable_pct, favorable_pct, trade.touch_mfe_pct, best_mid_favorable)
    trade.best_loss_proxy_pct = max(
        trade.best_loss_proxy_pct,
        raw_adverse_move_pct(event, favorable_pct),
        trade.touch_mae_pct,
        max(0.0, -worst_mid_favorable),
    )


def select_snapshot_rows(
    snapshot: dict[str, Any],
    profile: str,
    entry_mode: str,
    min_final_score: float,
) -> list[dict[str, Any]]:
    selected = snapshot.get("selected")
    if not isinstance(selected, list):
        return []
    filtered = []
    for row in selected:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "armed":
            continue
        if not profile_matches(row, profile):
            continue
        final_score = to_float(row.get("final_score"), 0.0)
        if final_score < min_final_score:
            continue
        filtered.append(row)
    filtered.sort(key=lambda item: to_float(item.get("final_score"), 0.0), reverse=True)
    if entry_mode == "top3":
        return filtered[:3]
    return filtered[:1]


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_existing_emit_keys(path: Path) -> set[tuple[str, float, str]]:
    keys: set[tuple[str, float, str]] = set()
    if not path.exists():
        return keys
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(item, dict):
                continue
            candidate_id = str(item.get("candidate_id", "")).strip()
            selected_ts = to_float(item.get("ts"), 0.0)
            outcome = str(item.get("outcome", "")).lower()
            if candidate_id and outcome:
                keys.add((candidate_id, selected_ts, outcome))
    return keys


def process_event(
    event: dict[str, Any],
    *,
    profile: str,
    entry_mode: str,
    min_final_score: float,
    pending: dict[str, PendingPaperTrade],
    paper_log_path: Path,
    counters: dict[str, int],
    emitted: set[tuple[str, float, str]],
) -> None:
    kind = str(event.get("event", "")).lower()
    if kind == "selection_snapshot_pre_touch":
        rows = select_snapshot_rows(event, profile, entry_mode, min_final_score)
        for row in rows:
            candidate_id = str(row.get("candidate_id", "")).strip()
            if not candidate_id or candidate_id in pending:
                continue
            pending[candidate_id] = PendingPaperTrade(
                selected_ts=to_float(event.get("ts"), 0.0),
                candidate_id=candidate_id,
                coin=str(row.get("coin", "")),
                side=str(row.get("side", "")).lower(),
                liquidity_bucket=str(row.get("liquidity_bucket", "")),
                side_weighting_profile=weighting_profile_for_row(row),
                handcrafted_score=handcrafted_for_row(row),
                ml_proba=to_float(row.get("ml_proba"), 0.0),
                final_score=to_float(row.get("final_score"), 0.0),
                behavior_tag=str(row.get("behavior_tag", "")),
            )
            counters["selected"] += 1
        return

    candidate_id = str(event.get("candidate_id", "")).strip()
    if not candidate_id or candidate_id not in pending:
        return

    trade = pending[candidate_id]

    if kind == "touched":
        update_post_touch_metrics(trade, event)
        counters["touched"] += 1
        return

    if kind == "heartbeat" and str(event.get("candidate_status", "")).lower() == "touched":
        update_post_touch_metrics(trade, event)
        return

    if kind not in RESOLVED_EVENTS:
        return

    emit_key = (trade.candidate_id, trade.selected_ts, kind)
    if emit_key in emitted:
        pending.pop(candidate_id, None)
        return

    update_post_touch_metrics(trade, event)
    favorable_pct = max(favorable_move_pct(event), trade.best_favorable_pct, trade.touch_mfe_pct)
    loss_pct = max(loss_proxy_pct(event, favorable_pct), trade.best_loss_proxy_pct, trade.touch_mae_pct)
    row = {
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
        "outcome": kind,
        "favorable_move_pct": favorable_pct,
        "loss_proxy_pct": loss_pct,
        "touch_mfe_pct": max(to_float(event.get("touch_mfe_pct"), 0.0), trade.touch_mfe_pct),
        "touch_mae_pct": max(to_float(event.get("touch_mae_pct"), 0.0), trade.touch_mae_pct),
    }
    append_jsonl(paper_log_path, row)
    emitted.add(emit_key)
    counters["resolved"] += 1
    pending.pop(candidate_id, None)


def resolve_default_paper_log(profile: str) -> Path:
    return Path("data") / f"paper_trade_{profile}.jsonl"


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper-trade runtime built on selection_snapshot_pre_touch events")
    parser.add_argument("--events-log", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--paper-log", default="")
    parser.add_argument("--profile", choices=sorted(VALID_PROFILES), default="trash_ask")
    parser.add_argument("--entry-mode", choices=["top1", "top3"], default="top1")
    parser.add_argument("--min-final-score", type=float, default=160.0)
    parser.add_argument("--bootstrap-bytes", type=int, default=8_000_000)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--run-seconds", type=float, default=30.0)
    args = parser.parse_args()

    events_path = Path(args.events_log)
    paper_log_path = Path(args.paper_log) if args.paper_log else resolve_default_paper_log(args.profile)
    pending: dict[str, PendingPaperTrade] = {}
    counters = {"selected": 0, "touched": 0, "resolved": 0}
    emitted = load_existing_emit_keys(paper_log_path)

    bootstrap_events, offset = read_tail_events(events_path, args.bootstrap_bytes)
    for event in bootstrap_events:
        process_event(
            event,
            profile=args.profile,
            entry_mode=args.entry_mode,
            min_final_score=args.min_final_score,
            pending=pending,
            paper_log_path=paper_log_path,
            counters=counters,
            emitted=emitted,
        )

    print(
        f"[paper-trade] profile={args.profile} entry_mode={args.entry_mode} "
        f"min_final_score={args.min_final_score:.1f} bootstrap_events={len(bootstrap_events)} "
        f"pending={len(pending)} selected={counters['selected']} resolved={counters['resolved']}"
    )

    started = time.time()
    while True:
        if args.run_seconds > 0 and (time.time() - started) >= args.run_seconds:
            break
        time.sleep(max(0.1, args.poll_sec))
        new_events, offset = follow_new_events(events_path, offset)
        for event in new_events:
            process_event(
                event,
                profile=args.profile,
                entry_mode=args.entry_mode,
                min_final_score=args.min_final_score,
                pending=pending,
                paper_log_path=paper_log_path,
                counters=counters,
                emitted=emitted,
            )

    print(
        f"[paper-trade] done profile={args.profile} selected={counters['selected']} "
        f"touched={counters['touched']} resolved={counters['resolved']} pending={len(pending)} "
        f"log={paper_log_path}"
    )


if __name__ == "__main__":
    main()
