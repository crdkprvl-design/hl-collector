from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

from density_screener import entry_score_from_metrics
from entry_timing_report import finalize_trade, metrics_from_armed_event, summarize_rows
from paper_trade_runtime import (
    PendingPaperTrade,
    RESOLVED_EVENTS,
    profile_matches,
    to_float,
    update_post_touch_metrics,
)
from rolling_stability_report import read_last_ts


WINDOWS_HOURS = {
    "24h": 24.0,
    "3d": 72.0,
    "7d": 168.0,
}


def pending_key(window_label: str, mode_name: str, lane: str, candidate_id: str) -> str:
    return f"{window_label}|{mode_name}|{lane}|{candidate_id}"


def event_fingerprint(event: dict[str, Any]) -> str:
    payload = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(payload.encode("utf-8", errors="ignore"), digest_size=16).hexdigest()


def resolve_source_paths(log_path: Path, cloud_glob: str) -> list[Path]:
    paths: list[Path] = []
    if cloud_glob:
        for path in sorted(Path(".").glob(cloud_glob)):
            if path.is_file():
                paths.append(path)
    if log_path.exists():
        paths.append(log_path)
    return paths


def read_last_ts_from_path(path: Path) -> float:
    if not path.exists():
        return 0.0
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
            if not buf.strip():
                return 0.0
            last = buf.strip().splitlines()[-1]
    try:
        obj = json.loads(last.decode("utf-8", errors="ignore"))
    except Exception:
        return 0.0
    return to_float(obj.get("ts"), 0.0)


def read_last_ts_from_sources(paths: list[Path]) -> float:
    last_ts = 0.0
    for path in paths:
        last_ts = max(last_ts, read_last_ts_from_path(path))
    return last_ts


def build_trade_from_row(
    *,
    ts: float,
    row: dict[str, Any],
    score_value: float,
) -> PendingPaperTrade:
    return PendingPaperTrade(
        selected_ts=ts,
        candidate_id=str(row.get("candidate_id", "")).strip(),
        coin=str(row.get("coin", "")),
        side=str(row.get("side", "")).lower(),
        liquidity_bucket=str(row.get("liquidity_bucket", "")),
        side_weighting_profile=str(row.get("side_weighting_profile", "")),
        handcrafted_score=to_float(row.get("handcrafted_score"), to_float(row.get("bounce_score"), 0.0)),
        ml_proba=to_float(row.get("ml_proba"), 0.0),
        final_score=score_value,
        behavior_tag=str(row.get("behavior_tag", "")),
    )


def sort_snapshot_rows(
    rows: list[dict[str, Any]],
    *,
    mode_name: str,
) -> list[dict[str, Any]]:
    if mode_name == "baseline":
        return sorted(rows, key=lambda r: to_float(r.get("final_score"), 0.0), reverse=True)
    return sorted(rows, key=lambda r: to_float(r.get("entry_augmented_score"), 0.0), reverse=True)


def prepare_snapshot_rows(
    event: dict[str, Any],
    *,
    profile: str,
    min_score: float,
    entry_weight: float,
    latest_armed: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    selected = event.get("selected")
    if not isinstance(selected, list):
        return []
    prepared: list[dict[str, Any]] = []
    for row in selected:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "armed":
            continue
        if not profile_matches(row, profile):
            continue
        base_score = to_float(row.get("final_score"), 0.0)
        if base_score < min_score:
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
        enriched["entry_augmented_score"] = base_score + (entry_weight * entry_score)
        prepared.append(enriched)
    return prepared


def increment_cycles(
    cycle_counts: dict[str, dict[str, dict[str, int]]],
    *,
    window_label: str,
) -> None:
    cycle_counts[window_label]["baseline"]["top_1_per_cycle"] += 1
    cycle_counts[window_label]["baseline"]["top_3_per_cycle"] += 1
    cycle_counts[window_label]["entry_augmented"]["top_1_per_cycle"] += 1
    cycle_counts[window_label]["entry_augmented"]["top_3_per_cycle"] += 1


def run_report(
    *,
    log_path: Path,
    cloud_glob: str,
    profile: str,
    min_score: float,
    entry_weight: float,
    out_json: Path | None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    source_paths = resolve_source_paths(log_path, cloud_glob)
    last_ts = read_last_ts_from_sources(source_paths)
    cutoffs = {label: last_ts - (hours * 3600.0) for label, hours in WINDOWS_HOURS.items()}
    min_cutoff = min(cutoffs.values())

    latest_armed: dict[str, dict[str, Any]] = {}
    pending: dict[str, PendingPaperTrade] = {}
    pending_by_candidate: dict[str, set[str]] = {}
    cycle_counts: dict[str, dict[str, dict[str, int]]] = {
        label: {
            "baseline": {"top_1_per_cycle": 0, "top_3_per_cycle": 0},
            "entry_augmented": {"top_1_per_cycle": 0, "top_3_per_cycle": 0},
        }
        for label in WINDOWS_HOURS
    }
    resolved_rows: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {
        label: {
            "baseline": {"overall": [], "top_1_per_cycle": [], "top_3_per_cycle": []},
            "entry_augmented": {"overall": [], "top_1_per_cycle": [], "top_3_per_cycle": []},
        }
        for label in WINDOWS_HOURS
    }

    lines_seen = 0
    relevant_events_seen: set[str] = set()
    duplicate_events_skipped = 0
    unique_relevant_events = 0
    for source_path in source_paths:
        with source_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                lines_seen += 1
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue

                kind = str(event.get("event", "")).lower()
                ts = to_float(event.get("ts"), 0.0)
                candidate_status = str(event.get("candidate_status", "")).lower()

                dedupe_relevant = (
                    kind in {"selection_snapshot_pre_touch", "selection_snapshot", "touched"}
                    or kind in RESOLVED_EVENTS
                    or (kind == "heartbeat" and candidate_status in {"armed", "touched"})
                )
                if dedupe_relevant and ts >= min_cutoff:
                    fingerprint = event_fingerprint(event)
                    if fingerprint in relevant_events_seen:
                        duplicate_events_skipped += 1
                        continue
                    relevant_events_seen.add(fingerprint)
                    unique_relevant_events += 1

                if kind == "heartbeat" and candidate_status == "armed":
                    candidate_id = str(event.get("candidate_id", "")).strip()
                    if candidate_id:
                        latest_armed[candidate_id] = event
                    continue

                if kind == "selection_snapshot_pre_touch":
                    if ts < min_cutoff:
                        continue
                    prepared_rows = prepare_snapshot_rows(
                        event,
                        profile=profile,
                        min_score=min_score,
                        entry_weight=entry_weight,
                        latest_armed=latest_armed,
                    )
                    if not prepared_rows:
                        continue
                    baseline_rows = sort_snapshot_rows(prepared_rows, mode_name="baseline")
                    entry_rows = sort_snapshot_rows(prepared_rows, mode_name="entry_augmented")

                    mode_rows = {
                        "baseline": {
                            "overall": baseline_rows,
                            "top_1_per_cycle": baseline_rows[:1],
                            "top_3_per_cycle": baseline_rows[:3],
                        },
                        "entry_augmented": {
                            "overall": entry_rows,
                            "top_1_per_cycle": entry_rows[:1],
                            "top_3_per_cycle": entry_rows[:3],
                        },
                    }

                    for window_label, cutoff in cutoffs.items():
                        if ts < cutoff:
                            continue
                        increment_cycles(cycle_counts, window_label=window_label)
                        for mode_name, lane_map in mode_rows.items():
                            for lane, rows in lane_map.items():
                                for row in rows:
                                    candidate_id = str(row.get("candidate_id", "")).strip()
                                    if not candidate_id:
                                        continue
                                    key = pending_key(window_label, mode_name, lane, candidate_id)
                                    if key in pending:
                                        continue
                                    score_value = to_float(
                                        row.get("final_score") if mode_name == "baseline" else row.get("entry_augmented_score"),
                                        0.0,
                                    )
                                    pending[key] = build_trade_from_row(ts=ts, row=row, score_value=score_value)
                                    pending_by_candidate.setdefault(candidate_id, set()).add(key)
                    continue

                candidate_id = str(event.get("candidate_id", "")).strip()
                if not candidate_id:
                    continue
                active_keys = pending_by_candidate.get(candidate_id)
                if not active_keys:
                    continue

                if kind == "touched":
                    for key in tuple(active_keys):
                        update_post_touch_metrics(pending[key], event)
                    continue
                if kind == "heartbeat" and candidate_status == "touched":
                    for key in tuple(active_keys):
                        update_post_touch_metrics(pending[key], event)
                    continue
                if kind not in RESOLVED_EVENTS:
                    continue

                for key in tuple(active_keys):
                    trade = pending.pop(key, None)
                    if trade is None:
                        continue
                    window_label, mode_name, lane, _ = key.split("|", 3)
                    resolved_rows[window_label][mode_name][lane].append(finalize_trade(trade, event))
                    active_keys.discard(key)
                if not active_keys:
                    pending_by_candidate.pop(candidate_id, None)

    report: dict[str, Any] = {
        "profile": profile,
        "min_score": min_score,
        "entry_weight": entry_weight,
        "last_ts": last_ts,
        "lines_seen": lines_seen,
        "duplicate_events_skipped": duplicate_events_skipped,
        "unique_relevant_events": unique_relevant_events,
        "source_paths": [str(p) for p in source_paths],
        "runtime_sec": time.perf_counter() - t0,
        "windows": {},
    }

    for window_label in ("24h", "3d", "7d"):
        report["windows"][window_label] = {}
        for mode_name in ("baseline", "entry_augmented"):
            overall = summarize_rows(resolved_rows[window_label][mode_name]["overall"])
            top1 = summarize_rows(resolved_rows[window_label][mode_name]["top_1_per_cycle"])
            top3 = summarize_rows(resolved_rows[window_label][mode_name]["top_3_per_cycle"])
            report["windows"][window_label][mode_name] = {
                "overall": overall,
                "top_1_per_cycle": {
                    "cycles": cycle_counts[window_label][mode_name]["top_1_per_cycle"],
                    **top1,
                },
                "top_3_per_cycle": {
                    "cycles": cycle_counts[window_label][mode_name]["top_3_per_cycle"],
                    **top3,
                },
            }

    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fast rolling A/B paper-trade report for baseline final_score vs entry-augmented ranking"
    )
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--cloud-glob", default="cloud_data/events_*.jsonl")
    parser.add_argument("--profile", default="trash_ask", choices=("trash_ask", "trash_bid"))
    parser.add_argument("--min-score", type=float, default=160.0)
    parser.add_argument("--entry-weight", type=float, default=0.12)
    parser.add_argument("--out-json", default="data/reports/rolling_paper_trade_ab_trash_ask.json")
    args = parser.parse_args()

    report = run_report(
        log_path=Path(args.log_path),
        cloud_glob=args.cloud_glob,
        profile=args.profile,
        min_score=args.min_score,
        entry_weight=args.entry_weight,
        out_json=Path(args.out_json),
    )

    print(f"runtime={report['runtime_sec']:.2f}s lines={int(report['lines_seen'])}")
    for label in ("24h", "3d", "7d"):
        print(f"[{label}]")
        for mode_name, title in (("baseline", "A"), ("entry_augmented", "B")):
            mode = report["windows"][label][mode_name]
            overall = mode["overall"]
            top1 = mode["top_1_per_cycle"]
            top3 = mode["top_3_per_cycle"]
            print(
                f"  mode {title} overall: count={int(overall['count'])} "
                f"WR={overall['wr_pct']:.2f}% exp={overall['expectancy_pct']:.4f}% "
                f"avg_MFE={overall['avg_mfe_pct']:.4f}% avg_MAE={overall['avg_mae_pct']:.4f}%"
            )
            print(
                f"  mode {title} top_1_per_cycle: cycles={int(top1['cycles'])} count={int(top1['count'])} "
                f"WR={top1['wr_pct']:.2f}% exp={top1['expectancy_pct']:.4f}%"
            )
            print(
                f"  mode {title} top_3_per_cycle: cycles={int(top3['cycles'])} count={int(top3['count'])} "
                f"WR={top3['wr_pct']:.2f}% exp={top3['expectancy_pct']:.4f}%"
            )


if __name__ == "__main__":
    main()
