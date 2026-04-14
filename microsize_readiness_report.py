from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


WINDOWS = {
    "24h": timedelta(hours=24),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def load_history(path: Path, profile: str, mode: str) -> list[dict[str, Any]]:
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
            if not isinstance(item, dict):
                continue
            if str(item.get("profile", "")) != profile:
                continue
            if str(item.get("mode", "")) != mode:
                continue
            ts_local = parse_ts(str(item.get("ts_local", "")))
            if ts_local is None:
                continue
            item["_ts_local"] = ts_local
            rows.append(item)
    rows.sort(key=lambda x: x["_ts_local"])
    return rows


def avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def latest_window_metrics(latest_row: dict[str, Any], label: str) -> dict[str, Any]:
    windows = latest_row.get("windows", {})
    item = windows.get(label, {}) if isinstance(windows, dict) else {}
    if not isinstance(item, dict):
        item = {}
    return {
        "paper_trade": item.get("paper_trade", {}),
        "top_1_per_cycle": item.get("top_1_per_cycle", {}),
        "top_3_per_cycle": item.get("top_3_per_cycle", {}),
        "health": item.get("health", {}),
    }


def aggregate_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    paper_expectancies: list[float] = []
    paper_wr: list[float] = []
    paper_mfe: list[float] = []
    paper_mae: list[float] = []
    top1_expectancies: list[float] = []
    top3_expectancies: list[float] = []
    for row in rows:
        win24 = row.get("windows", {}).get("24h", {})
        if not isinstance(win24, dict):
            continue
        paper = win24.get("paper_trade", {})
        top1 = win24.get("top_1_per_cycle", {})
        top3 = win24.get("top_3_per_cycle", {})
        paper_expectancies.append(to_float(paper.get("expectancy_pct"), 0.0))
        paper_wr.append(to_float(paper.get("wr_pct"), 0.0))
        paper_mfe.append(to_float(paper.get("avg_mfe_pct"), 0.0))
        paper_mae.append(to_float(paper.get("avg_mae_pct"), 0.0))
        top1_expectancies.append(to_float(top1.get("expectancy_pct"), 0.0))
        top3_expectancies.append(to_float(top3.get("expectancy_pct"), 0.0))
    return {
        "samples": len(rows),
        "avg_24h_expectancy_pct": avg(paper_expectancies),
        "avg_24h_wr_pct": avg(paper_wr),
        "avg_24h_mfe_pct": avg(paper_mfe),
        "avg_24h_mae_pct": avg(paper_mae),
        "avg_top_1_expectancy_pct": avg(top1_expectancies),
        "avg_top_3_expectancy_pct": avg(top3_expectancies),
    }


def build_readiness(history_rows: list[dict[str, Any]], profile: str, mode: str) -> dict[str, Any]:
    now_row = history_rows[-1]
    now_ts = now_row["_ts_local"]
    first_ts = history_rows[0]["_ts_local"]
    span = now_ts - first_ts

    windows: dict[str, Any] = {}
    reasons: list[str] = []

    for label, delta in WINDOWS.items():
        cutoff = now_ts - delta
        rows = [row for row in history_rows if row["_ts_local"] >= cutoff]
        has_span = span >= delta
        sufficient = has_span and len(rows) > 0
        aggregated = aggregate_from_rows(rows)
        current = latest_window_metrics(now_row, label) if label in {"24h", "3d", "7d"} else {}
        status_reason = ""
        if not has_span:
            status_reason = "недостаточно данных / окно ещё не накоплено"
        elif not rows:
            status_reason = "недостаточно данных / окно пустое"

        windows[label] = {
            "ready_window": sufficient,
            "reason": status_reason,
            "history_samples": len(rows),
            "history_aggregate": aggregated,
            "current_metrics": current,
        }

    ready = True
    for label in ("24h", "3d", "7d"):
        win = windows[label]
        if not win["ready_window"]:
            ready = False
            reasons.append(f"{label}: {win['reason']}")
            continue
        current = win["current_metrics"]
        paper = current.get("paper_trade", {})
        top1 = current.get("top_1_per_cycle", {})
        top3 = current.get("top_3_per_cycle", {})
        if to_float(paper.get("expectancy_pct"), 0.0) <= 0.0:
            ready = False
            reasons.append(f"{label}: expectancy <= 0")
        if to_float(top1.get("expectancy_pct"), 0.0) <= 0.0:
            ready = False
            reasons.append(f"{label}: top_1 expectancy <= 0")
        if to_float(top3.get("expectancy_pct"), 0.0) <= 0.0:
            ready = False
            reasons.append(f"{label}: top_3 expectancy <= 0")

    win14 = windows["14d"]
    if not win14["ready_window"]:
        ready = False
        reasons.append(f"14d: {win14['reason']}")
    else:
        agg14 = win14["history_aggregate"]
        if to_float(agg14.get("avg_24h_expectancy_pct"), 0.0) <= 0.0:
            ready = False
            reasons.append("14d: avg 24h expectancy <= 0")
        if to_float(agg14.get("avg_top_1_expectancy_pct"), 0.0) <= 0.0:
            ready = False
            reasons.append("14d: avg top_1 expectancy <= 0")
        if to_float(agg14.get("avg_top_3_expectancy_pct"), 0.0) <= 0.0:
            ready = False
            reasons.append("14d: avg top_3 expectancy <= 0")

    status = {
        "profile": profile,
        "mode": mode,
        "history_entries": len(history_rows),
        "history_start": first_ts.isoformat(timespec="seconds"),
        "history_end": now_ts.isoformat(timespec="seconds"),
        "history_span_hours": span.total_seconds() / 3600.0,
        "READY_FOR_MICROSIZE": ready,
        "reason": "; ".join(reasons) if reasons else "все окна накоплены и метрики положительные",
        "windows": windows,
        "latest_summary_health": now_row.get("health_summary", {}),
        "latest_summary_path_hint": "data/reports/daily_paper_trade_summary_trash_ask.json",
    }
    return status


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Microsize Readiness Report")
    lines.append("")
    lines.append(f"- Profile: `{report['profile']}`")
    lines.append(f"- Mode: `{report['mode']}`")
    lines.append(f"- READY_FOR_MICROSIZE: `{str(report['READY_FOR_MICROSIZE']).lower()}`")
    lines.append(f"- Reason: `{report['reason']}`")
    lines.append(f"- History entries: `{report['history_entries']}`")
    lines.append(f"- History start: `{report['history_start']}`")
    lines.append(f"- History end: `{report['history_end']}`")
    lines.append(f"- History span hours: `{report['history_span_hours']:.2f}`")
    lines.append("")
    for label in ("24h", "3d", "7d", "14d"):
        item = report["windows"][label]
        lines.append(f"## {label}")
        lines.append("")
        lines.append(f"- ready_window: `{str(item['ready_window']).lower()}`")
        if item["reason"]:
            lines.append(f"- reason: `{item['reason']}`")
        lines.append(f"- history_samples: `{item['history_samples']}`")
        agg = item["history_aggregate"]
        lines.append(
            f"- history avg 24h expectancy: `{agg['avg_24h_expectancy_pct']:.4f}%`, "
            f"avg top_1 expectancy: `{agg['avg_top_1_expectancy_pct']:.4f}%`, "
            f"avg top_3 expectancy: `{agg['avg_top_3_expectancy_pct']:.4f}%`"
        )
        current = item.get("current_metrics", {})
        if current:
            paper = current.get("paper_trade", {})
            top1 = current.get("top_1_per_cycle", {})
            top3 = current.get("top_3_per_cycle", {})
            lines.append(
                f"- current paper_trade: WR `{to_float(paper.get('wr_pct'), 0.0):.2f}%`, "
                f"expectancy `{to_float(paper.get('expectancy_pct'), 0.0):.4f}%`, "
                f"avg MFE `{to_float(paper.get('avg_mfe_pct'), 0.0):.4f}%`, "
                f"avg MAE `{to_float(paper.get('avg_mae_pct'), 0.0):.4f}%`"
            )
            lines.append(
                f"- current top_1: WR `{to_float(top1.get('wr_pct'), 0.0):.2f}%`, "
                f"expectancy `{to_float(top1.get('expectancy_pct'), 0.0):.4f}%`"
            )
            lines.append(
                f"- current top_3: WR `{to_float(top3.get('wr_pct'), 0.0):.2f}%`, "
                f"expectancy `{to_float(top3.get('expectancy_pct'), 0.0):.4f}%`"
            )
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Microsize readiness report from accumulated daily summary history.")
    parser.add_argument("--history-path", default="data/reports/daily_metrics_history.jsonl")
    parser.add_argument("--profile", default="trash_ask")
    parser.add_argument("--mode", default="B")
    parser.add_argument("--out-json", default="data/reports/microsize_readiness_trash_ask.json")
    parser.add_argument("--out-md", default="data/reports/microsize_readiness_trash_ask.md")
    args = parser.parse_args()

    history_rows = load_history(Path(args.history_path), args.profile, args.mode)
    if not history_rows:
        report = {
            "profile": args.profile,
            "mode": args.mode,
            "history_entries": 0,
            "READY_FOR_MICROSIZE": False,
            "reason": "недостаточно данных / история отсутствует",
            "windows": {
                label: {
                    "ready_window": False,
                    "reason": "недостаточно данных / окно ещё не накоплено",
                    "history_samples": 0,
                    "history_aggregate": {},
                    "current_metrics": {},
                }
                for label in WINDOWS
            },
        }
    else:
        report = build_readiness(history_rows, args.profile, args.mode)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(render_markdown(report), encoding="utf-8")

    print(
        f"READY_FOR_MICROSIZE={str(report['READY_FOR_MICROSIZE']).lower()} "
        f"reason={report['reason']} "
        f"json={out_json} md={out_md}"
    )


if __name__ == "__main__":
    main()
