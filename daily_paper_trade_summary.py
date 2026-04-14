from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from rolling_paper_trade_ab_report import run_report


def fmt_health_flag(ok: bool, good_text: str, bad_text: str) -> str:
    return good_text if ok else bad_text


def build_health(summary: dict) -> dict:
    windows = summary["windows"]
    order = ["24h", "3d", "7d"]
    health: dict[str, dict] = {}
    previous_label: str | None = None
    for label in order:
        mode_b = windows[label]["paper_trade"]
        mode_a = windows[label]["baseline_paper_trade"]
        count = float(mode_b.get("count", 0.0))
        expectancy = float(mode_b.get("expectancy_pct", 0.0))
        mae = float(mode_b.get("avg_mae_pct", 0.0))
        health_row = {
            "expectancy_status": fmt_health_flag(expectancy > 0.0, "positive", "negative"),
            "mode_b_vs_a": fmt_health_flag(
                expectancy >= float(mode_a.get("expectancy_pct", 0.0)),
                "better_or_equal",
                "worse",
            ),
            "avg_mae_vs_prev_window": "n/a",
            "count_vs_prev_window": "n/a",
        }
        if previous_label is not None:
            prev = windows[previous_label]["paper_trade"]
            prev_mae = float(prev.get("avg_mae_pct", 0.0))
            prev_count = float(prev.get("count", 0.0))
            prev_days = {"24h": 1.0, "3d": 3.0, "7d": 7.0}[previous_label]
            curr_days = {"24h": 1.0, "3d": 3.0, "7d": 7.0}[label]
            prev_daily_count = prev_count / prev_days if prev_days > 0 else 0.0
            curr_daily_count = count / curr_days if curr_days > 0 else 0.0
            health_row["avg_mae_vs_prev_window"] = fmt_health_flag(
                mae <= prev_mae,
                "stable_or_lower",
                "higher",
            )
            health_row["count_vs_prev_window"] = fmt_health_flag(
                curr_daily_count >= (prev_daily_count * 0.65),
                "stable",
                "sharp_drop",
            )
        health[label] = health_row
        previous_label = label
    return health


def render_markdown(summary: dict) -> str:
    lines: list[str] = []
    lines.append(f"# Daily Paper-Trade Summary")
    lines.append("")
    lines.append(f"- Profile: `{summary['baseline_profile']}`")
    lines.append(f"- Mode: `{summary['baseline_mode']}`")
    lines.append(f"- Formula: `{summary['score_formula']}`")
    lines.append(f"- Min score: `{summary['min_score']}`")
    lines.append(f"- Entry weight: `{summary['entry_weight']}`")
    lines.append(f"- Runtime: `{summary['runtime_sec']:.2f}s`")
    lines.append("")
    for label in ("24h", "3d", "7d"):
        window = summary["windows"][label]
        paper = window["paper_trade"]
        top1 = window["top_1_per_cycle"]
        top3 = window["top_3_per_cycle"]
        health = window["health"]
        lines.append(f"## {label}")
        lines.append("")
        lines.append(
            f"- Paper trade: count `{int(paper['count'])}`, WR `{paper['wr_pct']:.2f}%`, "
            f"expectancy `{paper['expectancy_pct']:.4f}%`, avg MFE `{paper['avg_mfe_pct']:.4f}%`, "
            f"avg MAE `{paper['avg_mae_pct']:.4f}%`"
        )
        lines.append(
            f"- Top 1 per cycle: cycles `{int(top1['cycles'])}`, count `{int(top1['count'])}`, "
            f"WR `{top1['wr_pct']:.2f}%`, expectancy `{top1['expectancy_pct']:.4f}%`"
        )
        lines.append(
            f"- Top 3 per cycle: cycles `{int(top3['cycles'])}`, count `{int(top3['count'])}`, "
            f"WR `{top3['wr_pct']:.2f}%`, expectancy `{top3['expectancy_pct']:.4f}%`"
        )
        lines.append(
            f"- Health: expectancy `{health['expectancy_status']}`, "
            f"mode B vs A `{health['mode_b_vs_a']}`, "
            f"avg MAE vs prev `{health['avg_mae_vs_prev_window']}`, "
            f"count vs prev `{health['count_vs_prev_window']}`"
        )
        lines.append("")
    return "\n".join(lines)


def append_history(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts_local": datetime.now().isoformat(timespec="seconds"),
        "profile": summary["baseline_profile"],
        "mode": summary["baseline_mode"],
        "score_formula": summary["score_formula"],
        "min_score": summary["min_score"],
        "entry_weight": summary["entry_weight"],
        "runtime_sec": summary["runtime_sec"],
        "sync_status": summary.get("sync_status", "unknown"),
        "collector_status": summary.get("collector_status", "unknown"),
        "health_summary": summary.get("health_summary", {}),
        "windows": summary["windows"],
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily operational summary for the current paper-trade baseline (trash_ask, mode B)"
    )
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--cloud-glob", default="cloud_data/events_*.jsonl")
    parser.add_argument("--profile", default="trash_ask", choices=("trash_ask", "trash_bid"))
    parser.add_argument("--min-score", type=float, default=160.0)
    parser.add_argument("--entry-weight", type=float, default=0.12)
    parser.add_argument("--out-json", default="data/reports/daily_paper_trade_summary_trash_ask.json")
    parser.add_argument("--out-md-dir", default="data/reports")
    parser.add_argument("--history-path", default="data/reports/daily_metrics_history.jsonl")
    parser.add_argument("--sync-status", default="unknown")
    parser.add_argument("--collector-status", default="unknown")
    args = parser.parse_args()

    report = run_report(
        log_path=Path(args.log_path),
        cloud_glob=args.cloud_glob,
        profile=args.profile,
        min_score=args.min_score,
        entry_weight=args.entry_weight,
        out_json=None,
    )

    summary = {
        "baseline_profile": args.profile,
        "baseline_mode": "B",
        "score_formula": "entry_augmented = final_score + entry_weight * entry_score",
        "min_score": args.min_score,
        "entry_weight": args.entry_weight,
        "runtime_sec": report["runtime_sec"],
        "source_paths": report.get("source_paths", []),
        "duplicate_events_skipped": report.get("duplicate_events_skipped", 0),
        "unique_relevant_events": report.get("unique_relevant_events", 0),
        "sync_status": args.sync_status,
        "collector_status": args.collector_status,
        "windows": {},
    }

    for label in ("24h", "3d", "7d"):
        mode_a = report["windows"][label]["baseline"]
        mode_b = report["windows"][label]["entry_augmented"]
        summary["windows"][label] = {
            "baseline_paper_trade": mode_a["overall"],
            "paper_trade": mode_b["overall"],
            "top_1_per_cycle": mode_b["top_1_per_cycle"],
            "top_3_per_cycle": mode_b["top_3_per_cycle"],
        }

    summary["windows_health"] = build_health(summary)
    for label in ("24h", "3d", "7d"):
        summary["windows"][label]["health"] = summary["windows_health"][label]
        top1 = summary["windows"][label]["top_1_per_cycle"]
        top3 = summary["windows"][label]["top_3_per_cycle"]
        summary["windows"][label]["health"]["top_1_expectancy_status"] = fmt_health_flag(
            float(top1.get("expectancy_pct", 0.0)) > 0.0,
            "positive",
            "negative",
        )
        summary["windows"][label]["health"]["top_3_expectancy_status"] = fmt_health_flag(
            float(top3.get("expectancy_pct", 0.0)) > 0.0,
            "positive",
            "negative",
        )
        summary["windows"][label]["health"]["collector_status"] = args.collector_status
        summary["windows"][label]["health"]["sync_status"] = args.sync_status

    summary["health_summary"] = {
        "expectancy_positive_all_windows": all(
            summary["windows"][label]["health"]["expectancy_status"] == "positive"
            for label in ("24h", "3d", "7d")
        ),
        "top_1_positive_all_windows": all(
            summary["windows"][label]["health"]["top_1_expectancy_status"] == "positive"
            for label in ("24h", "3d", "7d")
        ),
        "top_3_positive_all_windows": all(
            summary["windows"][label]["health"]["top_3_expectancy_status"] == "positive"
            for label in ("24h", "3d", "7d")
        ),
        "mode_b_better_or_equal_all_windows": all(
            summary["windows"][label]["health"]["mode_b_vs_a"] == "better_or_equal"
            for label in ("24h", "3d", "7d")
        ),
        "collector_alive": args.collector_status in {"alive", "started", "restarted"},
        "sync_success": args.sync_status in {"success", "skipped"},
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    md_dir = Path(args.out_md_dir)
    md_dir.mkdir(parents=True, exist_ok=True)
    md_path = md_dir / f"daily_metrics_{datetime.now().strftime('%Y-%m-%d')}.md"
    md_path.write_text(render_markdown(summary), encoding="utf-8")

    append_history(Path(args.history_path), summary)

    print(
        f"baseline={summary['baseline_profile']} mode={summary['baseline_mode']} "
        f"runtime={summary['runtime_sec']:.2f}s json={out_path} md={md_path}"
    )
    for label in ("24h", "3d", "7d"):
        window = summary["windows"][label]
        paper = window["paper_trade"]
        top1 = window["top_1_per_cycle"]
        top3 = window["top_3_per_cycle"]
        health = window["health"]
        print(f"[{label}]")
        print(
            f"  paper_trade: count={int(paper['count'])} WR={paper['wr_pct']:.2f}% "
            f"exp={paper['expectancy_pct']:.4f}% avg_MFE={paper['avg_mfe_pct']:.4f}% "
            f"avg_MAE={paper['avg_mae_pct']:.4f}%"
        )
        print(
            f"  top_1_per_cycle: cycles={int(top1['cycles'])} count={int(top1['count'])} "
            f"WR={top1['wr_pct']:.2f}% exp={top1['expectancy_pct']:.4f}%"
        )
        print(
            f"  top_3_per_cycle: cycles={int(top3['cycles'])} count={int(top3['count'])} "
            f"WR={top3['wr_pct']:.2f}% exp={top3['expectancy_pct']:.4f}%"
        )
        print(
            f"  health: expectancy={health['expectancy_status']} "
            f"modeB_vs_A={health['mode_b_vs_a']} "
            f"top1={health['top_1_expectancy_status']} "
            f"top3={health['top_3_expectancy_status']} "
            f"avg_MAE_vs_prev={health['avg_mae_vs_prev_window']} "
            f"count_vs_prev={health['count_vs_prev_window']} "
            f"collector={health['collector_status']} sync={health['sync_status']}"
        )


if __name__ == "__main__":
    main()
