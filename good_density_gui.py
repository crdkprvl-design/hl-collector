from __future__ import annotations

import json
import math
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import ttk
from typing import Any
import ctypes

from density_screener import (
    HyperliquidAPI,
    ScreenerConfig,
    WallObservation,
    behavior_tag_from_metrics,
    candidate_key,
    detect_observations,
    entry_score_from_metrics,
    liquidity_bucket,
    load_live_ml_ranker,
    predict_live_ml_proba,
    read_universe,
    score_candidate,
)


RU_TITLE = "HL \u0421\u043a\u0440\u0438\u043d\u0435\u0440 \u041f\u043b\u043e\u0442\u043d\u043e\u0441\u0442\u0435\u0439"
RU_SUBTITLE = "\u041e\u0434\u0438\u043d \u043b\u0443\u0447\u0448\u0438\u0439 \u0441\u0438\u0433\u043d\u0430\u043b \u043d\u0430 \u043c\u043e\u043d\u0435\u0442\u0443"
USE_DYNAMIC_WALL_FLOOR = True
BUILD_TAG = "r12"


@dataclass
class SeenState:
    obs: WallObservation
    first_seen_ts: float
    last_seen_ts: float
    seen_count: int
    behavior: dict[str, float] = field(default_factory=dict)
    has_heartbeat: bool = False
    disarm_pending_since_ts: float | None = None


@dataclass
class GuiConfig:
    quality_profile: str
    refresh_sec: int
    meta_refresh_sec: int
    min_seen_cycles: int
    top_n: int
    concurrency: int
    max_assets: int
    pause_collector: bool
    source_mode: str
    market_override: str
    side_override: str
    signal_confirm_sec: int


@dataclass
class IroncladFilterConfig:
    min_seen_count: int = 2
    min_visible_age_sec: float = 20.0
    min_dominance_ratio: float = 1.8
    min_stability_ratio: float = 0.55
    min_current_vs_peak_ratio: float = 0.55
    max_wall_level_index: int = 35
    max_wall_distance_pct: float = 3.0
    min_wall_ratio: float = 3.0
    min_wall_notional_usd: float = 0.0
    min_wall_to_day_volume_bps: float = 0.3
    min_wall_to_hour_volume_pct: float = 0.08
    min_round_level_score: float = 0.0
    max_wall_notional_volatility_ratio: float = 0.55
    min_pre_touch_decay_ratio: float = 0.62
    max_rebuild_count: int = 3


@dataclass
class SymbolIroncladFilter:
    min_seen_count: int
    min_visible_age_sec: float
    min_dominance_ratio: float
    min_stability_ratio: float
    min_current_vs_peak_ratio: float
    max_wall_level_index: int
    max_wall_distance_pct: float
    min_wall_ratio: float
    min_wall_notional_usd: float
    min_wall_to_day_volume_bps: float
    min_wall_to_hour_volume_pct: float
    min_round_level_score: float
    max_wall_notional_volatility_ratio: float
    min_pre_touch_decay_ratio: float
    max_rebuild_count: int
    resolved: int
    bounced: int
    failed: int
    win_rate_pct: float
    last_resolved_ts: float
    suppress_symbol: bool = False


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default




def _quality_profile_overrides(profile_name: str) -> dict[str, Any] | None:
    synthetic_profiles: dict[str, dict[str, Any]] = {
        "trash_bid": {
            "market": "all",
            "side": "bid",
            "min_ratio": 3.0,
            "max_ratio": float("inf"),
            "min_notional_usd": 0.0,
            "max_notional_usd": float("inf"),
            "min_day_volume_usd": 0.0,
            "max_day_volume_usd": 2_500_000.0,
            "max_wall_distance_pct_from_spread": 3.0,
        },
        "trash_ask": {
            "market": "all",
            "side": "ask",
            "min_ratio": 3.0,
            "max_ratio": float("inf"),
            "min_notional_usd": 0.0,
            "max_notional_usd": float("inf"),
            "min_day_volume_usd": 0.0,
            "max_day_volume_usd": 2_500_000.0,
            "max_wall_distance_pct_from_spread": 3.0,
        },
    }
    return synthetic_profiles.get(profile_name)


def _side_weighting_profile(side: str, day_volume_usd: float) -> dict[str, Any]:
    side_key = side if side in {"bid", "ask"} else "bid"
    bucket = liquidity_bucket(day_volume_usd)
    if side_key == "ask":
        volatility_weight = 29.0
        decay_weight = 24.0
        rebuild_weight = 16.0
    else:
        volatility_weight = 24.0
        decay_weight = 20.0
        rebuild_weight = 12.0
    if bucket == "majors":
        near_spread_weight = 24.0
        volatility_weight *= 0.85
        rebuild_weight *= 0.90
    elif bucket == "trash":
        near_spread_weight = 30.0
        volatility_weight *= 1.20
        decay_weight *= 1.15
        rebuild_weight *= 1.25
    else:
        near_spread_weight = 22.0
    return {
        "side": side_key,
        "bucket": bucket,
        "profile": f"{side_key}_{bucket}",
        "near_spread_weight": near_spread_weight,
        "volatility_weight": volatility_weight,
        "decay_weight": decay_weight,
        "rebuild_weight": rebuild_weight,
        "approach_weight": 9.0,
    }

def load_quality_profile(path: Path, profile: str) -> dict[str, Any]:
    default = {
        "market": "all",
        "side": "all",
        "min_ratio": 3.0,
        "max_ratio": float("inf"),
        "min_notional_usd": 0.0,
        "max_notional_usd": float("inf"),
        "min_day_volume_usd": 100_000.0,
        "max_day_volume_usd": float("inf"),
        "max_wall_distance_pct_from_spread": 3.0,
    }
    if not path.exists():
        return default
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

    profiles = obj.get("profiles")
    if not isinstance(profiles, dict):
        return default
    p = profiles.get(profile)
    if not isinstance(p, dict):
        p = _quality_profile_overrides(profile)
    if not isinstance(p, dict):
        return default

    side = "all"
    max_day_volume_usd = float("inf")
    if profile in {"trash_bid", "trash_ask"}:
        side_candidate = str(p.get("side", "all"))
        if side_candidate in {"bid", "ask"}:
            side = side_candidate
        max_day_volume_usd = min(2_500_000.0, safe_float(p.get("max_day_volume_usd"), 2_500_000.0))

    # Runtime policy:
    # - always scan all markets
    # - keep side-specific trash presets explicit when selected
    # - keep only baseline floors here; per-symbol model hardens them dynamically
    return {
        "market": "all",
        "side": side,
        "min_ratio": max(3.0, safe_float(p.get("min_ratio"), 3.0)),
        "max_ratio": float("inf"),
        "min_notional_usd": 0.0,
        "max_notional_usd": float("inf"),
        "min_day_volume_usd": max(100_000.0, safe_float(p.get("min_day_volume_usd"), 100_000.0)),
        "max_day_volume_usd": max_day_volume_usd,
        "max_wall_distance_pct_from_spread": min(3.0, safe_float(p.get("max_wall_distance_pct_from_spread"), 3.0)),
    }


def load_ironclad_filter(path: Path) -> IroncladFilterConfig:
    default = IroncladFilterConfig()
    if not path.exists():
        return default
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    if not isinstance(obj, dict):
        return default

    candidate = obj.get("selected_filter")
    if isinstance(candidate, dict):
        data = candidate
    else:
        data = obj

    loaded_min_wall_usd = max(0.0, safe_float(data.get("min_wall_notional_usd"), default.min_wall_notional_usd))
    # Explicit runtime policy switch to avoid hidden derive/runtime mismatch.
    effective_min_wall_usd = 0.0 if USE_DYNAMIC_WALL_FLOOR else loaded_min_wall_usd

    return IroncladFilterConfig(
        min_seen_count=max(default.min_seen_count, int(safe_float(data.get("min_seen_count"), default.min_seen_count))),
        min_visible_age_sec=max(
            default.min_visible_age_sec, safe_float(data.get("min_visible_age_sec"), default.min_visible_age_sec)
        ),
        min_dominance_ratio=max(
            default.min_dominance_ratio, safe_float(data.get("min_dominance_ratio"), default.min_dominance_ratio)
        ),
        min_stability_ratio=max(
            default.min_stability_ratio, safe_float(data.get("min_stability_ratio"), default.min_stability_ratio)
        ),
        min_current_vs_peak_ratio=max(
            default.min_current_vs_peak_ratio,
            safe_float(data.get("min_current_vs_peak_ratio"), default.min_current_vs_peak_ratio),
        ),
        max_wall_level_index=min(
            default.max_wall_level_index,
            max(1, int(safe_float(data.get("max_wall_level_index"), default.max_wall_level_index))),
        ),
        max_wall_distance_pct=max(
            0.0,
            min(default.max_wall_distance_pct, safe_float(data.get("max_wall_distance_pct"), default.max_wall_distance_pct)),
        ),
        min_wall_ratio=max(default.min_wall_ratio, safe_float(data.get("min_wall_ratio"), default.min_wall_ratio)),
        min_wall_notional_usd=max(0.0, effective_min_wall_usd),
        min_wall_to_day_volume_bps=max(
            default.min_wall_to_day_volume_bps,
            safe_float(data.get("min_wall_to_day_volume_bps"), default.min_wall_to_day_volume_bps),
        ),
        min_wall_to_hour_volume_pct=max(
            default.min_wall_to_hour_volume_pct,
            safe_float(data.get("min_wall_to_hour_volume_pct"), default.min_wall_to_hour_volume_pct),
        ),
        min_round_level_score=max(
            default.min_round_level_score, safe_float(data.get("min_round_level_score"), default.min_round_level_score)
        ),
        max_wall_notional_volatility_ratio=max(
            0.05,
            min(
                2.0,
                safe_float(
                    data.get("max_wall_notional_volatility_ratio"), default.max_wall_notional_volatility_ratio
                ),
            ),
        ),
        min_pre_touch_decay_ratio=max(
            0.05,
            min(1.5, safe_float(data.get("min_pre_touch_decay_ratio"), default.min_pre_touch_decay_ratio)),
        ),
        max_rebuild_count=max(0, int(safe_float(data.get("max_rebuild_count"), default.max_rebuild_count))),
    )


def load_symbol_ironclad_filters(path: Path) -> dict[str, SymbolIroncladFilter]:
    loaded: dict[str, SymbolIroncladFilter] = {}
    if not path.exists():
        return loaded
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return loaded
    if not isinstance(obj, dict):
        return loaded
    symbols = obj.get("symbols")
    if not isinstance(symbols, dict):
        return loaded

    for key, data in symbols.items():
        if not isinstance(key, str) or not isinstance(data, dict):
            continue
        try:
            loaded[key] = SymbolIroncladFilter(
                min_seen_count=max(1, int(safe_float(data.get("min_seen_count"), 2))),
                min_visible_age_sec=max(0.0, safe_float(data.get("min_visible_age_sec"), 20.0)),
                min_dominance_ratio=max(0.0, safe_float(data.get("min_dominance_ratio"), 1.8)),
                min_stability_ratio=max(0.0, safe_float(data.get("min_stability_ratio"), 0.6)),
                min_current_vs_peak_ratio=max(0.0, safe_float(data.get("min_current_vs_peak_ratio"), 0.55)),
                max_wall_level_index=max(1, int(safe_float(data.get("max_wall_level_index"), 35))),
                max_wall_distance_pct=max(0.0, safe_float(data.get("max_wall_distance_pct"), 3.0)),
                min_wall_ratio=max(0.0, safe_float(data.get("min_wall_ratio"), 3.0)),
                min_wall_notional_usd=max(0.0, safe_float(data.get("min_wall_notional_usd"), 0.0)),
                min_wall_to_day_volume_bps=max(0.0, safe_float(data.get("min_wall_to_day_volume_bps"), 0.3)),
                min_wall_to_hour_volume_pct=max(0.0, safe_float(data.get("min_wall_to_hour_volume_pct"), 0.08)),
                min_round_level_score=max(0.0, safe_float(data.get("min_round_level_score"), 0.0)),
                max_wall_notional_volatility_ratio=max(
                    0.05,
                    min(2.0, safe_float(data.get("max_wall_notional_volatility_ratio"), 0.55)),
                ),
                min_pre_touch_decay_ratio=max(
                    0.05,
                    min(1.5, safe_float(data.get("min_pre_touch_decay_ratio"), 0.62)),
                ),
                max_rebuild_count=max(0, int(safe_float(data.get("max_rebuild_count"), 3.0))),
                resolved=max(0, int(safe_float(data.get("resolved"), 0))),
                bounced=max(0, int(safe_float(data.get("bounced"), 0))),
                failed=max(0, int(safe_float(data.get("failed"), 0))),
                win_rate_pct=max(0.0, safe_float(data.get("win_rate_pct"), 0.0)),
                last_resolved_ts=max(0.0, safe_float(data.get("last_resolved_ts"), 0.0)),
                suppress_symbol=bool(data.get("suppress_symbol", False)),
            )
        except Exception:
            continue
    return loaded


def select_assets(
    assets: list[tuple[str, str, str]],
    day_volume: dict[str, float],
    max_assets: int,
) -> list[tuple[str, str, str]]:
    if max_assets <= 0 or len(assets) <= max_assets:
        return assets

    ranked = sorted(
        assets,
        key=lambda a: (
            day_volume.get(a[1], 0.0),
            1 if a[0] == "perp" else 0,
        ),
        reverse=True,
    )
    return ranked[:max_assets]


def direction_label(side: str) -> str:
    return "LONG" if side == "bid" else "SHORT"


def format_usd(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} \u043c\u043b\u0440\u0434 $"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} \u043c\u043b\u043d $"
    if value >= 1_000:
        return f"{value / 1_000:.2f} \u0442\u044b\u0441 $"
    return f"{value:.0f} $"


def format_pct(value: float) -> str:
    return f"{value:.2f}%"


def run_ps_script(base_dir: Path, script_name: str) -> tuple[int, str]:
    script_path = base_dir / script_name
    if not script_path.exists():
        return 1, f"script not found: {script_path}"
    try:
        proc = subprocess.run(
            ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(script_path)],
            cwd=str(base_dir),
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
        )
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        merged = out if out else ""
        if err:
            merged = f"{merged}\n{err}".strip()
        return proc.returncode, merged
    except Exception as exc:  # noqa: BLE001
        return 1, str(exc)


class ScreenerRuntime:
    def __init__(self, base_dir: Path, cfg: GuiConfig, ui_queue: queue.Queue[dict[str, Any]]) -> None:
        self.base_dir = base_dir
        self.cfg = cfg
        self.ui_queue = ui_queue
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.collector_paused = False
        self.feed_states: dict[str, SeenState] = {}
        self.feed_offset = 0
        self.active_collector_instance_id: str | None = None
        self.active_collector_started_at_ts: float = 0.0
        self.ironclad_filter = load_ironclad_filter(self.base_dir / "data" / "ironclad_filters.json")
        self.symbol_filters_path = self.base_dir / "data" / "symbol_ironclad_filters.json"
        self.symbol_ironclad_filters = load_symbol_ironclad_filters(self.symbol_filters_path)
        self.symbol_filters_mtime: float = 0.0
        if self.symbol_filters_path.exists():
            try:
                self.symbol_filters_mtime = self.symbol_filters_path.stat().st_mtime
            except OSError:
                self.symbol_filters_mtime = 0.0
        self.max_bootstrap_bytes = 8 * 1024 * 1024
        self.max_feed_lines_per_cycle = 25000
        self.last_rows_signature: tuple[tuple[str, str, str, str, str, str], ...] | None = None
        self.empty_signal_quiet_cycles = 0
        # Last seen side signatures per coin (from collector events), used to suppress
        # mirrored/MM-style liquidity even if only one side currently survives strict gates.
        self.recent_side_snapshots: dict[str, dict[str, tuple[float, float, float, float, float]]] = {}
        self.ml_ranker = load_live_ml_ranker(self.base_dir / "data" / "models" / "bounce_model.joblib")

    def _load_active_collector_from_lock(self) -> None:
        lock_path = self.base_dir / "data" / "signal_events_all_pairs.jsonl.lock"
        if not lock_path.exists():
            return
        try:
            data = json.loads(lock_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(data, dict):
            return
        instance_id = str(data.get("collector_instance_id") or "").strip()
        started_ts = safe_float(data.get("collector_started_at_ts"), 0.0)
        if not instance_id:
            return
        self.active_collector_instance_id = instance_id
        self.active_collector_started_at_ts = started_ts
        self._emit(
            {
                "type": "status",
                "text": f"Collector instance pinned: {instance_id[:16]}...",
            }
        )

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)

    def _emit(self, payload: dict[str, Any]) -> None:
        self.ui_queue.put(payload)

    def _maybe_reload_symbol_filters(self) -> None:
        if not self.symbol_filters_path.exists():
            return
        try:
            mtime = self.symbol_filters_path.stat().st_mtime
        except OSError:
            return
        if mtime <= self.symbol_filters_mtime:
            return
        self.symbol_ironclad_filters = load_symbol_ironclad_filters(self.symbol_filters_path)
        self.symbol_filters_mtime = mtime
        self._emit(
            {
                "type": "status",
                "text": f"РћР±РЅРѕРІР»РµРЅС‹ symbol anti-fake С„РёР»СЊС‚СЂС‹: {len(self.symbol_ironclad_filters)} РјРѕРЅРµС‚",
            }
        )

    def _read_collector_pid(self) -> int | None:
        pid_path = self.base_dir / "data" / "collector.pid"
        if not pid_path.exists():
            return None
        try:
            raw = pid_path.read_text(encoding="utf-8").strip()
        except Exception:
            return None
        if not raw:
            return None
        try:
            pid = int(raw)
        except ValueError:
            return None
        return pid if pid > 0 else None

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False

    def _pause_collector_if_running(self) -> bool:
        if not self.cfg.pause_collector or self.collector_paused:
            return False

        pid = self._read_collector_pid()
        if pid is None or not self._is_pid_alive(pid):
            return False

        rc, msg = run_ps_script(self.base_dir, "stop_collector.ps1")
        if rc == 0:
            self.collector_paused = True
            self._emit({"type": "status", "text": "\u0424\u043e\u043d\u043e\u0432\u044b\u0439 \u0441\u0431\u043e\u0440\u0449\u0438\u043a \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d"})
            return True

        details = msg.strip() if msg.strip() else "\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430\u044f \u043e\u0448\u0438\u0431\u043a\u0430"
        self._emit({"type": "status", "text": f"\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043f\u0430\u0443\u0437\u043d\u0443\u0442\u044c \u0441\u0431\u043e\u0440\u0449\u0438\u043a: {details}"})
        return False

    def _ensure_collector_running(self) -> bool:
        pid = self._read_collector_pid()
        if pid is not None and self._is_pid_alive(pid):
            return True

        rc, msg = run_ps_script(self.base_dir, "start_collector.ps1")
        if rc == 0:
            self._emit({"type": "status", "text": "\u0424\u043e\u043d\u043e\u0432\u044b\u0439 \u0441\u0431\u043e\u0440\u0449\u0438\u043a \u0437\u0430\u043f\u0443\u0449\u0435\u043d"})
            return True

        details = msg.strip() if msg.strip() else "\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430\u044f \u043e\u0448\u0438\u0431\u043a\u0430"
        self._emit({"type": "status", "text": f"\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0437\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u044c \u0441\u0431\u043e\u0440\u0449\u0438\u043a: {details}"})
        return False

    def _resolve_config(self) -> ScreenerConfig:
        profile = load_quality_profile(self.base_dir / "data" / "quality_rules.json", self.cfg.quality_profile)
        market_filter = "all"
        side_filter = "all"

        screener_cfg = ScreenerConfig(
            market_filter=market_filter,
            side_filter=side_filter,
            min_wall_ratio=safe_float(profile.get("min_ratio"), 3.0),
            max_wall_ratio=float("inf"),
            min_wall_notional_usd=safe_float(profile.get("min_notional_usd"), 0.0),
            max_wall_notional_usd=float("inf"),
            min_day_volume_usd=safe_float(profile.get("min_day_volume_usd"), 100_000.0),
            max_day_volume_usd=float("inf"),
            max_wall_distance_from_spread_pct=safe_float(profile.get("max_wall_distance_pct_from_spread"), 3.0),
            scan_interval_sec=self.cfg.refresh_sec,
            concurrency=self.cfg.concurrency,
            top_n=self.cfg.top_n,
        )

        self._emit(
            {
                "type": "status",
                "text": (
                    f"\u0420\u0435\u0436\u0438\u043c={self.cfg.source_mode} | "
                    f"build={BUILD_TAG} | "
                    f"\u041f\u0440\u043e\u0444\u0438\u043b\u044c={self.cfg.quality_profile} | "
                    f"\u0420\u044b\u043d\u043e\u043a=all | "
                    f"\u0421\u0442\u043e\u0440\u043e\u043d\u0430=all | "
                    f"coin-models={len(self.symbol_ironclad_filters)} | "
                    f"confirm>={self.cfg.signal_confirm_sec}s | "
                    f"ironclad(dom>={self.ironclad_filter.min_dominance_ratio:.2f}, "
                    f"stab>={self.ironclad_filter.min_stability_ratio:.2f}, "
                    f"age>={self.ironclad_filter.min_visible_age_sec:.0f}s, "
                    f"hour>={self.ironclad_filter.min_wall_to_hour_volume_pct:.2f}%, "
                    f"dv>={self.ironclad_filter.min_wall_to_day_volume_bps:.2f}bps) | "
                    f"dynamic_floor={'on' if USE_DYNAMIC_WALL_FLOOR else 'off'} | "
                    f"ml_ranker={'on' if self.ml_ranker is not None else 'off'} | "
                    f"ratio=[{screener_cfg.min_wall_ratio}, {screener_cfg.max_wall_ratio}] | "
                    f"\u041c\u0430\u043a\u0441.\u0434\u0438\u0441\u0442.={screener_cfg.max_wall_distance_from_spread_pct}%"
                ),
            }
        )
        return screener_cfg

    @staticmethod
    def _obs_matches_cfg(obs: WallObservation, cfg: ScreenerConfig) -> bool:
        if cfg.market_filter != "all" and obs.market != cfg.market_filter:
            return False
        if cfg.side_filter != "all" and obs.side != cfg.side_filter:
            return False
        if obs.wall_ratio < cfg.min_wall_ratio or obs.wall_ratio > cfg.max_wall_ratio:
            return False
        if obs.wall_notional_usd > cfg.max_wall_notional_usd:
            return False
        if obs.day_volume_usd < cfg.min_day_volume_usd or obs.day_volume_usd > cfg.max_day_volume_usd:
            return False
        if obs.wall_distance_from_spread_pct > cfg.max_wall_distance_from_spread_pct:
            return False
        return True

    @staticmethod
    def _event_observation(payload: dict[str, Any]) -> WallObservation | None:
        side = str(payload.get("side", "")).lower()
        if side not in {"bid", "ask"}:
            return None

        coin = str(payload.get("coin", "")).strip()
        if not coin:
            return None
        display = str(payload.get("display_symbol") or coin).strip()
        market = str(payload.get("market", "perp")).lower()
        if market not in {"perp", "spot"}:
            market = "perp"

        wall_price = safe_float(payload.get("wall_price"), 0.0)
        if wall_price <= 0:
            return None

        spread_ref_price = safe_float(payload.get("spread_ref_price"), wall_price)
        dist = safe_float(payload.get("wall_distance_from_spread_pct"), 0.0)
        wall_notional = safe_float(payload.get("wall_notional_usd"), 0.0)
        wall_ratio = safe_float(payload.get("wall_ratio"), 0.0)
        wall_dom_ratio = safe_float(payload.get("wall_dominance_ratio"), 1.0)
        wall_level_index = int(safe_float(payload.get("wall_level_index"), 0.0))
        tick_size = safe_float(payload.get("tick_size"), 0.0)
        day_volume = safe_float(payload.get("day_volume_usd"), 0.0)
        seen_at = safe_float(payload.get("ts"), time.time())

        return WallObservation(
            market=market,
            coin=coin,
            display_symbol=display,
            side=side,
            wall_price=wall_price,
            spread_ref_price=spread_ref_price,
            wall_distance_from_spread_pct=dist,
            wall_notional_usd=wall_notional,
            wall_ratio=wall_ratio,
            wall_dominance_ratio=wall_dom_ratio,
            wall_level_index=wall_level_index,
            tick_size=tick_size,
            day_volume_usd=day_volume,
            seen_at=seen_at,
        )

    def _is_ironclad_payload(self, payload: dict[str, Any], obs: WallObservation) -> bool:
        f = self.ironclad_filter
        symbol_key = f"{obs.market}|{obs.coin}"
        sf = self.symbol_ironclad_filters.get(symbol_key)
        eff_min_seen_count = f.min_seen_count
        eff_min_visible_age_sec = f.min_visible_age_sec
        eff_min_dominance_ratio = f.min_dominance_ratio
        eff_min_stability_ratio = f.min_stability_ratio
        eff_min_current_vs_peak_ratio = f.min_current_vs_peak_ratio
        eff_max_wall_level_index = f.max_wall_level_index
        eff_max_wall_distance_pct = f.max_wall_distance_pct
        eff_min_wall_ratio = f.min_wall_ratio
        eff_min_wall_to_day_volume_bps = f.min_wall_to_day_volume_bps
        eff_min_wall_to_hour_volume_pct = f.min_wall_to_hour_volume_pct
        eff_min_round_level_score = f.min_round_level_score
        eff_max_volatility_ratio = f.max_wall_notional_volatility_ratio
        eff_min_pre_touch_decay_ratio = f.min_pre_touch_decay_ratio
        eff_max_rebuild_count = f.max_rebuild_count

        suppress_symbol_quarantine = False
        if sf is not None:
            symbol_is_poor = sf.resolved >= 60 and sf.win_rate_pct < 18.0 and sf.failed >= max(40, sf.bounced * 4)
            suppress_symbol_quarantine = sf.suppress_symbol
            if suppress_symbol_quarantine or symbol_is_poor:
                eff_min_seen_count = max(eff_min_seen_count, sf.min_seen_count)
                eff_min_visible_age_sec = max(eff_min_visible_age_sec, sf.min_visible_age_sec)
                eff_min_dominance_ratio = max(eff_min_dominance_ratio, sf.min_dominance_ratio)
                eff_min_stability_ratio = max(eff_min_stability_ratio, sf.min_stability_ratio)
                eff_min_current_vs_peak_ratio = max(eff_min_current_vs_peak_ratio, sf.min_current_vs_peak_ratio)
                eff_max_wall_level_index = min(eff_max_wall_level_index, sf.max_wall_level_index)
                eff_max_wall_distance_pct = min(eff_max_wall_distance_pct, sf.max_wall_distance_pct)
                eff_min_wall_ratio = max(eff_min_wall_ratio, sf.min_wall_ratio)
                eff_min_wall_to_day_volume_bps = max(eff_min_wall_to_day_volume_bps, sf.min_wall_to_day_volume_bps)
                eff_min_wall_to_hour_volume_pct = max(eff_min_wall_to_hour_volume_pct, sf.min_wall_to_hour_volume_pct)
                eff_min_round_level_score = max(eff_min_round_level_score, sf.min_round_level_score)
                eff_max_volatility_ratio = min(eff_max_volatility_ratio, sf.max_wall_notional_volatility_ratio)
                eff_min_pre_touch_decay_ratio = max(eff_min_pre_touch_decay_ratio, sf.min_pre_touch_decay_ratio)
                eff_max_rebuild_count = min(eff_max_rebuild_count, sf.max_rebuild_count)
        seen_count = int(safe_float(payload.get("seen_count"), 1.0))
        visible_age_sec = safe_float(payload.get("visible_age_sec"), 0.0)
        dominance_ratio = safe_float(payload.get("wall_dominance_ratio"), obs.wall_dominance_ratio)
        stability_ratio = safe_float(payload.get("wall_notional_stability_ratio"), 0.0)
        current_vs_peak_ratio = safe_float(payload.get("wall_notional_current_vs_peak_ratio"), 1.0)
        volatility_ratio = safe_float(payload.get("wall_notional_volatility_ratio"), 0.0)
        pre_touch_decay_ratio = safe_float(payload.get("pre_touch_decay_ratio"), 1.0)
        rebuild_count = int(safe_float(payload.get("rebuild_count"), 0.0))
        level_index = int(safe_float(payload.get("wall_level_index"), obs.wall_level_index))
        round_level_score = safe_float(payload.get("round_level_score"), 0.0)
        wall_to_day_volume_bps = 0.0
        if obs.day_volume_usd > 0:
            wall_to_day_volume_bps = (obs.wall_notional_usd / obs.day_volume_usd) * 10_000.0
        wall_to_hour_volume_pct = safe_float(payload.get("wall_to_hour_volume_pct"), -1.0)
        hour_volume_usd_est = 0.0
        if wall_to_hour_volume_pct < 0:
            if obs.day_volume_usd > 0:
                hour_volume_usd_est = obs.day_volume_usd / 24.0
                if hour_volume_usd_est > 0:
                    wall_to_hour_volume_pct = (obs.wall_notional_usd / hour_volume_usd_est) * 100.0
                else:
                    wall_to_hour_volume_pct = 0.0
            else:
                wall_to_hour_volume_pct = 0.0
        elif obs.day_volume_usd > 0:
            hour_volume_usd_est = obs.day_volume_usd / 24.0

        # Anti-dust gate: dynamic by symbol liquidity, not a fixed universal wall size.
        # This removes tiny "signals" on dead books while keeping adaptive sizing by coin.
        dynamic_notional_floor = max(12_000.0, min(60_000.0, obs.day_volume_usd * 0.006))
        if obs.wall_notional_usd < dynamic_notional_floor:
            ultra_strong_override = (
                obs.wall_ratio >= max(eff_min_wall_ratio * 2.2, 14.0)
                and dominance_ratio >= max(eff_min_dominance_ratio * 2.0, 5.0)
                and stability_ratio >= max(eff_min_stability_ratio, 0.88)
                and current_vs_peak_ratio >= max(eff_min_current_vs_peak_ratio, 0.88)
                and visible_age_sec >= max(float(self.cfg.refresh_sec) * 2.0, 28.0)
            )
            if not ultra_strong_override:
                return False

        if seen_count < eff_min_seen_count:
            very_strong_fast_pass = (
                seen_count >= 2
                and obs.wall_ratio >= max(eff_min_wall_ratio * 2.0, 10.0)
                and dominance_ratio >= max(eff_min_dominance_ratio * 2.0, 6.0)
                and stability_ratio >= max(eff_min_stability_ratio, 0.90)
                and current_vs_peak_ratio >= max(eff_min_current_vs_peak_ratio, 0.90)
                and wall_to_hour_volume_pct >= max(eff_min_wall_to_hour_volume_pct * 2.0, 8.0)
            )
            if not very_strong_fast_pass:
                return False
        required_age_sec = max(eff_min_visible_age_sec, float(self.cfg.refresh_sec))
        if visible_age_sec < required_age_sec:
            # Strong, persistent walls can be shown a bit earlier than the full age gate.
            early_age_ok = (
                visible_age_sec >= max(float(self.cfg.refresh_sec) * 2.0, required_age_sec * 0.66)
                and dominance_ratio >= (eff_min_dominance_ratio * 1.5)
                and stability_ratio >= max(eff_min_stability_ratio, 0.85)
                and current_vs_peak_ratio >= max(eff_min_current_vs_peak_ratio, 0.85)
            )
            if not early_age_ok:
                return False
        if suppress_symbol_quarantine:
            # Quarantine mode for symbols with chronically poor history:
            # allow only exceptional, persistent walls instead of a hard ban.
            if seen_count < max(eff_min_seen_count, 3):
                return False
            if visible_age_sec < max(required_age_sec, 45.0):
                return False
            if dominance_ratio < max(eff_min_dominance_ratio, 5.0):
                return False
            if stability_ratio < max(eff_min_stability_ratio, 0.90):
                return False
            if current_vs_peak_ratio < max(eff_min_current_vs_peak_ratio, 0.88):
                return False
            if obs.wall_ratio < max(eff_min_wall_ratio, 14.0):
                return False
            # Near-spread micro liquidity is the main failure mode for these symbols.
            if obs.wall_distance_from_spread_pct < 0.12:
                return False
        # Thin/unknown-volume symbols are noisy: enforce stronger relative bars,
        # without hard notional USD cutoffs.
        if obs.day_volume_usd <= 0:
            if dominance_ratio < max(eff_min_dominance_ratio, 6.0):
                return False
            if stability_ratio < max(eff_min_stability_ratio, 0.9):
                return False
            if visible_age_sec < max(required_age_sec, 35.0):
                return False
            if obs.wall_ratio < max(eff_min_wall_ratio, 9.0):
                return False
        elif obs.day_volume_usd < 300_000.0:
            if dominance_ratio < max(eff_min_dominance_ratio, 4.0):
                return False
            if stability_ratio < max(eff_min_stability_ratio, 0.85):
                return False
            if visible_age_sec < max(required_age_sec, 25.0):
                return False
            if wall_to_hour_volume_pct < max(eff_min_wall_to_hour_volume_pct, 35.0):
                return False
        elif obs.day_volume_usd < 1_000_000.0:
            if dominance_ratio < max(eff_min_dominance_ratio, 3.0):
                return False
            if wall_to_hour_volume_pct < max(eff_min_wall_to_hour_volume_pct, 15.0):
                return False
        elif obs.day_volume_usd < 2_000_000.0:
            if wall_to_hour_volume_pct < max(eff_min_wall_to_hour_volume_pct, 12.0):
                return False
        # Near-spread walls are often synthetic liquidity and disappear quickly.
        if obs.wall_distance_from_spread_pct < 0.15:
            if dominance_ratio < max(eff_min_dominance_ratio, 4.0):
                return False
            if stability_ratio < max(eff_min_stability_ratio, 0.9):
                return False
            if obs.wall_ratio < max(eff_min_wall_ratio, 6.0):
                return False
            if visible_age_sec < max(required_age_sec, 30.0):
                return False
        # Top-2 levels near spread are often refresh-grid/MM liquidity.
        if level_index <= 2 and obs.wall_distance_from_spread_pct <= 0.35:
            if dominance_ratio < max(eff_min_dominance_ratio * 1.4, 3.6):
                return False
            if stability_ratio < max(eff_min_stability_ratio, 0.82):
                return False
            if current_vs_peak_ratio < max(eff_min_current_vs_peak_ratio, 0.82):
                return False
            if visible_age_sec < max(required_age_sec, 22.0):
                return False
        # Historical quality guard by symbol: weak WR symbols require stronger confirmation.
        if sf is not None and sf.resolved >= 80 and sf.win_rate_pct <= 2.0 and sf.failed >= max(60, sf.bounced * 10):
            # Chronically failing symbols (almost no real bounces in recent history)
            # should be suppressed in live tape to avoid persistent fake walls.
            return False

        if sf is not None and sf.resolved >= 30 and sf.win_rate_pct < 25.0:
            if dominance_ratio < max(eff_min_dominance_ratio * 1.4, 3.5):
                return False
            if wall_to_hour_volume_pct < max(eff_min_wall_to_hour_volume_pct * 1.4, 0.8):
                return False
            if visible_age_sec < max(required_age_sec, 24.0):
                return False
        if sf is not None and sf.resolved >= 50 and sf.win_rate_pct < 12.0:
            # Medium-poor symbols are allowed only with extra-strong confirmation.
            if dominance_ratio < max(eff_min_dominance_ratio * 1.8, 4.5):
                return False
            if stability_ratio < max(eff_min_stability_ratio, 0.88):
                return False
            if current_vs_peak_ratio < max(eff_min_current_vs_peak_ratio, 0.86):
                return False
            if obs.wall_ratio < max(eff_min_wall_ratio * 1.5, 8.0):
                return False
            if visible_age_sec < max(required_age_sec, 35.0):
                return False
        if dominance_ratio < eff_min_dominance_ratio:
            return False
        if stability_ratio < eff_min_stability_ratio:
            return False
        if current_vs_peak_ratio < eff_min_current_vs_peak_ratio:
            return False
        if level_index > eff_max_wall_level_index:
            return False
        if obs.wall_distance_from_spread_pct > eff_max_wall_distance_pct:
            return False
        if obs.wall_ratio < eff_min_wall_ratio:
            return False
        if wall_to_day_volume_bps < eff_min_wall_to_day_volume_bps:
            return False
        if wall_to_hour_volume_pct < eff_min_wall_to_hour_volume_pct:
            return False
        if round_level_score < eff_min_round_level_score:
            return False
        # Behavior guards: fake walls are usually jumpy, decaying before touch, and frequently rebuilt.
        if seen_count >= 3 and volatility_ratio > eff_max_volatility_ratio:
            return False
        if seen_count >= 3 and pre_touch_decay_ratio < eff_min_pre_touch_decay_ratio:
            return False
        if rebuild_count > eff_max_rebuild_count and dominance_ratio < max(eff_min_dominance_ratio * 1.5, 3.5):
            return False
        return True

    def _extract_behavior_metrics(
        self,
        payload: dict[str, Any],
        obs: WallObservation,
        first_seen_ts: float,
        last_seen_ts: float,
        seen_count: int,
    ) -> dict[str, float]:
        visible_age_sec = max(0.0, safe_float(payload.get("visible_age_sec"), last_seen_ts - first_seen_ts))
        wall_to_hour_volume_pct = safe_float(payload.get("wall_to_hour_volume_pct"), -1.0)
        if wall_to_hour_volume_pct < 0:
            if obs.day_volume_usd > 0:
                hour_volume = obs.day_volume_usd / 24.0
                wall_to_hour_volume_pct = (obs.wall_notional_usd / hour_volume) * 100.0 if hour_volume > 0 else 0.0
            else:
                wall_to_hour_volume_pct = 0.0
        return {
            "visible_age_sec": visible_age_sec,
            "wall_notional_stability_ratio": safe_float(payload.get("wall_notional_stability_ratio"), 0.0),
            "wall_notional_current_vs_peak_ratio": safe_float(payload.get("wall_notional_current_vs_peak_ratio"), 1.0),
            "wall_to_hour_volume_pct": max(0.0, wall_to_hour_volume_pct),
            "round_level_score": max(0.0, safe_float(payload.get("round_level_score"), 0.0)),
            "wall_notional_volatility_ratio": max(0.0, safe_float(payload.get("wall_notional_volatility_ratio"), 0.0)),
            "pre_touch_decay_ratio": max(0.0, safe_float(payload.get("pre_touch_decay_ratio"), 1.0)),
            "rebuild_count": max(0.0, safe_float(payload.get("rebuild_count"), 0.0)),
            "seen_count": float(max(1, seen_count)),
            "approach_speed_pct_per_sec": max(0.0, safe_float(payload.get("approach_speed_pct_per_sec"), 0.0)),
            "touch_attempt_count": max(0.0, safe_float(payload.get("touch_attempt_count"), 0.0)),
            "updates_before_touch": max(0.0, safe_float(payload.get("updates_before_touch"), 0.0)),
        }

    def _state_score_breakdown(self, state: SeenState, cfg: ScreenerConfig, now_ts: float) -> dict[str, float | str]:
        obs = state.obs
        base = score_candidate(obs, cfg)
        m = state.behavior
        visible_age = max(0.0, safe_float(m.get("visible_age_sec"), now_ts - state.first_seen_ts))
        age_component = min(visible_age / 60.0, 1.5)
        stability = max(0.0, safe_float(m.get("wall_notional_stability_ratio"), 0.0))
        current_vs_peak = max(0.0, safe_float(m.get("wall_notional_current_vs_peak_ratio"), 0.0))
        wall_to_hour = max(0.0, safe_float(m.get("wall_to_hour_volume_pct"), 0.0))
        wall_to_hour_component = min(math.log1p(max(wall_to_hour, 0.01)) / math.log1p(80.0), 1.5)
        roundness = max(0.0, safe_float(m.get("round_level_score"), 0.0))
        volatility_ratio = max(0.0, safe_float(m.get("wall_notional_volatility_ratio"), 0.0))
        pre_touch_decay_ratio = max(0.0, safe_float(m.get("pre_touch_decay_ratio"), 1.0))
        rebuild_penalty = min(max(0.0, safe_float(m.get("rebuild_count"), 0.0)) / 3.0, 1.5)
        decay_penalty = max(0.0, 1.0 - pre_touch_decay_ratio)
        approach_speed = max(0.0, safe_float(m.get("approach_speed_pct_per_sec"), 0.0))
        approach_penalty = min(approach_speed / 0.08, 1.0)
        dist_pct = max(0.0, obs.wall_distance_from_spread_pct)
        near_spread_penalty = min((0.20 - dist_pct) / 0.20, 1.0) if dist_pct < 0.20 else 0.0

        weighting = _side_weighting_profile(obs.side, obs.day_volume_usd)
        bucket = weighting["bucket"]
        near_spread_weight = float(weighting["near_spread_weight"])
        volatility_weight = float(weighting["volatility_weight"])
        decay_weight = float(weighting["decay_weight"])
        rebuild_weight = float(weighting["rebuild_weight"])

        boosted = (
            base
            + (18.0 * age_component)
            + (22.0 * stability)
            + (18.0 * current_vs_peak)
            + (16.0 * wall_to_hour_component)
            + (6.0 * roundness)
            - (near_spread_weight * near_spread_penalty)
            - (volatility_weight * volatility_ratio)
            - (decay_weight * decay_penalty)
            - (rebuild_weight * rebuild_penalty)
            - (9.0 * approach_penalty)
        )
        handcrafted_score = max(0.0, boosted)
        ml_proba = predict_live_ml_proba(self.ml_ranker, obs, m)
        final_score = (0.55 * handcrafted_score) + (0.45 * (ml_proba * 100.0))
        return {
            "handcrafted_score": handcrafted_score,
            "ml_proba": ml_proba,
            "final_score": final_score,
            "entry_score": entry_score_from_metrics(m),
            "behavior_tag": behavior_tag_from_metrics(obs, m),
            "liquidity_bucket": bucket,
            "side_weighting_profile": weighting["profile"],
        }

    def _apply_feed_payload(self, payload: dict[str, Any], cfg: ScreenerConfig, now_ts: float) -> None:
        event = str(payload.get("event", "")).lower()
        collector_instance_id = str(payload.get("collector_instance_id") or "").strip()
        collector_started_at_ts = safe_float(payload.get("collector_started_at_ts"), 0.0)
        if collector_instance_id:
            if self.active_collector_instance_id is None:
                self.active_collector_instance_id = collector_instance_id
                self.active_collector_started_at_ts = collector_started_at_ts
            elif collector_instance_id != self.active_collector_instance_id:
                if collector_started_at_ts >= self.active_collector_started_at_ts:
                    self.active_collector_instance_id = collector_instance_id
                    self.active_collector_started_at_ts = collector_started_at_ts
                    self.feed_states.clear()
                    self._emit(
                        {
                            "type": "status",
                            "text": "РћР±РЅР°СЂСѓР¶РµРЅ РЅРѕРІС‹Р№ collector instance: РїРµСЂРµР·Р°РїСѓСЃРєР°СЋ Р»РѕРєР°Р»СЊРЅРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ Р»РµРЅС‚С‹",
                        }
                    )
                else:
                    return
        elif self.active_collector_instance_id is not None:
            # Once a tagged collector is selected, ignore legacy/untagged events.
            return

        obs = self._event_observation(payload)
        if obs is None:
            return
        event_ts = safe_float(payload.get("ts"), now_ts)
        if event_ts <= 0:
            event_ts = now_ts
        max_feed_lag_sec = max(20.0, float(self.cfg.refresh_sec) * 3.0)
        if event in {"armed", "heartbeat", "touched"} and (now_ts - event_ts) > max_feed_lag_sec:
            return
        key = candidate_key(obs)

        if event in {"armed", "heartbeat", "touched"}:
            by_side = self.recent_side_snapshots.setdefault(obs.coin, {})
            by_side[obs.side] = (
                event_ts,
                max(0.0, obs.wall_notional_usd),
                max(0.0, obs.wall_distance_from_spread_pct),
                max(0.0, obs.wall_ratio),
                max(0.0, obs.wall_dominance_ratio),
            )
            # Keep exactly one active wall per market+coin+side in GUI state.
            # This prevents stale old-price keys from competing with the fresh level.
            for other_key, other_state in list(self.feed_states.items()):
                if other_key == key:
                    continue
                if (
                    other_state.obs.market == obs.market
                    and other_state.obs.coin == obs.coin
                    and other_state.obs.side == obs.side
                ):
                    del self.feed_states[other_key]

        if event in {"armed", "heartbeat"}:
            payload_seen_count = int(safe_float(payload.get("seen_count"), 1.0))
            existing = self.feed_states.get(key)
            if existing is None:
                # Entry gate for NEW walls.
                if not self._obs_matches_cfg(obs, cfg):
                    return
                if not self._is_ironclad_payload(payload, obs):
                    return
                self.feed_states[key] = SeenState(
                    obs=obs,
                    first_seen_ts=event_ts,
                    last_seen_ts=event_ts,
                    seen_count=max(1, payload_seen_count),
                    behavior=self._extract_behavior_metrics(
                        payload=payload,
                        obs=obs,
                        first_seen_ts=event_ts,
                        last_seen_ts=event_ts,
                        seen_count=max(1, payload_seen_count),
                    ),
                    has_heartbeat=event == "heartbeat",
                    disarm_pending_since_ts=None,
                )
            else:
                # Keep only walls that STILL pass runtime gates, so GUI stays
                # aligned with the actual DOM and does not keep stale/weak rows.
                if not self._obs_matches_cfg(obs, cfg):
                    self.feed_states.pop(key, None)
                    return
                if not self._is_ironclad_payload(payload, obs):
                    self.feed_states.pop(key, None)
                    return
                existing.obs = obs
                existing.last_seen_ts = event_ts
                existing.seen_count = max(existing.seen_count + 1, payload_seen_count)
                existing.behavior = self._extract_behavior_metrics(
                    payload=payload,
                    obs=obs,
                    first_seen_ts=existing.first_seen_ts,
                    last_seen_ts=event_ts,
                    seen_count=existing.seen_count,
                )
                if event == "heartbeat":
                    existing.has_heartbeat = True
                existing.disarm_pending_since_ts = None
            return

        if event in {"bounced", "expired", "failed_breakdown", "failed_breakout"}:
            self.feed_states.pop(key, None)
            return

        if event == "disarmed_missing":
            existing = self.feed_states.get(key)
            if existing is not None:
                existing.disarm_pending_since_ts = event_ts
            return

        if event == "touched":
            existing = self.feed_states.get(key)
            if existing is not None:
                existing.obs = obs
                existing.last_seen_ts = event_ts
                payload_seen_count = int(safe_float(payload.get("seen_count"), float(existing.seen_count)))
                existing.seen_count = max(existing.seen_count, payload_seen_count)
                existing.behavior = self._extract_behavior_metrics(
                    payload=payload,
                    obs=obs,
                    first_seen_ts=existing.first_seen_ts,
                    last_seen_ts=event_ts,
                    seen_count=existing.seen_count,
                )
                existing.has_heartbeat = True
                existing.disarm_pending_since_ts = None

    def _bootstrap_feed(self, events_file: Path, cfg: ScreenerConfig, now_ts: float) -> None:
        self.feed_states.clear()
        self.feed_offset = 0
        self.active_collector_instance_id = None
        self.active_collector_started_at_ts = 0.0
        if not events_file.exists():
            return
        try:
            file_size = events_file.stat().st_size
            bootstrap_from = 0
            fresh_cutoff_ts = now_ts - max(180.0, float(self.cfg.refresh_sec) * 20.0)
            if file_size > self.max_bootstrap_bytes:
                bootstrap_from = file_size - self.max_bootstrap_bytes
                self._emit(
                    {
                        "type": "status",
                        "text": "\u0411\u044b\u0441\u0442\u0440\u044b\u0439 \u0441\u0442\u0430\u0440\u0442: \u0447\u0438\u0442\u0430\u044e \u0445\u0432\u043e\u0441\u0442 feed (\u0431\u0435\u0437 \u043f\u043e\u043b\u043d\u043e\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438)",
                    }
                )
            with events_file.open("r", encoding="utf-8", errors="ignore") as fh:
                if bootstrap_from > 0:
                    fh.seek(bootstrap_from)
                    _ = fh.readline()
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(payload, dict):
                        payload_ts = safe_float(payload.get("ts"), 0.0)
                        if payload_ts > 0 and payload_ts < fresh_cutoff_ts:
                            continue
                        self._apply_feed_payload(payload, cfg, now_ts)
                self.feed_offset = fh.tell()
        except Exception as exc:  # noqa: BLE001
            self._emit({"type": "status", "text": f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0447\u0442\u0435\u043d\u0438\u044f feed: {exc}"})

    def _read_feed_delta(self, events_file: Path, cfg: ScreenerConfig, now_ts: float) -> tuple[int, bool]:
        if not events_file.exists():
            self.feed_offset = 0
            return 0, False

        try:
            file_size = events_file.stat().st_size
        except OSError:
            return 0, False
        if file_size < self.feed_offset:
            self.feed_offset = 0
            self.feed_states.clear()

        applied = 0
        has_backlog = False
        try:
            with events_file.open("r", encoding="utf-8", errors="ignore") as fh:
                fh.seek(self.feed_offset)
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    self._apply_feed_payload(payload, cfg, now_ts)
                    applied += 1
                    if applied >= self.max_feed_lines_per_cycle:
                        has_backlog = True
                        break
                self.feed_offset = fh.tell()
        except Exception as exc:  # noqa: BLE001
            self._emit({"type": "status", "text": f"\u041e\u0448\u0438\u0431\u043a\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f feed: {exc}"})
            return applied, has_backlog
        if not has_backlog and self.feed_offset < file_size:
            has_backlog = True
        return applied, has_backlog

    def _prune_feed_states(self, now_ts: float) -> None:
        ttl_sec = max(28, self.cfg.refresh_sec * 4)
        # Collector updates per-symbol in bursts, so too-short grace removes live walls.
        disarm_grace_sec = max(16.0, float(self.cfg.refresh_sec) * 2.0)
        for key, state in list(self.feed_states.items()):
            if state.disarm_pending_since_ts is not None and (now_ts - state.disarm_pending_since_ts) > disarm_grace_sec:
                del self.feed_states[key]
                continue
            if now_ts - state.last_seen_ts > ttl_sec:
                del self.feed_states[key]

    def _emit_signals(self, rows: list[tuple[str, str, str, str, str, str]]) -> None:
        signature = tuple(rows)
        if signature == self.last_rows_signature:
            return
        self.last_rows_signature = signature
        self._emit(
            {
                "type": "signals",
                "rows": rows,
                "count": len(rows),
                "at": time.strftime("%H:%M:%S"),
            }
        )

    def _run_from_collector(self, cfg: ScreenerConfig) -> None:
        if self.cfg.pause_collector:
            self._emit(
                {
                    "type": "status",
                    "text": "\u0420\u0435\u0436\u0438\u043c collector: \u043f\u0430\u0443\u0437\u0430 \u0441\u0431\u043e\u0440\u0449\u0438\u043a\u0430 \u0438\u0433\u043d\u043e\u0440\u0438\u0440\u0443\u0435\u0442\u0441\u044f",
                }
            )

        self._ensure_collector_running()
        self._load_active_collector_from_lock()
        events_file = self.base_dir / "data" / "signal_events_all_pairs.jsonl"

        wait_attempts = 0
        while not self.stop_event.is_set() and not events_file.exists():
            wait_attempts += 1
            if wait_attempts % 4 == 1:
                self._ensure_collector_running()
            self._emit({"type": "status", "text": "\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435 \u043f\u043e\u0442\u043e\u043a\u0430 \u0441\u043e\u0431\u044b\u0442\u0438\u0439 \u043e\u0442 \u0441\u0431\u043e\u0440\u0449\u0438\u043a\u0430..."})
            self.stop_event.wait(2)

        now_ts = time.time()
        self._bootstrap_feed(events_file, cfg, now_ts)

        while not self.stop_event.is_set():
            now_ts = time.time()
            self._maybe_reload_symbol_filters()
            applied, has_backlog = self._read_feed_delta(events_file, cfg, now_ts)
            self._prune_feed_states(now_ts)
            rows = self._build_rows(self.feed_states, cfg)
            assets_count = len({state.obs.coin for state in self.feed_states.values()})
            self._emit({"type": "meta", "assets": assets_count})
            if rows:
                self.empty_signal_quiet_cycles = 0
                self._emit_signals(rows)
            else:
                quiet_no_data_cycle = (applied == 0 and not has_backlog)
                if quiet_no_data_cycle and self.last_rows_signature:
                    # Avoid blink-to-empty during short feed gaps between heartbeat bursts.
                    self.empty_signal_quiet_cycles += 1
                    if self.empty_signal_quiet_cycles >= 16:
                        self._emit_signals(rows)
                else:
                    self.empty_signal_quiet_cycles = 0
                    self._emit_signals(rows)
            if has_backlog:
                self._emit(
                    {
                        "type": "status",
                        "text": f"\u0414\u043e\u0433\u043e\u043d\u044f\u044e feed (\u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u043d\u043e +{applied}, \u0435\u0441\u0442\u044c \u0445\u0432\u043e\u0441\u0442)",
                    }
                )
            elif applied == 0:
                self._emit({"type": "status", "text": "\u041e\u043d\u043b\u0430\u0439\u043d \u043c\u043e\u043d\u0438\u0442\u043e\u0440 (\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a: collector feed)"})
            else:
                self._emit({"type": "status", "text": f"\u041e\u043d\u043b\u0430\u0439\u043d \u043c\u043e\u043d\u0438\u0442\u043e\u0440 (\u0441\u043e\u0431\u044b\u0442\u0438\u0439 +{applied})"})
            wait_sec = 0.2 if has_backlog else min(2.0, max(0.8, float(self.cfg.refresh_sec) * 0.25))
            self.stop_event.wait(wait_sec)

    def _run_from_api(self, cfg: ScreenerConfig) -> None:
        api = HyperliquidAPI(timeout_sec=12, min_request_interval_sec=0.05, max_retries=2)
        states: dict[str, SeenState] = {}
        next_meta_refresh = 0.0
        init_429_count = 0
        consecutive_scan_429 = 0
        assets: list[tuple[str, str, str]] = []
        day_volume: dict[str, float] = {}
        scan_cursor = 0

        if self.cfg.pause_collector:
            self._pause_collector_if_running()

        api_assets_limit = min(max(self.cfg.max_assets, 1), 120) if self.cfg.max_assets != 0 else 120

        while not self.stop_event.is_set():
            self._maybe_reload_symbol_filters()
            try:
                assets, day_volume = read_universe(api)
                assets = select_assets(assets, day_volume, api_assets_limit)
                self._emit({"type": "meta", "assets": len(assets)})
                break
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                if "429" in msg:
                    init_429_count += 1
                    if self.cfg.pause_collector:
                        self._pause_collector_if_running()
                    retry_sec = min(70, 5 + (init_429_count * 5))
                    self._emit({"type": "status", "text": f"429 Too Many Requests. \u041f\u043e\u0432\u0442\u043e\u0440 \u0447\u0435\u0440\u0435\u0437 {retry_sec} \u0441\u0435\u043a"})
                    if init_429_count >= 4:
                        self._emit(
                            {
                                "type": "status",
                                "text": "API \u043f\u0435\u0440\u0435\u0433\u0440\u0443\u0436\u0435\u043d (429). \u0410\u0432\u0442\u043e-\u043f\u0435\u0440\u0435\u0445\u043e\u0434 \u043d\u0430 collector feed",
                            }
                        )
                        self._run_from_collector(cfg)
                        return
                    self.stop_event.wait(retry_sec)
                    continue
                self._emit({"type": "status", "text": f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0438\u043d\u0438\u0446\u0438\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u0438 API: {msg}"})
                self.stop_event.wait(4)

        def next_batch_assets(full_assets: list[tuple[str, str, str]], batch_size: int) -> list[tuple[str, str, str]]:
            nonlocal scan_cursor
            if not full_assets:
                return []
            batch = max(1, min(batch_size, len(full_assets)))
            if scan_cursor >= len(full_assets):
                scan_cursor = 0
            end = scan_cursor + batch
            if end <= len(full_assets):
                picked = full_assets[scan_cursor:end]
            else:
                picked = full_assets[scan_cursor:] + full_assets[: end - len(full_assets)]
            scan_cursor = end % len(full_assets)
            return picked

        while not self.stop_event.is_set():
            now = time.time()
            self._maybe_reload_symbol_filters()

            if now >= next_meta_refresh:
                try:
                    assets, day_volume = read_universe(api)
                    assets = select_assets(assets, day_volume, api_assets_limit)
                    self._emit({"type": "meta", "assets": len(assets)})
                except Exception as exc:  # noqa: BLE001
                    self._emit({"type": "status", "text": f"\u041e\u0448\u0438\u0431\u043a\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f API: {exc}"})
                next_meta_refresh = now + self.cfg.meta_refresh_sec

            base_batch_size = min(36, max(12, len(assets) // 4 if assets else 0))
            if self.cfg.refresh_sec <= 5:
                base_batch_size = min(base_batch_size, 20)
            if consecutive_scan_429 > 0:
                base_batch_size = max(8, base_batch_size - (consecutive_scan_429 * 4))
            scan_assets = next_batch_assets(assets, base_batch_size)

            try:
                observations = detect_observations(api, scan_assets, day_volume, cfg, now)
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                if "429" in msg:
                    consecutive_scan_429 += 1
                    wait_sec = min(90, 6 + (consecutive_scan_429 * 8))
                    self._emit(
                        {
                            "type": "status",
                            "text": (
                                f"429 \u043d\u0430 API-\u0441\u043a\u0430\u043d\u0435 (x{consecutive_scan_429}), "
                                f"\u0431\u0430\u0442\u0447={len(scan_assets)}. \u041f\u0430\u0443\u0437\u0430 {wait_sec}\u0441"
                            ),
                        }
                    )
                    if consecutive_scan_429 >= 5:
                        self._emit(
                            {
                                "type": "status",
                                "text": "429 \u0441\u0435\u0440\u0438\u044f \u0432 API-\u0440\u0435\u0436\u0438\u043c\u0435. \u0410\u0432\u0442\u043e-\u043f\u0435\u0440\u0435\u0445\u043e\u0434 \u043d\u0430 collector",
                            }
                        )
                        self._run_from_collector(cfg)
                        return
                    self.stop_event.wait(wait_sec)
                else:
                    self._emit({"type": "status", "text": f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0441\u043a\u0430\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u044f API: {msg}"})
                    self.stop_event.wait(self.cfg.refresh_sec)
                continue

            scan_rate_limits = api.consume_rate_limit_hits()
            if scan_rate_limits > 0:
                consecutive_scan_429 += 1
                wait_sec = min(75, 5 + (consecutive_scan_429 * 6))
                self._emit(
                    {
                        "type": "status",
                        "text": (
                            f"API rate-limit: {scan_rate_limits} hit(s), \u0431\u0430\u0442\u0447={len(scan_assets)}, "
                            f"\u043f\u0430\u0443\u0437\u0430 {wait_sec}\u0441"
                        ),
                    }
                )
                if consecutive_scan_429 >= 5:
                    self._emit(
                        {
                            "type": "status",
                            "text": "\u041b\u0438\u043c\u0438\u0442 API \u0441\u043b\u0438\u0448\u043a\u043e\u043c \u0447\u0430\u0441\u0442\u044b\u0439. \u0410\u0432\u0442\u043e-\u043f\u0435\u0440\u0435\u0445\u043e\u0434 \u043d\u0430 collector feed",
                        }
                    )
                    self._run_from_collector(cfg)
                    return
                self.stop_event.wait(wait_sec)
            else:
                consecutive_scan_429 = 0

            seen_keys: set[str] = set()
            scanned_coins = {coin for _, coin, _ in scan_assets}
            for obs in observations:
                key = candidate_key(obs)
                seen_keys.add(key)
                existing = states.get(key)
                if existing is None:
                    states[key] = SeenState(
                        obs=obs,
                        first_seen_ts=now,
                        last_seen_ts=now,
                        seen_count=1,
                        behavior={
                            "visible_age_sec": 0.0,
                            "wall_notional_stability_ratio": 0.0,
                            "wall_notional_current_vs_peak_ratio": 1.0,
                            "wall_to_hour_volume_pct": (obs.wall_notional_usd / (obs.day_volume_usd / 24.0) * 100.0)
                            if obs.day_volume_usd > 0
                            else 0.0,
                            "round_level_score": 0.0,
                            "wall_notional_volatility_ratio": 0.0,
                            "pre_touch_decay_ratio": 1.0,
                            "rebuild_count": 0.0,
                            "seen_count": 1.0,
                        },
                    )
                else:
                    existing.obs = obs
                    existing.last_seen_ts = now
                    existing.seen_count += 1
                    existing.behavior["visible_age_sec"] = max(0.0, now - existing.first_seen_ts)
                    existing.behavior["seen_count"] = float(existing.seen_count)
                    if obs.day_volume_usd > 0:
                        hour_volume = obs.day_volume_usd / 24.0
                        existing.behavior["wall_to_hour_volume_pct"] = (
                            (obs.wall_notional_usd / hour_volume) * 100.0 if hour_volume > 0 else 0.0
                        )

            stale_ttl = max(self.cfg.refresh_sec * 4, 30)
            for key, state in list(states.items()):
                if key in seen_keys:
                    continue
                if state.obs.coin in scanned_coins and now - state.last_seen_ts > stale_ttl:
                    del states[key]

            rows = self._build_rows(states, cfg)
            self._emit_signals(rows)
            self._emit(
                {
                    "type": "status",
                    "text": (
                        f"\u041f\u0440\u044f\u043c\u043e\u0439 API-\u0441\u043a\u0430\u043d: \u0431\u0430\u0442\u0447 {len(scan_assets)}/{len(assets)} "
                        f"| \u0441\u0438\u0433\u043d\u0430\u043b\u044b {len(rows)}"
                    ),
                }
            )
            self.stop_event.wait(self.cfg.refresh_sec)

    def _build_rows(self, states: dict[str, SeenState], cfg: ScreenerConfig) -> list[tuple[str, str, str, str, str, str]]:
        now_ts = time.time()
        min_seen = max(self.cfg.min_seen_cycles, self.ironclad_filter.min_seen_count)
        # Collector heartbeat cadence for a single symbol can be slower than GUI refresh.
        # Keep rows visible across normal feed gaps, but still drop stale ghosts quickly.
        freshness_sec = max(18.0, float(self.cfg.refresh_sec) * 2.2)
        disarm_grace_sec = max(18.0, float(self.cfg.refresh_sec) * 2.2)
        ready = [
            s
            for s in states.values()
            if s.seen_count >= min_seen
            and (now_ts - s.first_seen_ts) >= self.cfg.signal_confirm_sec
            and (now_ts - s.last_seen_ts) <= freshness_sec
            and s.has_heartbeat
            and (
                s.disarm_pending_since_ts is None or (now_ts - s.disarm_pending_since_ts) <= disarm_grace_sec
            )
        ]

        # Drop likely market-maker/grid mirrors: similar strong walls on both sides
        # of the same coin near spread are rarely directional "bounce" signals.
        by_coin: dict[str, list[SeenState]] = {}
        for state in ready:
            by_coin.setdefault(state.obs.coin, []).append(state)

        reject_mm_coins: set[str] = set()
        for coin, items in by_coin.items():
            bids = [s for s in items if s.obs.side == "bid"]
            asks = [s for s in items if s.obs.side == "ask"]
            if not bids or not asks:
                continue
            best_bid = max(bids, key=lambda s: float(self._state_score_breakdown(s, cfg, now_ts)["final_score"]))
            best_ask = max(asks, key=lambda s: float(self._state_score_breakdown(s, cfg, now_ts)["final_score"]))

            bid_usd = max(0.0, best_bid.obs.wall_notional_usd)
            ask_usd = max(0.0, best_ask.obs.wall_notional_usd)
            max_usd = max(bid_usd, ask_usd)
            min_usd = min(bid_usd, ask_usd)
            if max_usd <= 0:
                continue

            size_similarity = min_usd / max_usd
            dist_gap = abs(best_bid.obs.wall_distance_from_spread_pct - best_ask.obs.wall_distance_from_spread_pct)
            near_spread = max(best_bid.obs.wall_distance_from_spread_pct, best_ask.obs.wall_distance_from_spread_pct) <= 0.9
            mature_pair = min(best_bid.seen_count, best_ask.seen_count) >= max(min_seen + 1, 4)
            strong_pair = (
                min(best_bid.obs.wall_ratio, best_ask.obs.wall_ratio) >= 6.0
                and min(best_bid.obs.wall_dominance_ratio, best_ask.obs.wall_dominance_ratio) >= 1.8
            )
            mirrored = size_similarity >= 0.60 and dist_gap <= 0.25

            if near_spread and mature_pair and strong_pair and mirrored:
                reject_mm_coins.add(coin)

        if reject_mm_coins:
            ready = [s for s in ready if s.obs.coin not in reject_mm_coins]

        recent_mm_window_sec = max(300.0, float(self.cfg.refresh_sec) * 30.0)

        def has_recent_mirror_mm(state: SeenState) -> bool:
            by_side = self.recent_side_snapshots.get(state.obs.coin)
            if not by_side:
                return False
            opp_side = "ask" if state.obs.side == "bid" else "bid"
            snap = by_side.get(opp_side)
            if snap is None:
                return False
            snap_ts, snap_usd, snap_dist, snap_ratio, snap_dom = snap
            if (now_ts - snap_ts) > recent_mm_window_sec:
                return False
            cur_usd = max(0.0, state.obs.wall_notional_usd)
            max_usd = max(cur_usd, snap_usd)
            min_usd = min(cur_usd, snap_usd)
            if max_usd <= 0:
                return False
            size_similarity = min_usd / max_usd
            dist_gap = abs(max(0.0, state.obs.wall_distance_from_spread_pct) - snap_dist)
            near_spread = max(max(0.0, state.obs.wall_distance_from_spread_pct), snap_dist) <= 1.0
            strong_pair = (
                min(state.obs.wall_ratio, snap_ratio) >= 6.0
                and min(state.obs.wall_dominance_ratio, snap_dom) >= 1.8
            )
            # Allow partial thinning on one side: MM mirrors often decay unevenly.
            return near_spread and strong_pair and size_similarity >= 0.25 and dist_gap <= 0.35

        ready = [s for s in ready if not has_recent_mirror_mm(s)]

        best_by_coin: dict[str, tuple[SeenState, float]] = {}
        for state in ready:
            coin_key = state.obs.coin
            score = float(self._state_score_breakdown(state, cfg, now_ts)["final_score"])
            existing = best_by_coin.get(coin_key)
            if existing is None or score > existing[1]:
                best_by_coin[coin_key] = (state, score)

        scored_ready = sorted(best_by_coin.values(), key=lambda item: item[1], reverse=True)
        if len(scored_ready) > 10:
            keep_count = max(3, int(math.ceil(len(scored_ready) * 0.30)))
            scored_ready = scored_ready[:keep_count]
        rows: list[tuple[str, str, str, str, str, str]] = []
        for state, _score in scored_ready:
            score_info = self._state_score_breakdown(state, cfg, now_ts)
            symbol = f"{self._format_terminal_symbol(state.obs)} / {score_info['behavior_tag']} / {score_info.get('side_weighting_profile', 'bid_mids')}"
            rows.append(
                (
                    direction_label(state.obs.side),
                    symbol,
                    f"{state.obs.wall_price:.8f}",
                    f"{state.obs.wall_ratio:.2f}",
                    format_pct(state.obs.wall_distance_from_spread_pct),
                    format_usd(state.obs.wall_notional_usd),
                )
            )
        return rows

    @staticmethod
    def _format_terminal_symbol(obs: WallObservation) -> str:
        market_tag = obs.market.upper()
        if obs.market == "perp":
            base = (obs.coin or obs.display_symbol or "").strip()
            if base.endswith("-PERP"):
                base = base[: -len("-PERP")]
            if not base:
                base = "UNKNOWN"
            return f"{base}-USDC [{market_tag}]"

        display = (obs.display_symbol or obs.coin or "").strip()
        # Typical spot label looks like "TAO/USDC (@123)" -> "TAO-USDC [SPOT]"
        m = re.match(r"^\s*([^/\s]+)/([^)\s]+)", display)
        if m:
            base = m.group(1).strip()
            quote = m.group(2).strip()
            return f"{base}-{quote} [{market_tag}]"

        fallback = display.replace("/", "-") if display else "UNKNOWN"
        return f"{fallback} [{market_tag}]"

    def _run(self) -> None:
        screener_cfg = self._resolve_config()
        try:
            if self.cfg.source_mode == "collector":
                self._run_from_collector(screener_cfg)
            else:
                self._run_from_api(screener_cfg)
        finally:
            if self.collector_paused:
                rc, msg = run_ps_script(self.base_dir, "start_collector.ps1")
                if rc != 0:
                    details = msg.strip() if msg.strip() else "\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u0430\u044f \u043e\u0448\u0438\u0431\u043a\u0430"
                    self._emit({"type": "status", "text": f"\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0432\u043e\u0437\u043e\u0431\u043d\u043e\u0432\u0438\u0442\u044c \u0441\u0431\u043e\u0440\u0449\u0438\u043a: {details}"})


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.base_dir = Path(__file__).resolve().parent
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.runtime: ScreenerRuntime | None = None

        self.root.title(RU_TITLE)
        self.root.geometry("1220x780")
        self.root.minsize(980, 640)
        self.root.configure(bg="#070B11")

        self._icon_ref: tk.PhotoImage | None = None
        icon_path = self.base_dir / "assets" / "screener_icon.png"
        if icon_path.exists():
            try:
                self._icon_ref = tk.PhotoImage(file=str(icon_path))
                self.root.iconphoto(True, self._icon_ref)
            except Exception:
                pass

        self._build_style()
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(200, self._poll_queue)

    def _build_style(self) -> None:
        style = ttk.Style(self.root)
        style.theme_use("clam")

        style.configure(".", background="#010507", foreground="#B7FFD9")
        style.configure("Root.TFrame", background="#010507")
        style.configure("Shell.TFrame", background="#010507")
        style.configure("Sidebar.TFrame", background="#041019")
        style.configure("SidebarCard.TFrame", background="#071723")
        style.configure("Main.TFrame", background="#010507")
        style.configure("Card.TFrame", background="#071723")
        style.configure("TableCard.TFrame", background="#071723")
        style.configure("TableInner.TFrame", background="#010B11")

        style.configure(
            "HeroTitle.TLabel",
            background="#071723",
            foreground="#8EFFAF",
            font=("Consolas Bold", 20),
        )
        style.configure(
            "HeroSub.TLabel",
            background="#071723",
            foreground="#5EA6C0",
            font=("Consolas", 10),
        )
        style.configure(
            "SideSection.TLabel",
            background="#071723",
            foreground="#61F9AA",
            font=("Consolas Bold", 10),
        )
        style.configure(
            "SideLabel.TLabel",
            background="#071723",
            foreground="#BDEBD6",
            font=("Consolas", 10),
        )
        style.configure(
            "SideHint.TLabel",
            background="#071723",
            foreground="#5D8EA8",
            font=("Consolas", 9),
        )
        style.configure(
            "MainTitle.TLabel",
            background="#010507",
            foreground="#90FFC9",
            font=("Consolas Bold", 21),
        )
        style.configure(
            "MainSub.TLabel",
            background="#010507",
            foreground="#5E96B5",
            font=("Consolas", 10),
        )
        style.configure(
            "CardTitle.TLabel",
            background="#071723",
            foreground="#66C4F2",
            font=("Consolas Bold", 10),
        )
        style.configure(
            "CardValue.TLabel",
            background="#071723",
            foreground="#E0FFF2",
            font=("Consolas Bold", 16),
        )
        style.configure(
            "StatusValue.TLabel",
            background="#071723",
            foreground="#F6FFF9",
            font=("Consolas", 11),
        )
        style.configure(
            "Log.TLabel",
            background="#071723",
            foreground="#78D2FF",
            font=("Consolas Bold", 9),
        )

        style.configure(
            "Sidebar.TCheckbutton",
            background="#071723",
            foreground="#C2EFD9",
            font=("Consolas", 10),
        )
        style.map(
            "Sidebar.TCheckbutton",
            background=[("active", "#071723")],
            foreground=[("active", "#ECFFF6")],
        )

        style.configure(
            "Control.TEntry",
            fieldbackground="#01070D",
            foreground="#C9FFE3",
            insertcolor="#C9FFE3",
            bordercolor="#1D4A62",
            lightcolor="#1D4A62",
            darkcolor="#1D4A62",
            padding=(8, 6),
            font=("Consolas", 10),
        )
        style.configure(
            "Control.TCombobox",
            fieldbackground="#01070D",
            foreground="#C9FFE3",
            background="#01070D",
            selectforeground="#C9FFE3",
            selectbackground="#01070D",
            bordercolor="#1D4A62",
            lightcolor="#1D4A62",
            darkcolor="#1D4A62",
            arrowsize=14,
            padding=(8, 6),
            font=("Consolas", 10),
        )
        style.map(
            "Control.TCombobox",
            fieldbackground=[("readonly", "#01070D")],
            foreground=[("readonly", "#C9FFE3")],
            selectforeground=[("readonly", "#C9FFE3")],
            selectbackground=[("readonly", "#01070D")],
            background=[("readonly", "#01070D")],
        )

        style.configure(
            "Primary.TButton",
            background="#69FFB2",
            foreground="#002815",
            borderwidth=0,
            padding=(14, 10),
            font=("Consolas Bold", 11),
        )
        style.map(
            "Primary.TButton",
            background=[("disabled", "#1C4A35"), ("pressed", "#4EEA9A"), ("active", "#83FFC1")],
            foreground=[("disabled", "#79B798")],
        )
        style.configure(
            "Secondary.TButton",
            background="#113149",
            foreground="#D7F2FF",
            borderwidth=0,
            padding=(14, 10),
            font=("Consolas Bold", 11),
        )
        style.map(
            "Secondary.TButton",
            background=[("disabled", "#0C1E2D"), ("pressed", "#184664"), ("active", "#205679")],
            foreground=[("disabled", "#7DA5BE")],
        )

        style.configure(
            "Signals.Treeview",
            background="#01070D",
            fieldbackground="#01070D",
            foreground="#C8FFE2",
            rowheight=30,
            borderwidth=0,
            font=("Consolas", 11),
        )
        style.configure(
            "Signals.Treeview.Heading",
            background="#0E2638",
            foreground="#65F1AD",
            relief="flat",
            font=("Consolas Bold", 10),
        )
        style.map("Signals.Treeview", background=[("selected", "#184F66")], foreground=[("selected", "#F1FFFA")])

        style.configure(
            "Signals.Vertical.TScrollbar",
            background="#103049",
            troughcolor="#01070E",
            arrowcolor="#A3D7F2",
            bordercolor="#01070E",
            darkcolor="#103049",
            lightcolor="#103049",
        )

    def _build_ui(self) -> None:
        root_frame = ttk.Frame(self.root, style="Root.TFrame", padding=12)
        root_frame.pack(fill=tk.BOTH, expand=True)

        self.profile_var = tk.StringVar(value="strict")
        self.refresh_var = tk.StringVar(value="8")
        self.assets_var = tk.StringVar(value="80")
        self.pause_var = tk.BooleanVar(value=False)
        self.source_var = tk.StringVar(value="collector")

        self.status_text = tk.StringVar(value="Ожидание запуска")
        self.assets_text = tk.StringVar(value="-")
        self.updated_text = tk.StringVar(value="-")
        self.count_text = tk.StringVar(value="0")

        shell = ttk.Frame(root_frame, style="Shell.TFrame")
        shell.pack(fill=tk.BOTH, expand=True)
        shell.columnconfigure(0, weight=0)
        shell.columnconfigure(1, weight=1)
        shell.rowconfigure(0, weight=1)

        sidebar = ttk.Frame(shell, style="Sidebar.TFrame", padding=(14, 12))
        sidebar.grid(row=0, column=0, sticky="nsw")
        sidebar.configure(width=330)
        sidebar.grid_propagate(False)

        main = ttk.Frame(shell, style="Main.TFrame", padding=(14, 6, 4, 6))
        main.grid(row=0, column=1, sticky="nsew")
        main.columnconfigure(0, weight=1)
        main.rowconfigure(3, weight=1)
        main.rowconfigure(4, weight=0)

        brand = ttk.Frame(sidebar, style="SidebarCard.TFrame", padding=(12, 12))
        brand.pack(fill=tk.X)
        ttk.Label(brand, text=RU_TITLE, style="HeroTitle.TLabel", wraplength=280, justify=tk.LEFT).pack(anchor=tk.W)
        ttk.Label(brand, text="[collector stream + live terminal view]", style="HeroSub.TLabel").pack(anchor=tk.W, pady=(4, 0))

        controls = ttk.Frame(sidebar, style="SidebarCard.TFrame", padding=(12, 12))
        controls.pack(fill=tk.X, pady=(10, 0))
        controls.columnconfigure(0, weight=1)
        controls.columnconfigure(1, weight=1)

        ttk.Label(controls, text="SCAN CONFIG", style="SideSection.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 10)
        )

        ttk.Label(controls, text="Источник", style="SideLabel.TLabel").grid(row=1, column=0, sticky="w")
        ttk.Label(controls, text="Профиль", style="SideLabel.TLabel").grid(row=1, column=1, sticky="w", padx=(8, 0))
        ttk.Combobox(
            controls,
            textvariable=self.source_var,
            values=["collector", "api"],
            state="readonly",
            style="Control.TCombobox",
        ).grid(row=2, column=0, pady=(4, 10), sticky="ew")
        ttk.Combobox(
            controls,
            textvariable=self.profile_var,
            values=["strict", "balanced", "flow", "actionable", "trash_bid", "trash_ask"],
            state="readonly",
            style="Control.TCombobox",
        ).grid(row=2, column=1, pady=(4, 10), sticky="ew", padx=(8, 0))

        ttk.Label(controls, text="Обновление (сек)", style="SideLabel.TLabel").grid(
            row=3, column=0, columnspan=2, sticky="w"
        )
        ttk.Entry(controls, textvariable=self.refresh_var, style="Control.TEntry", width=8).grid(
            row=4, column=0, columnspan=2, pady=(4, 10), sticky="ew"
        )

        ttk.Label(controls, text="Макс. активов", style="SideLabel.TLabel").grid(
            row=5, column=0, columnspan=2, sticky="w"
        )
        ttk.Entry(controls, textvariable=self.assets_var, style="Control.TEntry", width=8).grid(
            row=6, column=0, columnspan=2, pady=(4, 10), sticky="ew"
        )

        ttk.Checkbutton(
            controls,
            text="Пауза сборщика (только в API-режиме)",
            variable=self.pause_var,
            style="Sidebar.TCheckbutton",
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(0, 10))

        self.start_btn = ttk.Button(
            controls,
            text="СТАРТ",
            style="Primary.TButton",
            command=self._start,
        )
        self.start_btn.grid(row=8, column=0, columnspan=2, sticky="ew")
        self.stop_btn = ttk.Button(
            controls,
            text="СТОП",
            style="Secondary.TButton",
            command=self._stop,
            state=tk.DISABLED,
        )
        self.stop_btn.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        ttk.Label(
            controls,
            text="Для стабильной работы и минимума 429 используй collector",
            style="SideHint.TLabel",
            wraplength=275,
            justify=tk.LEFT,
        ).grid(row=10, column=0, columnspan=2, sticky="w", pady=(12, 0))

        ttk.Label(main, text="HL DENSITY TERMINAL // EXEC", style="MainTitle.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            main,
            text="stream: hyperliquid orderflow | one real wall per coin | anti-fake ironclad",
            style="MainSub.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 14))

        cards = ttk.Frame(main, style="Main.TFrame")
        cards.grid(row=2, column=0, sticky="ew", pady=(0, 14))
        for i in range(4):
            cards.columnconfigure(i, weight=1)

        status_value = self._build_stat_card(cards, "Статус", self.status_text, 0)
        status_value.configure(style="StatusValue.TLabel", wraplength=290, justify=tk.LEFT)
        self._build_stat_card(cards, "Активов в feed", self.assets_text, 1)
        self._build_stat_card(cards, "Сигналы", self.count_text, 2)
        self._build_stat_card(cards, "Обновлено", self.updated_text, 3)

        table_card = ttk.Frame(main, style="TableCard.TFrame", padding=(12, 12))
        table_card.grid(row=3, column=0, sticky="nsew")
        table_card.columnconfigure(0, weight=1)
        table_card.rowconfigure(1, weight=1)

        ttk.Label(table_card, text="SIGNAL TAPE // IRONCLAD", style="CardTitle.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 10)
        )

        table_inner = ttk.Frame(table_card, style="TableInner.TFrame", padding=(8, 8))
        table_inner.grid(row=1, column=0, sticky="nsew")
        table_inner.columnconfigure(0, weight=1)
        table_inner.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            table_inner,
            columns=("dir", "coin", "price", "ratio", "dist", "wall_usd"),
            show="headings",
            height=18,
            style="Signals.Treeview",
        )
        self.tree.heading("dir", text="SIDE")
        self.tree.heading("coin", text="INSTRUMENT")
        self.tree.heading("price", text="WALL PX")
        self.tree.heading("ratio", text="RATIO")
        self.tree.heading("dist", text="DIST %")
        self.tree.heading("wall_usd", text="WALL USD")
        self.tree.column("dir", width=86, anchor=tk.W)
        self.tree.column("coin", width=212, anchor=tk.W)
        self.tree.column("price", width=138, anchor=tk.E)
        self.tree.column("ratio", width=92, anchor=tk.E)
        self.tree.column("dist", width=98, anchor=tk.E)
        self.tree.column("wall_usd", width=144, anchor=tk.E)
        self.tree.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(table_inner, orient=tk.VERTICAL, command=self.tree.yview, style="Signals.Vertical.TScrollbar")
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=y_scroll.set)

        self.tree.tag_configure("long", foreground="#8BFFC3")
        self.tree.tag_configure("short", foreground="#FF9D77")

        log_card = ttk.Frame(main, style="Card.TFrame", padding=(12, 10))
        log_card.grid(row=4, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(log_card, text="RUNTIME LOG", style="Log.TLabel").pack(anchor=tk.W)
        self.runtime_log = tk.Text(
            log_card,
            height=4,
            bg="#01070D",
            fg="#8FE7FF",
            insertbackground="#8FE7FF",
            relief="flat",
            borderwidth=0,
            font=("Consolas", 9),
            wrap="none",
            state=tk.DISABLED,
        )
        self.runtime_log.pack(fill=tk.X, expand=False, pady=(6, 0))

    def _build_stat_card(self, parent: ttk.Frame, title: str, variable: tk.StringVar, column: int) -> ttk.Label:
        card = ttk.Frame(parent, style="Card.TFrame", padding=(14, 12))
        card.grid(row=0, column=column, sticky="nsew", padx=(0, 8 if column < 3 else 0))
        ttk.Label(card, text=title, style="CardTitle.TLabel").pack(anchor=tk.W)
        value = ttk.Label(card, textvariable=variable, style="CardValue.TLabel")
        value.pack(anchor=tk.W, pady=(8, 0))
        return value

    def _start(self) -> None:
        if self.runtime is not None:
            return
        try:
            source_mode = self.source_var.get().strip().lower() or "collector"
            if source_mode not in {"collector", "api"}:
                source_mode = "collector"

            cfg = GuiConfig(
                quality_profile=self.profile_var.get().strip() or "strict",
                refresh_sec=max(3, int(self.refresh_var.get())),
                meta_refresh_sec=420,
                min_seen_cycles=2,
                top_n=0,
                concurrency=4 if source_mode == "api" else 6,
                max_assets=max(0, int(self.assets_var.get())),
                pause_collector=self.pause_var.get(),
                source_mode=source_mode,
                market_override="all",
                side_override="all",
                signal_confirm_sec=10,
            )
            if cfg.source_mode == "api" and cfg.refresh_sec < 6:
                cfg.refresh_sec = 6
        except ValueError:
            self.status_text.set("Неверные параметры")
            return

        self.runtime = ScreenerRuntime(self.base_dir, cfg, self.queue)
        self.runtime.start()
        self.status_text.set(f"Инициализация ({cfg.source_mode})...")
        self.start_btn.configure(state=tk.DISABLED)
        self.stop_btn.configure(state=tk.NORMAL)

    def _stop(self) -> None:
        if self.runtime is None:
            return
        self.runtime.stop()
        self.runtime = None
        self.status_text.set("Остановлено")
        self.start_btn.configure(state=tk.NORMAL)
        self.stop_btn.configure(state=tk.DISABLED)

    def _replace_rows(self, rows: list[tuple[str, str, str, str, str, str]]) -> None:
        signature = tuple(rows)
        if signature == getattr(self, "_last_rows_signature", None):
            return
        self._last_rows_signature = signature
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        for row in rows:
            direction = str(row[0]).upper()
            tag = ""
            if "LONG" in direction:
                tag = "long"
            elif "SHORT" in direction:
                tag = "short"
            self.tree.insert("", tk.END, values=row, tags=(tag,) if tag else ())

    def _append_runtime_log(self, text: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] {text}\n"
        self.runtime_log.configure(state=tk.NORMAL)
        self.runtime_log.insert(tk.END, line)
        lines_count = int(self.runtime_log.index("end-1c").split(".")[0])
        if lines_count > 120:
            self.runtime_log.delete("1.0", f"{lines_count - 120}.0")
        self.runtime_log.see(tk.END)
        self.runtime_log.configure(state=tk.DISABLED)

    def _poll_queue(self) -> None:
        try:
            while True:
                item = self.queue.get_nowait()
                kind = item.get("type")
                if kind == "status":
                    text = str(item.get("text", ""))
                    self.status_text.set(text)
                    if text:
                        self._append_runtime_log(text)
                elif kind == "meta":
                    self.assets_text.set(str(item.get("assets", "-")))
                elif kind == "signals":
                    rows = item.get("rows", [])
                    self.count_text.set(str(item.get("count", 0)))
                    self.updated_text.set(str(item.get("at", "-")))
                    self._replace_rows(rows)
        except queue.Empty:
            pass
        self.root.after(220, self._poll_queue)

    def _on_close(self) -> None:
        self._stop()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
