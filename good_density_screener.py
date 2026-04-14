from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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


@dataclass
class SeenState:
    obs: WallObservation
    first_seen_ts: float
    last_seen_ts: float
    seen_count: int
    max_wall_notional_usd: float = 0.0
    min_wall_notional_usd: float = math.inf
    sum_wall_notional_usd: float = 0.0
    notional_samples: list[float] = field(default_factory=list)
    last_missing_started_at: float | None = None
    missing_episodes: int = 0
    rebuild_count: int = 0


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_quality_profile(
    path: Path,
    profile: str,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "market": "perp",
            "side": "all",
            "min_ratio": 3.0,
            "max_ratio": float("inf"),
            "min_notional_usd": 10_000.0,
            "max_notional_usd": float("inf"),
            "min_day_volume_usd": 0.0,
            "max_day_volume_usd": float("inf"),
            "max_wall_distance_pct_from_spread": 3.0,
        }

    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "market": "perp",
            "side": "all",
            "min_ratio": 3.0,
            "max_ratio": float("inf"),
            "min_notional_usd": 10_000.0,
            "max_notional_usd": float("inf"),
            "min_day_volume_usd": 0.0,
            "max_day_volume_usd": float("inf"),
            "max_wall_distance_pct_from_spread": 3.0,
        }

    profiles = obj.get("profiles")
    if isinstance(profiles, dict):
        p = profiles.get(profile)
        if not isinstance(p, dict):
            p = _quality_profile_overrides(profile)
        if isinstance(p, dict):
            return {
                "market": str(p.get("market", "perp")),
                "side": str(p.get("side", "all")),
                "min_ratio": safe_float(p.get("min_ratio"), 3.0),
                "max_ratio": safe_float(p.get("max_ratio"), float("inf")),
                "min_notional_usd": safe_float(p.get("min_notional_usd"), 10_000.0),
                "max_notional_usd": safe_float(p.get("max_notional_usd"), float("inf")),
                "min_day_volume_usd": safe_float(p.get("min_day_volume_usd"), 0.0),
                "max_day_volume_usd": safe_float(p.get("max_day_volume_usd"), float("inf")),
                "max_wall_distance_pct_from_spread": safe_float(
                    p.get("max_wall_distance_pct_from_spread"),
                    3.0,
                ),
            }

    return {
        "market": "perp",
        "side": "all",
        "min_ratio": 3.0,
        "max_ratio": float("inf"),
        "min_notional_usd": 10_000.0,
        "max_notional_usd": float("inf"),
        "min_day_volume_usd": 0.0,
        "max_day_volume_usd": float("inf"),
        "max_wall_distance_pct_from_spread": 3.0,
    }




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
    }

def direction_label(side: str) -> str:
    # bid wall -> support -> expected bounce up (LONG)
    # ask wall -> resistance -> expected bounce down (SHORT)
    return "LONG" if side == "bid" else "SHORT"


def _append_limited(series: list[float], value: float, max_len: int = 12) -> None:
    series.append(value)
    if len(series) > max_len:
        del series[:-max_len]


def _safe_std_ratio(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_v = sum(values) / len(values)
    if mean_v <= 0:
        return 0.0
    try:
        return statistics.pstdev(values) / mean_v
    except Exception:
        return 0.0


def score_seen_state(state: SeenState, cfg: ScreenerConfig, now_ts: float, ml_ranker: Any | None = None) -> float:
    return score_seen_state_breakdown(state, cfg, now_ts, ml_ranker=ml_ranker).get("final_score", 0.0)


def score_seen_state_breakdown(state: SeenState, cfg: ScreenerConfig, now_ts: float, ml_ranker: Any | None = None) -> dict[str, Any]:
    base = score_candidate(state.obs, cfg)
    peak = max(state.max_wall_notional_usd, state.obs.wall_notional_usd)
    floor = min(state.min_wall_notional_usd, state.obs.wall_notional_usd)
    stability = (floor / peak) if peak > 0 else 0.0
    current_vs_peak = (state.obs.wall_notional_usd / peak) if peak > 0 else 0.0
    visible_age = max(0.0, now_ts - state.first_seen_ts)
    age_component = min(visible_age / 60.0, 1.5)
    hour_volume = state.obs.day_volume_usd / 24.0 if state.obs.day_volume_usd > 0 else 0.0
    wall_to_hour = (state.obs.wall_notional_usd / hour_volume) * 100.0 if hour_volume > 0 else 0.0
    wall_to_hour_component = min(math.log1p(max(wall_to_hour, 0.0)) / math.log1p(80.0), 1.5)
    volatility = _safe_std_ratio(state.notional_samples[-8:] if state.notional_samples else [state.obs.wall_notional_usd])
    pre_touch_decay = 1.0
    tail = state.notional_samples[-3:]
    if len(tail) >= 3 and tail[-3] > 0:
        pre_touch_decay = tail[-1] / tail[-3]
    dist_pct = max(0.0, state.obs.wall_distance_from_spread_pct)
    near_spread_penalty = min((0.20 - dist_pct) / 0.20, 1.0) if dist_pct < 0.20 else 0.0
    rebuild_penalty = min(max(0, state.rebuild_count) / 3.0, 1.5)
    decay_penalty = max(0.0, 1.0 - pre_touch_decay)

    weighting = _side_weighting_profile(state.obs.side, state.obs.day_volume_usd)
    bucket = weighting["bucket"]
    near_spread_weight = float(weighting["near_spread_weight"])
    volatility_weight = float(weighting["volatility_weight"])
    decay_weight = float(weighting["decay_weight"])
    rebuild_weight = float(weighting["rebuild_weight"])

    behavior_metrics = {
        "visible_age_sec": visible_age,
        "wall_notional_stability_ratio": stability,
        "wall_notional_current_vs_peak_ratio": current_vs_peak,
        "wall_to_hour_volume_pct": wall_to_hour,
        "round_level_score": 0.0,
        "wall_notional_volatility_ratio": volatility,
        "pre_touch_decay_ratio": pre_touch_decay,
        "rebuild_count": float(state.rebuild_count),
        "seen_count": float(max(1, state.seen_count)),
        "approach_speed_pct_per_sec": 0.0,
        "touch_attempt_count": 0.0,
        "updates_before_touch": 0.0,
    }
    boosted = (
        base
        + (16.0 * age_component)
        + (20.0 * stability)
        + (16.0 * current_vs_peak)
        + (14.0 * wall_to_hour_component)
        - (near_spread_weight * near_spread_penalty)
        - (volatility_weight * volatility)
        - (decay_weight * decay_penalty)
        - (rebuild_weight * rebuild_penalty)
    )
    handcrafted_score = max(0.0, boosted)
    ml_proba = predict_live_ml_proba(ml_ranker, state.obs, behavior_metrics)
    final_score = (0.55 * handcrafted_score) + (0.45 * (ml_proba * 100.0))
    return {
        "handcrafted_score": handcrafted_score,
        "ml_proba": ml_proba,
        "final_score": final_score,
        "entry_score": entry_score_from_metrics(behavior_metrics),
        "behavior_tag": behavior_tag_from_metrics(state.obs, behavior_metrics),
        "liquidity_bucket": bucket,
        "side_weighting_profile": weighting["profile"],
    }


def print_signal_table(
    states: dict[str, SeenState],
    cfg: ScreenerConfig,
    min_seen_cycles: int,
    top_n: int,
    ml_ranker: Any | None = None,
) -> None:
    now_ts = time.time()
    ready: list[SeenState] = [s for s in states.values() if s.seen_count >= min_seen_cycles]
    best_by_coin: dict[str, SeenState] = {}
    for state in ready:
        coin_key = state.obs.coin
        existing = best_by_coin.get(coin_key)
        if existing is None:
            best_by_coin[coin_key] = state
            continue
        if (
            score_seen_state_breakdown(state, cfg, now_ts, ml_ranker=ml_ranker)["final_score"]
            > score_seen_state_breakdown(existing, cfg, now_ts, ml_ranker=ml_ranker)["final_score"]
        ):
            best_by_coin[coin_key] = state

    unique_ready = list(best_by_coin.values())
    unique_ready.sort(
        key=lambda s: score_seen_state_breakdown(s, cfg, now_ts, ml_ranker=ml_ranker)["final_score"],
        reverse=True,
    )

    print("\n" + "=" * 88)
    print(f"Good density signals: {len(unique_ready)} | showing top {min(top_n, len(unique_ready))}")
    print("-" * 88)
    print("DIR    COIN                     PRICE         DIST%")
    print("-" * 88)
    for s in unique_ready[:top_n]:
        score_info = score_seen_state_breakdown(s, cfg, now_ts, ml_ranker=ml_ranker)
        symbol = f"{s.obs.display_symbol[:20]:20s} {str(score_info.get('behavior_tag', 'STABLE'))[:8]:8s} {str(score_info.get('side_weighting_profile', 'bid_mids'))[:10]:10s}"
        print(
            f"{direction_label(s.obs.side):6s} "
            f"{symbol:28s} "
            f"{s.obs.wall_price:12.8f} "
            f"{s.obs.wall_distance_from_spread_pct:6.2f}"
        )


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
            timeout=30,
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Good-density screener for bounce trading")
    parser.add_argument("--quality-rules-json", default="data/quality_rules.json")
    parser.add_argument("--quality-profile", choices=["strict", "balanced", "flow", "actionable", "trash_bid", "trash_ask"], default="strict")
    parser.add_argument("--refresh-sec", type=int, default=15)
    parser.add_argument("--meta-refresh-sec", type=int, default=300)
    parser.add_argument("--min-seen-cycles", type=int, default=2)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--run-seconds", type=int, default=0, help="0 = run forever")
    parser.add_argument("--http-timeout-sec", type=int, default=12)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--max-assets", type=int, default=180, help="0 = all assets")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    profile = load_quality_profile(Path(args.quality_rules_json), args.quality_profile)
    market_filter = str(profile.get("market", "perp"))
    if market_filter not in {"all", "perp", "spot"}:
        market_filter = "perp"
    side_filter = str(profile.get("side", "all"))
    if side_filter not in {"all", "bid", "ask"}:
        side_filter = "all"

    cfg = ScreenerConfig(
        market_filter=market_filter,
        side_filter=side_filter,
        min_wall_ratio=safe_float(profile.get("min_ratio"), 3.0),
        max_wall_ratio=safe_float(profile.get("max_ratio"), float("inf")),
        min_wall_notional_usd=safe_float(profile.get("min_notional_usd"), 10_000.0),
        max_wall_notional_usd=safe_float(profile.get("max_notional_usd"), float("inf")),
        min_day_volume_usd=safe_float(profile.get("min_day_volume_usd"), 0.0),
        max_day_volume_usd=safe_float(profile.get("max_day_volume_usd"), float("inf")),
        max_wall_distance_from_spread_pct=safe_float(profile.get("max_wall_distance_pct_from_spread"), 3.0),
        scan_interval_sec=args.refresh_sec,
        concurrency=args.concurrency,
        top_n=args.top_n,
    )

    print(
        "Using quality profile: "
        f"{args.quality_profile} | market={cfg.market_filter} "
        f"| side={cfg.side_filter} "
        f"| ratio=[{cfg.min_wall_ratio}, {cfg.max_wall_ratio}] "
        f"| wall_usd=[{cfg.min_wall_notional_usd}, {cfg.max_wall_notional_usd}] "
        f"| day_volume=[{cfg.min_day_volume_usd}, {cfg.max_day_volume_usd}] "
        f"| max_dist%={cfg.max_wall_distance_from_spread_pct}"
    )

    api = HyperliquidAPI(timeout_sec=args.http_timeout_sec)
    ml_ranker = load_live_ml_ranker(base_dir / "data" / "models" / "bounce_model.joblib")
    print("ML ranker:", "ON" if ml_ranker is not None else "OFF (handcrafted only)")
    collector_paused = False
    init_429_count = 0
    while True:
        try:
            assets, day_volume = read_universe(api)
            assets = select_assets(assets, day_volume, args.max_assets)
            print(f"Loaded assets: {len(assets)} (max_assets={args.max_assets})")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"init universe error: {exc}")
            if "429" in str(exc):
                init_429_count += 1
            if init_429_count >= 3 and not collector_paused:
                rc, msg = run_ps_script(base_dir, "stop_collector.ps1")
                print("auto-pause collector:", "ok" if rc == 0 else "failed")
                if msg:
                    print(msg)
                collector_paused = rc == 0
            time.sleep(5)

    states: dict[str, SeenState] = {}
    started = time.time()
    next_meta_refresh = 0.0

    try:
        while True:
            now = time.time()
            if args.run_seconds > 0 and now - started >= args.run_seconds:
                print("run_seconds reached, exiting.")
                return

            if now >= next_meta_refresh:
                try:
                    assets, day_volume = read_universe(api)
                    assets = select_assets(assets, day_volume, args.max_assets)
                except Exception as exc:  # noqa: BLE001
                    print(f"meta refresh error: {exc}")
                next_meta_refresh = now + args.meta_refresh_sec

            try:
                observations = detect_observations(api, assets, day_volume, cfg, now)
            except Exception as exc:  # noqa: BLE001
                print(f"scan error: {exc}")
                time.sleep(args.refresh_sec)
                continue
            seen_keys: set[str] = set()
            for obs in observations:
                k = candidate_key(obs)
                seen_keys.add(k)
                existing = states.get(k)
                if existing is None:
                    states[k] = SeenState(
                        obs=obs,
                        first_seen_ts=now,
                        last_seen_ts=now,
                        seen_count=1,
                        max_wall_notional_usd=obs.wall_notional_usd,
                        min_wall_notional_usd=obs.wall_notional_usd,
                        sum_wall_notional_usd=obs.wall_notional_usd,
                        notional_samples=[obs.wall_notional_usd],
                    )
                else:
                    if existing.last_missing_started_at is not None:
                        if (now - existing.last_missing_started_at) <= max(18.0, args.refresh_sec * 2.2):
                            existing.rebuild_count += 1
                        existing.last_missing_started_at = None
                    existing.obs = obs
                    existing.last_seen_ts = now
                    existing.seen_count += 1
                    existing.max_wall_notional_usd = max(existing.max_wall_notional_usd, obs.wall_notional_usd)
                    existing.min_wall_notional_usd = min(existing.min_wall_notional_usd, obs.wall_notional_usd)
                    existing.sum_wall_notional_usd += obs.wall_notional_usd
                    _append_limited(existing.notional_samples, obs.wall_notional_usd)

            for k, st in list(states.items()):
                if k in seen_keys:
                    continue
                if st.last_missing_started_at is None:
                    st.last_missing_started_at = now
                    st.missing_episodes += 1
                if now - st.last_seen_ts > args.refresh_sec * 2:
                    del states[k]

            print_signal_table(
                states=states,
                cfg=cfg,
                min_seen_cycles=args.min_seen_cycles,
                top_n=args.top_n,
                ml_ranker=ml_ranker,
            )
            time.sleep(args.refresh_sec)
    finally:
        if collector_paused:
            rc, msg = run_ps_script(base_dir, "start_collector.ps1")
            print("auto-resume collector:", "ok" if rc == 0 else "failed")
            if msg:
                print(msg)


if __name__ == "__main__":
    main()
