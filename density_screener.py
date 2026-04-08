from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


INFO_ENDPOINT = "https://api.hyperliquid.xyz/info"


@dataclass(frozen=True)
class ScreenerConfig:
    market_filter: str = "all"
    min_wall_ratio: float = 10.0
    min_wall_notional_usd: float = 50_000.0
    min_day_volume_usd: float = 1_000_000.0
    approach_ticks: int = 4
    breakout_ticks: int = 2
    bounce_pct: float = 0.5
    min_persistence_sec: int = 20
    max_signal_age_sec: int = 300
    scan_interval_sec: int = 20
    concurrency: int = 40
    top_n: int = 25


@dataclass
class WallObservation:
    market: str
    coin: str
    display_symbol: str
    side: str
    wall_price: float
    wall_notional_usd: float
    wall_ratio: float
    tick_size: float
    day_volume_usd: float
    seen_at: float


@dataclass
class CandidateState:
    obs: WallObservation
    first_seen: float
    last_seen: float
    touched_at: float | None = None
    touch_price: float | None = None
    status: str = "watching"
    last_mid: float | None = None


class HyperliquidAPI:
    def __init__(self, timeout_sec: int = 12) -> None:
        self.session = requests.Session()
        self.timeout_sec = timeout_sec

    def _post(self, payload: dict[str, Any]) -> Any:
        response = self.session.post(INFO_ENDPOINT, json=payload, timeout=self.timeout_sec)
        response.raise_for_status()
        return response.json()

    def perp_meta_and_ctx(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        body = self._post({"type": "metaAndAssetCtxs"})
        if not isinstance(body, list) or len(body) != 2:
            raise RuntimeError("Unexpected response shape for metaAndAssetCtxs")
        meta, ctx = body
        if not isinstance(meta, dict) or not isinstance(ctx, list):
            raise RuntimeError("Invalid payload types for metaAndAssetCtxs")
        return meta, [x for x in ctx if isinstance(x, dict)]

    def spot_meta_and_ctx(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        body = self._post({"type": "spotMetaAndAssetCtxs"})
        if not isinstance(body, list) or len(body) != 2:
            raise RuntimeError("Unexpected response shape for spotMetaAndAssetCtxs")
        meta, ctx = body
        if not isinstance(meta, dict) or not isinstance(ctx, list):
            raise RuntimeError("Invalid payload types for spotMetaAndAssetCtxs")
        return meta, [x for x in ctx if isinstance(x, dict)]

    def all_mids(self) -> dict[str, float]:
        body = self._post({"type": "allMids"})
        if not isinstance(body, dict):
            return {}
        mids: dict[str, float] = {}
        for coin, value in body.items():
            try:
                mids[str(coin)] = float(value)
            except (TypeError, ValueError):
                continue
        return mids

    def l2_book(self, coin: str) -> dict[str, Any] | None:
        body = self._post({"type": "l2Book", "coin": coin})
        return body if isinstance(body, dict) else None


def safe_print(*args: Any, **kwargs: Any) -> None:
    try:
        print(*args, **kwargs)
    except OSError:
        # Output pipe can be closed by host/timeout during short smoke tests.
        return


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_usd(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def detect_tick_size(prices: list[float]) -> float:
    if len(prices) < 2:
        return 0.0
    diffs = sorted({abs(prices[i] - prices[i - 1]) for i in range(1, len(prices)) if prices[i] != prices[i - 1]})
    return diffs[0] if diffs else 0.0


def detect_side_wall(
    side_levels: list[dict[str, Any]],
    side: str,
    market: str,
    coin: str,
    display_symbol: str,
    day_volume_usd: float,
    seen_at: float,
) -> WallObservation | None:
    rows: list[tuple[float, float, float]] = []
    for lvl in side_levels:
        px = to_float(lvl.get("px"))
        sz = to_float(lvl.get("sz"))
        if px <= 0 or sz <= 0:
            continue
        rows.append((px, sz, px * sz))

    if len(rows) < 3:
        return None

    notionals = [x[2] for x in rows]
    med_notional = statistics.median(notionals)
    if med_notional <= 0:
        return None

    idx = max(range(len(rows)), key=lambda i: rows[i][2])
    wall_px, _, wall_notional = rows[idx]
    wall_ratio = wall_notional / med_notional
    tick_size = detect_tick_size([x[0] for x in rows])

    return WallObservation(
        market=market,
        coin=coin,
        display_symbol=display_symbol,
        side=side,
        wall_price=wall_px,
        wall_notional_usd=wall_notional,
        wall_ratio=wall_ratio,
        tick_size=tick_size,
        day_volume_usd=day_volume_usd,
        seen_at=seen_at,
    )


def read_universe(api: HyperliquidAPI) -> tuple[list[tuple[str, str, str]], dict[str, float]]:
    perp_meta, perp_ctxs = api.perp_meta_and_ctx()
    spot_meta, spot_ctxs = api.spot_meta_and_ctx()

    assets: list[tuple[str, str, str]] = []
    day_volume: dict[str, float] = {}

    perp_uni = perp_meta.get("universe", [])
    if isinstance(perp_uni, list):
        for i, item in enumerate(perp_uni):
            if not isinstance(item, dict):
                continue
            coin = item.get("name")
            if not isinstance(coin, str) or not coin:
                continue
            assets.append(("perp", coin, coin))
            if i < len(perp_ctxs) and isinstance(perp_ctxs[i], dict):
                day_volume[coin] = to_float(perp_ctxs[i].get("dayNtlVlm"))

    spot_tokens = spot_meta.get("tokens", [])
    spot_uni = spot_meta.get("universe", [])
    if isinstance(spot_uni, list):
        for item in spot_uni:
            if not isinstance(item, dict):
                continue
            coin = item.get("name")
            if not isinstance(coin, str) or not coin:
                continue
            display = coin
            if coin.startswith("@") and isinstance(spot_tokens, list):
                try:
                    base_idx, quote_idx = item["tokens"]
                    base_name = spot_tokens[base_idx]["name"]
                    quote_name = spot_tokens[quote_idx]["name"]
                    display = f"{base_name}/{quote_name} ({coin})"
                except Exception:
                    display = coin
            assets.append(("spot", coin, display))

    for ctx in spot_ctxs:
        coin = ctx.get("coin")
        if isinstance(coin, str):
            day_volume[coin] = to_float(ctx.get("dayNtlVlm"))

    return assets, day_volume


def score_candidate(obs: WallObservation, cfg: ScreenerConfig) -> float:
    ratio_den = max(cfg.min_wall_ratio * 2.0, 1e-9)
    notional_den = max(cfg.min_wall_notional_usd * 2.5, 1e-9)
    volume_den = max(cfg.min_day_volume_usd * 4.0, 1.0)
    ratio_component = min(obs.wall_ratio / ratio_den, 1.5)
    notional_component = min(obs.wall_notional_usd / notional_den, 1.5)
    volume_component = min(obs.day_volume_usd / volume_den, 1.5)
    raw = (0.45 * ratio_component) + (0.35 * notional_component) + (0.20 * volume_component)
    return raw * 100.0


def detect_observations(
    api: HyperliquidAPI,
    assets: list[tuple[str, str, str]],
    day_volume: dict[str, float],
    cfg: ScreenerConfig,
    now_ts: float,
) -> list[WallObservation]:
    observations: list[WallObservation] = []

    def task(asset: tuple[str, str, str]) -> list[WallObservation]:
        market, coin, display = asset
        if cfg.market_filter != "all" and market != cfg.market_filter:
            return []
        if day_volume.get(coin, 0.0) < cfg.min_day_volume_usd:
            return []

        book = api.l2_book(coin)
        if not book:
            return []
        levels = book.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            return []

        result: list[WallObservation] = []
        for side_name, side_levels in (("bid", levels[0]), ("ask", levels[1])):
            if not isinstance(side_levels, list):
                continue
            obs = detect_side_wall(
                side_levels=side_levels,
                side=side_name,
                market=market,
                coin=coin,
                display_symbol=display,
                day_volume_usd=day_volume.get(coin, 0.0),
                seen_at=now_ts,
            )
            if obs is None:
                continue
            if obs.wall_ratio < cfg.min_wall_ratio:
                continue
            if obs.wall_notional_usd < cfg.min_wall_notional_usd:
                continue
            result.append(obs)
        return result

    with ThreadPoolExecutor(max_workers=cfg.concurrency) as executor:
        futures = [executor.submit(task, asset) for asset in assets]
        for fut in as_completed(futures):
            observations.extend(fut.result())

    return observations


def candidate_key(obs: WallObservation) -> str:
    if obs.tick_size > 0:
        bucket = int(round(obs.wall_price / obs.tick_size))
        return f"{obs.market}|{obs.coin}|{obs.side}|tick={obs.tick_size:.12f}|bucket={bucket}"
    return f"{obs.market}|{obs.coin}|{obs.side}|px={obs.wall_price:.10f}"


def log_event(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def print_dashboard(states: dict[str, CandidateState], cfg: ScreenerConfig) -> None:
    active = [s for s in states.values() if s.status in {"armed", "touched"}]
    active.sort(key=lambda s: score_candidate(s.obs, cfg), reverse=True)
    safe_print("\n" + "=" * 120)
    safe_print(f"Active candidates: {len(active)} | top: {min(cfg.top_n, len(active))}")
    safe_print("-" * 120)
    safe_print("score  market symbol                     side wall_px        wall_usd    ratio  vol24      status")
    safe_print("-" * 120)
    for st in active[: cfg.top_n]:
        score = score_candidate(st.obs, cfg)
        safe_print(
            f"{score:5.1f}  "
            f"{st.obs.market:5s} "
            f"{st.obs.display_symbol[:24]:24s} "
            f"{st.obs.side:4s} "
            f"{st.obs.wall_price:12.8f} "
            f"{format_usd(st.obs.wall_notional_usd):10s} "
            f"{st.obs.wall_ratio:6.2f} "
            f"{format_usd(st.obs.day_volume_usd):10s} "
            f"{st.status}"
        )


def process_mid_updates(
    mids: dict[str, float],
    states: dict[str, CandidateState],
    cfg: ScreenerConfig,
    now_ts: float,
    log_path: Path,
) -> None:
    for key, st in list(states.items()):
        obs = st.obs
        mid = mids.get(obs.coin)
        if mid is None:
            continue
        st.last_mid = mid

        if st.status == "armed":
            if obs.side == "bid":
                near_px = obs.wall_price + (cfg.approach_ticks * obs.tick_size)
                if mid <= near_px:
                    st.status = "touched"
                    st.touched_at = now_ts
                    st.touch_price = mid
                    log_event(
                        log_path,
                        {
                            "ts": now_ts,
                            "event": "touched",
                            "market": obs.market,
                            "coin": obs.coin,
                            "display_symbol": obs.display_symbol,
                            "side": obs.side,
                            "wall_price": obs.wall_price,
                            "touch_price": mid,
                            "wall_ratio": obs.wall_ratio,
                            "wall_notional_usd": obs.wall_notional_usd,
                            "day_volume_usd": obs.day_volume_usd,
                        },
                    )
            else:
                near_px = obs.wall_price - (cfg.approach_ticks * obs.tick_size)
                if mid >= near_px:
                    st.status = "touched"
                    st.touched_at = now_ts
                    st.touch_price = mid
                    log_event(
                        log_path,
                        {
                            "ts": now_ts,
                            "event": "touched",
                            "market": obs.market,
                            "coin": obs.coin,
                            "display_symbol": obs.display_symbol,
                            "side": obs.side,
                            "wall_price": obs.wall_price,
                            "touch_price": mid,
                            "wall_ratio": obs.wall_ratio,
                            "wall_notional_usd": obs.wall_notional_usd,
                            "day_volume_usd": obs.day_volume_usd,
                        },
                    )
            continue

        if st.status != "touched":
            continue
        if st.touched_at is None:
            continue

        bounce_up = obs.wall_price * (1.0 + cfg.bounce_pct / 100.0)
        bounce_down = obs.wall_price * (1.0 - cfg.bounce_pct / 100.0)

        if obs.side == "bid":
            fail_line = obs.wall_price - (cfg.breakout_ticks * obs.tick_size)
            if mid <= fail_line:
                st.status = "failed"
                log_event(
                    log_path,
                    {
                        "ts": now_ts,
                        "event": "failed_breakdown",
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "touch_price": st.touch_price,
                        "last_mid": mid,
                        "wall_ratio": obs.wall_ratio,
                        "wall_notional_usd": obs.wall_notional_usd,
                    },
                )
                del states[key]
                continue
            if mid >= bounce_up:
                st.status = "bounced"
                log_event(
                    log_path,
                    {
                        "ts": now_ts,
                        "event": "bounced",
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "touch_price": st.touch_price,
                        "exit_mid": mid,
                        "bounce_pct_target": cfg.bounce_pct,
                        "wall_ratio": obs.wall_ratio,
                        "wall_notional_usd": obs.wall_notional_usd,
                    },
                )
                del states[key]
                continue
        else:
            fail_line = obs.wall_price + (cfg.breakout_ticks * obs.tick_size)
            if mid >= fail_line:
                st.status = "failed"
                log_event(
                    log_path,
                    {
                        "ts": now_ts,
                        "event": "failed_breakout",
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "touch_price": st.touch_price,
                        "last_mid": mid,
                        "wall_ratio": obs.wall_ratio,
                        "wall_notional_usd": obs.wall_notional_usd,
                    },
                )
                del states[key]
                continue
            if mid <= bounce_down:
                st.status = "bounced"
                log_event(
                    log_path,
                    {
                        "ts": now_ts,
                        "event": "bounced",
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "touch_price": st.touch_price,
                        "exit_mid": mid,
                        "bounce_pct_target": cfg.bounce_pct,
                        "wall_ratio": obs.wall_ratio,
                        "wall_notional_usd": obs.wall_notional_usd,
                    },
                )
                del states[key]
                continue

        if now_ts - st.touched_at > cfg.max_signal_age_sec:
            st.status = "expired"
            log_event(
                log_path,
                {
                    "ts": now_ts,
                    "event": "expired",
                    "market": obs.market,
                    "coin": obs.coin,
                    "display_symbol": obs.display_symbol,
                    "side": obs.side,
                    "wall_price": obs.wall_price,
                    "touch_price": st.touch_price,
                    "last_mid": mid,
                    "wall_ratio": obs.wall_ratio,
                    "wall_notional_usd": obs.wall_notional_usd,
                },
            )
            del states[key]


def update_candidates(
    states: dict[str, CandidateState],
    observations: list[WallObservation],
    now_ts: float,
    cfg: ScreenerConfig,
    log_path: Path,
) -> None:
    seen_keys: set[str] = set()

    for obs in observations:
        key = candidate_key(obs)
        seen_keys.add(key)
        existing = states.get(key)
        if existing is None:
            states[key] = CandidateState(obs=obs, first_seen=now_ts, last_seen=now_ts, status="watching")
            continue

        existing.obs = obs
        existing.last_seen = now_ts
        if existing.status == "watching" and now_ts - existing.first_seen >= cfg.min_persistence_sec:
            existing.status = "armed"
            log_event(
                log_path,
                {
                    "ts": now_ts,
                    "event": "armed",
                    "market": obs.market,
                    "coin": obs.coin,
                    "display_symbol": obs.display_symbol,
                    "side": obs.side,
                    "wall_price": obs.wall_price,
                    "wall_ratio": obs.wall_ratio,
                    "wall_notional_usd": obs.wall_notional_usd,
                    "day_volume_usd": obs.day_volume_usd,
                },
            )

    for key, state in list(states.items()):
        if state.status in {"touched"}:
            continue
        if key in seen_keys:
            continue
        if now_ts - state.last_seen > cfg.scan_interval_sec * 2:
            del states[key]


def run(args: argparse.Namespace) -> None:
    apply_quality_profile(args)
    cfg = ScreenerConfig(
        market_filter=args.market_filter,
        min_wall_ratio=args.min_wall_ratio,
        min_wall_notional_usd=args.min_wall_usd,
        min_day_volume_usd=args.min_day_volume_usd,
        approach_ticks=args.approach_ticks,
        breakout_ticks=args.breakout_ticks,
        bounce_pct=args.bounce_pct,
        min_persistence_sec=args.min_persistence_sec,
        max_signal_age_sec=args.max_signal_age_sec,
        scan_interval_sec=args.scan_interval_sec,
        concurrency=args.concurrency,
        top_n=args.top_n,
    )
    api = HyperliquidAPI(timeout_sec=args.http_timeout_sec)
    log_path = Path(args.log_path)

    assets, day_volume = read_universe(api)
    safe_print(f"Loaded assets: {len(assets)} | day volume keys: {len(day_volume)}")

    states: dict[str, CandidateState] = {}
    next_metadata_refresh = 0.0
    next_scan = 0.0
    started_at = time.time()

    while True:
        now_ts = time.time()
        if args.run_seconds > 0 and now_ts - started_at >= args.run_seconds:
            safe_print(f"[{time.strftime('%H:%M:%S')}] run_seconds reached, exiting.")
            break
        if now_ts >= next_metadata_refresh:
            try:
                assets, day_volume = read_universe(api)
                safe_print(f"[{time.strftime('%H:%M:%S')}] universe refresh: {len(assets)} assets")
            except Exception as exc:  # noqa: BLE001
                safe_print(f"[{time.strftime('%H:%M:%S')}] universe refresh error: {exc}")
            next_metadata_refresh = now_ts + args.metadata_refresh_sec

        if now_ts >= next_scan:
            try:
                observations = detect_observations(api, assets, day_volume, cfg, now_ts)
                update_candidates(states, observations, now_ts, cfg, log_path)
                safe_print(
                    f"[{time.strftime('%H:%M:%S')}] scan complete: observations={len(observations)} states={len(states)}"
                )
            except Exception as exc:  # noqa: BLE001
                safe_print(f"[{time.strftime('%H:%M:%S')}] scan error: {exc}")
            next_scan = now_ts + cfg.scan_interval_sec

        try:
            mids = api.all_mids()
            process_mid_updates(mids, states, cfg, time.time(), log_path)
        except Exception as exc:  # noqa: BLE001
            safe_print(f"[{time.strftime('%H:%M:%S')}] mids error: {exc}")

        print_dashboard(states, cfg)
        time.sleep(args.mids_poll_sec)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hyperliquid density bounce screener")
    parser.add_argument("--min-wall-ratio", type=float, default=10.0)
    parser.add_argument("--min-wall-usd", type=float, default=50_000.0)
    parser.add_argument("--min-day-volume-usd", type=float, default=1_000_000.0)
    parser.add_argument("--market-filter", choices=["all", "perp", "spot"], default="all")
    parser.add_argument("--approach-ticks", type=int, default=4)
    parser.add_argument("--breakout-ticks", type=int, default=2)
    parser.add_argument("--bounce-pct", type=float, default=0.5)
    parser.add_argument("--min-persistence-sec", type=int, default=20)
    parser.add_argument("--max-signal-age-sec", type=int, default=300)
    parser.add_argument("--scan-interval-sec", type=int, default=20)
    parser.add_argument("--mids-poll-sec", type=float, default=1.0)
    parser.add_argument("--metadata-refresh-sec", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--http-timeout-sec", type=int, default=12)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--log-path", default="data/signal_events.jsonl")
    parser.add_argument("--quality-rules-json", default="", help="Path to output of derive_quality_rules.py")
    parser.add_argument("--quality-profile", choices=["strict", "balanced", "flow"], default="strict")
    parser.add_argument("--run-seconds", type=int, default=0, help="0 = run forever")
    return parser.parse_args()


def apply_quality_profile(args: argparse.Namespace) -> None:
    if not args.quality_rules_json:
        return

    path = Path(args.quality_rules_json)
    if not path.exists():
        safe_print(f"quality profile skipped: file not found: {path}")
        return

    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        safe_print(f"quality profile skipped: cannot parse JSON ({exc})")
        return

    profiles = obj.get("profiles")
    if not isinstance(profiles, dict):
        safe_print("quality profile skipped: 'profiles' section missing")
        return

    profile = profiles.get(args.quality_profile)
    if not isinstance(profile, dict):
        safe_print(f"quality profile skipped: profile '{args.quality_profile}' unavailable")
        return

    market = profile.get("market", "all")
    if market not in {"all", "perp", "spot"}:
        market = "all"

    args.market_filter = market
    args.min_wall_ratio = float(profile.get("min_ratio", args.min_wall_ratio))
    args.min_wall_usd = float(profile.get("min_notional_usd", args.min_wall_usd))
    args.min_day_volume_usd = float(profile.get("min_day_volume_usd", args.min_day_volume_usd))

    safe_print(
        "Applied quality profile "
        f"'{args.quality_profile}': market={args.market_filter}, "
        f"min_ratio={args.min_wall_ratio}, min_wall_usd={args.min_wall_usd}, "
        f"min_day_volume_usd={args.min_day_volume_usd}"
    )


if __name__ == "__main__":
    run(parse_args())
