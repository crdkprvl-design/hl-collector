from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import math
import os
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

try:
    import joblib
except Exception:  # noqa: BLE001
    joblib = None


INFO_ENDPOINT = "https://api.hyperliquid.xyz/info"
WS_ENDPOINT = "wss://api.hyperliquid.xyz/ws"
MIN_NOTIONAL_EPSILON_RATIO = 0.99
COLLECTOR_STARTED_AT_TS = time.time()
COLLECTOR_INSTANCE_ID = f"{os.getpid()}-{int(COLLECTOR_STARTED_AT_TS * 1000)}"
_COLLECTOR_LOG_SEQ = 0


@dataclass(frozen=True)
class ScreenerConfig:
    market_filter: str = "all"
    side_filter: str = "all"
    min_wall_ratio: float = 10.0
    max_wall_ratio: float = math.inf
    min_wall_notional_usd: float = 0.0
    max_wall_notional_usd: float = math.inf
    min_day_volume_usd: float = 1_000_000.0
    max_day_volume_usd: float = math.inf
    max_wall_distance_from_spread_pct: float = 3.0
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
    spread_ref_price: float
    wall_distance_from_spread_pct: float
    wall_notional_usd: float
    wall_ratio: float
    wall_dominance_ratio: float
    wall_level_index: int
    tick_size: float
    day_volume_usd: float
    seen_at: float


@dataclass
class CandidateState:
    candidate_id: str
    obs: WallObservation
    first_seen: float
    last_seen: float
    touched_at: float | None = None
    touch_price: float | None = None
    status: str = "watching"
    last_mid: float | None = None
    seen_count: int = 1
    max_wall_notional_usd: float = 0.0
    min_wall_notional_usd: float = math.inf
    sum_wall_notional_usd: float = 0.0
    notional_samples: list[float] = field(default_factory=list)
    distance_samples: list[float] = field(default_factory=list)
    sample_timestamps: list[float] = field(default_factory=list)
    rebuild_count: int = 0
    missing_episodes: int = 0
    last_missing_started_at: float | None = None
    touch_seen_count: int = 0
    touch_attempt_count: int = 0
    updates_before_touch: int = 0
    touch_wall_notional_usd: float = 0.0
    touch_distance_pct: float = 0.0
    pre_touch_notional_1: float | None = None
    pre_touch_notional_2: float | None = None
    pre_touch_notional_3: float | None = None
    touch_mfe_pct: float = 0.0
    touch_mae_pct: float = 0.0
    touch_best_mid: float | None = None
    touch_worst_mid: float | None = None


@dataclass(frozen=True)
class LiveMLRanker:
    model: Any
    top_frac: float = 0.10


class HyperliquidAPI:
    def __init__(
        self,
        timeout_sec: int = 12,
        min_request_interval_sec: float = 0.04,
        max_retries: int = 2,
    ) -> None:
        self.session = requests.Session()
        self.session.trust_env = False
        self.timeout_sec = timeout_sec
        self.min_request_interval_sec = max(0.0, float(min_request_interval_sec))
        self.max_retries = max(0, int(max_retries))
        self._request_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._next_request_at = 0.0
        self._rate_limit_hits = 0

    def _acquire_request_slot(self) -> None:
        if self.min_request_interval_sec <= 0:
            return
        with self._request_lock:
            now = time.monotonic()
            wait_sec = self._next_request_at - now
            if wait_sec > 0:
                time.sleep(wait_sec)
                now = time.monotonic()
            self._next_request_at = now + self.min_request_interval_sec

    def _mark_rate_limit_hit(self) -> None:
        with self._stats_lock:
            self._rate_limit_hits += 1

    def consume_rate_limit_hits(self) -> int:
        with self._stats_lock:
            hits = self._rate_limit_hits
            self._rate_limit_hits = 0
            return hits

    def _post(self, payload: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._acquire_request_slot()
            try:
                response = self.session.post(INFO_ENDPOINT, json=payload, timeout=self.timeout_sec)
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(min(2.0, 0.2 * (attempt + 1)))
                continue

            if response.status_code == 429:
                self._mark_rate_limit_hit()
                retry_after = 0.0
                raw_retry_after = response.headers.get("Retry-After")
                if raw_retry_after:
                    try:
                        retry_after = float(raw_retry_after)
                    except (TypeError, ValueError):
                        retry_after = 0.0
                if attempt >= self.max_retries:
                    response.raise_for_status()
                time.sleep(max(retry_after, min(4.0, 0.35 * (attempt + 1) ** 2)))
                continue

            if response.status_code >= 500 and attempt < self.max_retries:
                time.sleep(min(2.0, 0.2 * (attempt + 1)))
                continue

            response.raise_for_status()
            return response.json()

        if last_error is not None:
            raise RuntimeError(f"HTTP request failed: {last_error}") from last_error
        raise RuntimeError("HTTP request failed after retries")

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


class WsMarketFeed:
    def __init__(self, ws_url: str, subscribe_mids: bool = True) -> None:
        self.ws_url = ws_url
        self.subscribe_mids = subscribe_mids
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._coins: tuple[str, ...] = tuple()
        self._mids: dict[str, float] = {}
        self._books: dict[str, dict[str, Any]] = {}
        self._book_ts: dict[str, float] = {}
        self._last_mids_ts: float = 0.0
        self._connected: bool = False
        self._last_error: str = ""

    def close(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=3.0)
        self._thread = None
        with self._lock:
            self._connected = False

    def configure(self, coins: list[str]) -> bool:
        unique_sorted = tuple(sorted({c for c in coins if isinstance(c, str) and c}))
        with self._lock:
            current = self._coins
        if unique_sorted == current and self._thread and self._thread.is_alive():
            return True

        self.close()
        self._stop_event = threading.Event()
        with self._lock:
            self._coins = unique_sorted
            self._books = {}
            self._book_ts = {}
            self._mids = {}
            self._last_mids_ts = 0.0
            self._connected = False
            self._last_error = ""
        self._thread = threading.Thread(target=self._run_thread, name="hl-ws-feed", daemon=True)
        self._thread.start()
        return True

    def status(self) -> tuple[bool, str]:
        with self._lock:
            return self._connected, self._last_error

    def mids_snapshot(self) -> tuple[dict[str, float], float]:
        with self._lock:
            return dict(self._mids), self._last_mids_ts

    def books_snapshot(self, coins: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
        with self._lock:
            out_books: dict[str, dict[str, Any]] = {}
            out_ts: dict[str, float] = {}
            for coin in coins:
                book = self._books.get(coin)
                if isinstance(book, dict):
                    out_books[coin] = book
                    out_ts[coin] = self._book_ts.get(coin, 0.0)
            return out_books, out_ts

    def _run_thread(self) -> None:
        try:
            asyncio.run(self._run_loop())
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self._connected = False
                self._last_error = f"ws thread error: {exc}"

    async def _run_loop(self) -> None:
        backoff = 1.0
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=3,
                    max_queue=4096,
                ) as ws:
                    with self._lock:
                        self._connected = True
                        self._last_error = ""
                        coins = self._coins

                    if self.subscribe_mids:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
                    for coin in coins:
                        await ws.send(
                            json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}})
                        )
                        await asyncio.sleep(0.003)

                    backoff = 1.0
                    while not self._stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except TimeoutError:
                            continue
                        self._handle_message(raw)
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._last_error = str(exc)
                    self._connected = False

            if self._stop_event.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 8.0)

    def _handle_message(self, raw: Any) -> None:
        try:
            payload = json.loads(raw)
        except Exception:
            return
        if not isinstance(payload, dict):
            return

        now_ts = time.time()
        channel = str(payload.get("channel", "")).lower()
        data = payload.get("data")
        subscription = payload.get("subscription")
        sub_type = ""
        sub_coin = ""
        if isinstance(subscription, dict):
            sub_type = str(subscription.get("type", "")).lower()
            sub_coin = str(subscription.get("coin", "")).strip()

        if channel == "allmids" or sub_type == "allmids":
            mids_obj: dict[str, Any] | None = None
            if isinstance(data, dict) and isinstance(data.get("mids"), dict):
                mids_obj = data.get("mids")
            elif isinstance(data, dict):
                mids_obj = data
            if mids_obj is None:
                return
            mids: dict[str, float] = {}
            for coin, val in mids_obj.items():
                try:
                    mids[str(coin)] = float(val)
                except (TypeError, ValueError):
                    continue
            if not mids:
                return
            with self._lock:
                self._mids.update(mids)
                self._last_mids_ts = now_ts
            return

        if channel == "l2book" or sub_type == "l2book":
            if not isinstance(data, dict):
                return
            coin = str(data.get("coin") or sub_coin or "").strip()
            levels = data.get("levels")
            if not coin or not isinstance(levels, list):
                return
            with self._lock:
                self._books[coin] = data
                self._book_ts[coin] = now_ts


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


def meets_min_notional(value_usd: float, min_required_usd: float) -> bool:
    if min_required_usd <= 0:
        return True
    # Keep a small tolerance for boundary rounding in API payloads.
    return value_usd >= (min_required_usd * MIN_NOTIONAL_EPSILON_RATIO)


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
    spread_ref_price: float,
    max_distance_pct: float,
) -> WallObservation | None:
    if spread_ref_price <= 0:
        return None

    rows: list[tuple[int, float, float, float, float]] = []
    for idx, lvl in enumerate(side_levels):
        px = to_float(lvl.get("px"))
        sz = to_float(lvl.get("sz"))
        if px <= 0 or sz <= 0:
            continue
        if side == "bid":
            distance_pct = ((spread_ref_price - px) / spread_ref_price) * 100.0
        else:
            distance_pct = ((px - spread_ref_price) / spread_ref_price) * 100.0
        if distance_pct < 0:
            continue
        if distance_pct > max_distance_pct:
            continue
        rows.append((idx, px, sz, px * sz, distance_pct))

    if len(rows) < 3:
        return None

    notionals = [x[3] for x in rows]
    med_notional = statistics.median(notionals)
    if med_notional <= 0:
        return None

    best_local_idx = max(range(len(rows)), key=lambda i: rows[i][3])
    wall_level_index, wall_px, _, wall_notional, wall_distance_pct = rows[best_local_idx]
    wall_ratio = wall_notional / med_notional
    sorted_notionals = sorted(notionals, reverse=True)
    second_notional = sorted_notionals[1] if len(sorted_notionals) > 1 else med_notional
    wall_dominance_ratio = wall_notional / max(second_notional, 1e-9)
    tick_size = detect_tick_size([x[1] for x in rows])

    return WallObservation(
        market=market,
        coin=coin,
        display_symbol=display_symbol,
        side=side,
        wall_price=wall_px,
        spread_ref_price=spread_ref_price,
        wall_distance_from_spread_pct=wall_distance_pct,
        wall_notional_usd=wall_notional,
        wall_ratio=wall_ratio,
        wall_dominance_ratio=wall_dominance_ratio,
        wall_level_index=wall_level_index,
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
    distance_den = max(cfg.max_wall_distance_from_spread_pct, 1e-9)
    ratio_component = min(obs.wall_ratio / ratio_den, 1.5)
    notional_component = min(obs.wall_notional_usd / notional_den, 1.5)
    volume_component = min(obs.day_volume_usd / volume_den, 1.5)
    dominance_component = min(obs.wall_dominance_ratio / 4.0, 1.5)
    distance_component = max(0.0, 1.0 - min(obs.wall_distance_from_spread_pct / distance_den, 1.0))
    raw = (
        (0.36 * ratio_component)
        + (0.28 * notional_component)
        + (0.14 * volume_component)
        + (0.12 * dominance_component)
        + (0.10 * distance_component)
    )
    return raw * 100.0


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


def liquidity_bucket(day_volume_usd: float) -> str:
    if day_volume_usd >= 25_000_000:
        return "majors"
    if day_volume_usd >= 2_500_000:
        return "mids"
    return "trash"


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def behavior_tag_from_metrics(obs: WallObservation, metrics: dict[str, Any]) -> str:
    volatility = max(0.0, to_float(metrics.get("wall_notional_volatility_ratio"), 0.0))
    pre_touch_decay = max(0.0, to_float(metrics.get("pre_touch_decay_ratio"), 1.0))
    rebuild_count = max(0.0, to_float(metrics.get("rebuild_count"), 0.0))
    dist_pct = max(0.0, obs.wall_distance_from_spread_pct)
    approach_speed = max(0.0, to_float(metrics.get("approach_speed_pct_per_sec"), 0.0))
    stability = max(0.0, to_float(metrics.get("wall_notional_stability_ratio"), 0.0))
    current_vs_peak = max(0.0, to_float(metrics.get("wall_notional_current_vs_peak_ratio"), 0.0))

    if rebuild_count >= 2 or volatility >= 0.55:
        return "FLICKER"
    if pre_touch_decay < 0.78:
        return "DECAYING"
    if dist_pct < 0.20 or approach_speed >= 0.07:
        return "AGGRESSIVE"
    if stability >= 0.80 and current_vs_peak >= 0.80 and pre_touch_decay >= 0.92:
        return "CLEAN"
    return "STABLE"


def entry_score_from_metrics(metrics: dict[str, Any]) -> float:
    touch_attempt_count = max(0.0, to_float(metrics.get("touch_attempt_count"), 0.0))
    updates_before_touch = max(0.0, to_float(metrics.get("updates_before_touch"), 0.0))
    seen_count = max(0.0, to_float(metrics.get("seen_count"), 0.0))
    approach_speed = max(0.0, to_float(metrics.get("approach_speed_pct_per_sec"), 0.0))
    touch_survival_sec = max(0.0, to_float(metrics.get("touch_survival_sec"), 0.0))
    pre_touch_decay_ratio = max(0.0, to_float(metrics.get("pre_touch_decay_ratio"), 1.0))
    distance_compression_ratio = max(0.0, to_float(metrics.get("distance_compression_ratio"), 1.0))
    stability_ratio = max(0.0, to_float(metrics.get("wall_notional_stability_ratio"), 0.0))
    current_vs_peak_ratio = max(0.0, to_float(metrics.get("wall_notional_current_vs_peak_ratio"), 0.0))

    establishment_updates = updates_before_touch if updates_before_touch > 0.0 else seen_count
    establishment_bonus = min(math.log1p(establishment_updates) / math.log1p(20.0), 1.35)
    freshness_bonus = 1.0 if touch_attempt_count <= 1.0 else max(0.0, 1.0 - ((touch_attempt_count - 1.0) / 3.0))
    aggression_penalty = min(approach_speed / 0.08, 1.25)
    survival_bonus = min(touch_survival_sec / 15.0, 1.5)
    decay_bonus = min(pre_touch_decay_ratio, 1.25)
    compression_bonus = clamp(1.0 - abs(1.0 - distance_compression_ratio), 0.0, 1.0)
    stability_bonus = min(max(stability_ratio, current_vs_peak_ratio), 1.2)

    score = (
        24.0 * establishment_bonus
        + 14.0 * freshness_bonus
        + 14.0 * decay_bonus
        + 10.0 * compression_bonus
        + 8.0 * stability_bonus
        + 12.0 * survival_bonus
        - 18.0 * aggression_penalty
        - 10.0 * max(0.0, touch_attempt_count - 1.0)
    )
    return max(0.0, score)


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


def _side_weighting_profile(obs: WallObservation) -> dict[str, Any]:
    side = obs.side if obs.side in {"bid", "ask"} else "bid"
    bucket = liquidity_bucket(obs.day_volume_usd)
    if side == "ask":
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
        "side": side,
        "bucket": bucket,
        "profile": f"{side}_{bucket}",
        "near_spread_weight": near_spread_weight,
        "volatility_weight": volatility_weight,
        "decay_weight": decay_weight,
        "rebuild_weight": rebuild_weight,
        "approach_weight": 9.0,
    }


def _build_ml_feature_row(obs: WallObservation, metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "market": obs.market,
        "side": obs.side,
        "liquidity_bucket": liquidity_bucket(obs.day_volume_usd),
        "wall_ratio": max(0.0, obs.wall_ratio),
        "wall_dominance_ratio": max(0.0, obs.wall_dominance_ratio),
        "wall_notional_usd": max(0.0, obs.wall_notional_usd),
        "wall_distance_from_spread_pct": max(0.0, obs.wall_distance_from_spread_pct),
        "day_volume_usd": max(0.0, obs.day_volume_usd),
        "visible_age_sec": max(0.0, to_float(metrics.get("visible_age_sec"), 0.0)),
        "seen_count": max(0.0, to_float(metrics.get("seen_count"), 0.0)),
        "wall_notional_stability_ratio": max(0.0, to_float(metrics.get("wall_notional_stability_ratio"), 0.0)),
        "round_level_score": max(0.0, to_float(metrics.get("round_level_score"), 0.0)),
        "wall_notional_volatility_ratio": max(0.0, to_float(metrics.get("wall_notional_volatility_ratio"), 0.0)),
        "distance_volatility_ratio": max(0.0, to_float(metrics.get("distance_volatility_ratio"), 0.0)),
        "pre_touch_decay_ratio": max(0.0, to_float(metrics.get("pre_touch_decay_ratio"), 1.0)),
        "distance_compression_ratio": max(0.0, to_float(metrics.get("distance_compression_ratio"), 1.0)),
        "rebuild_count": max(0.0, to_float(metrics.get("rebuild_count"), 0.0)),
        "touch_survival_sec": max(0.0, to_float(metrics.get("touch_survival_sec"), 0.0)),
        "touch_attempt_count": max(0.0, to_float(metrics.get("touch_attempt_count"), 0.0)),
        "updates_before_touch": max(0.0, to_float(metrics.get("updates_before_touch"), 0.0)),
        "approach_speed_pct_per_sec": max(0.0, to_float(metrics.get("approach_speed_pct_per_sec"), 0.0)),
    }


def load_live_ml_ranker(model_path: Path) -> LiveMLRanker | None:
    if joblib is None:
        return None
    if not model_path.exists():
        return None
    try:
        model = joblib.load(model_path)
    except Exception:
        return None
    return LiveMLRanker(model=model)


def predict_live_ml_proba(ml_ranker: LiveMLRanker | None, obs: WallObservation, metrics: dict[str, Any]) -> float:
    if ml_ranker is None:
        return 0.0
    feature_row = _build_ml_feature_row(obs, metrics)
    try:
        proba = ml_ranker.model.predict_proba([feature_row])[0][1]
        return max(0.0, min(1.0, float(proba)))
    except Exception:
        return 0.0


def _score_state_handcrafted(
    obs: WallObservation,
    metrics: dict[str, Any],
    cfg: ScreenerConfig,
    weighting: dict[str, Any] | None = None,
) -> float:
    """Behavior-aware score with side/liquidity-specific weighting."""
    base = score_candidate(obs, cfg)

    visible_age = to_float(metrics.get("visible_age_sec"), 0.0)
    age_component = min(visible_age / 60.0, 1.5)
    stability = to_float(metrics.get("wall_notional_stability_ratio"), 0.0)
    current_vs_peak = to_float(metrics.get("wall_notional_current_vs_peak_ratio"), 0.0)
    wall_to_hour = max(0.0, to_float(metrics.get("wall_to_hour_volume_pct"), 0.0))
    wall_to_hour_component = min(math.log1p(wall_to_hour) / math.log1p(80.0), 1.5)
    roundness = max(0.0, to_float(metrics.get("round_level_score"), 0.0))

    dist_pct = max(0.0, obs.wall_distance_from_spread_pct)
    near_spread_penalty = 0.0
    if dist_pct < 0.20:
        near_spread_penalty = min((0.20 - dist_pct) / 0.20, 1.0)

    volatility_ratio = max(0.0, to_float(metrics.get("wall_notional_volatility_ratio"), 0.0))
    pre_touch_decay_ratio = max(0.0, to_float(metrics.get("pre_touch_decay_ratio"), 1.0))
    rebuild_count = max(0.0, to_float(metrics.get("rebuild_count"), 0.0))
    rebuild_penalty = min(rebuild_count / 3.0, 1.5)
    decay_penalty = max(0.0, 1.0 - pre_touch_decay_ratio)
    approach_speed = max(0.0, to_float(metrics.get("approach_speed_pct_per_sec"), 0.0))
    approach_penalty = min(approach_speed / 0.08, 1.0)
    weights = weighting or _side_weighting_profile(obs)

    boosted = (
        base
        + (18.0 * age_component)
        + (22.0 * stability)
        + (18.0 * current_vs_peak)
        + (16.0 * wall_to_hour_component)
        + (6.0 * roundness)
        - (float(weights["near_spread_weight"]) * near_spread_penalty)
        - (float(weights["volatility_weight"]) * volatility_ratio)
        - (float(weights["decay_weight"]) * decay_penalty)
        - (float(weights["rebuild_weight"]) * rebuild_penalty)
        - (float(weights["approach_weight"]) * approach_penalty)
    )
    return max(0.0, boosted)


def score_state_candidate(
    state: CandidateState,
    cfg: ScreenerConfig,
    now_ts: float,
    ml_ranker: LiveMLRanker | None = None,
) -> float:
    metrics = state_metrics(state, now_ts)
    handcrafted_score = _score_state_handcrafted(state.obs, metrics, cfg)
    ml_proba = predict_live_ml_proba(ml_ranker, state.obs, metrics)
    # Soft-rank blend: preserve handcrafted signal quality and let ML refine ordering.
    return (0.55 * handcrafted_score) + (0.45 * (ml_proba * 100.0))


def score_state_breakdown(
    state: CandidateState,
    cfg: ScreenerConfig,
    now_ts: float,
    ml_ranker: LiveMLRanker | None = None,
) -> dict[str, Any]:
    metrics = state_metrics(state, now_ts)
    weighting = _side_weighting_profile(state.obs)
    handcrafted_score = _score_state_handcrafted(state.obs, metrics, cfg, weighting=weighting)
    ml_proba = predict_live_ml_proba(ml_ranker, state.obs, metrics)
    final_score = (0.55 * handcrafted_score) + (0.45 * (ml_proba * 100.0))
    return {
        "metrics": metrics,
        "handcrafted_score": handcrafted_score,
        "ml_proba": ml_proba,
        "final_score": final_score,
        "entry_score": to_float(metrics.get("entry_score"), 0.0),
        "behavior_tag": behavior_tag_from_metrics(state.obs, metrics),
        "liquidity_bucket": weighting["bucket"],
        "side_weighting_profile": weighting["profile"],
        "scoring_weights": {
            "near_spread_weight": weighting["near_spread_weight"],
            "volatility_weight": weighting["volatility_weight"],
            "decay_weight": weighting["decay_weight"],
            "rebuild_weight": weighting["rebuild_weight"],
            "approach_weight": weighting["approach_weight"],
        },
    }


def detect_observations(
    api: HyperliquidAPI,
    assets: list[tuple[str, str, str]],
    day_volume: dict[str, float],
    cfg: ScreenerConfig,
    now_ts: float,
    mids: dict[str, float] | None = None,
    books: dict[str, dict[str, Any]] | None = None,
) -> list[WallObservation]:
    observations: list[WallObservation] = []

    def best_px(levels: list[dict[str, Any]]) -> float:
        for lvl in levels:
            px = to_float(lvl.get("px"))
            sz = to_float(lvl.get("sz"))
            if px > 0 and sz > 0:
                return px
        return 0.0

    def task(asset: tuple[str, str, str]) -> list[WallObservation]:
        market, coin, display = asset
        if cfg.market_filter != "all" and market != cfg.market_filter:
            return []
        coin_day_volume = day_volume.get(coin, 0.0)
        if coin_day_volume < cfg.min_day_volume_usd:
            return []
        if coin_day_volume > cfg.max_day_volume_usd:
            return []

        try:
            book = None
            if books is not None:
                cached = books.get(coin)
                if isinstance(cached, dict):
                    book = cached
            if book is None:
                book = api.l2_book(coin)
        except Exception:
            return []
        if not book:
            return []
        levels = book.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            return []
        bid_levels = levels[0] if isinstance(levels[0], list) else []
        ask_levels = levels[1] if isinstance(levels[1], list) else []
        top_bid = best_px(bid_levels)
        top_ask = best_px(ask_levels)
        spread_mid = 0.0
        if top_bid > 0 and top_ask > 0:
            spread_mid = (top_bid + top_ask) / 2.0
        mid_ref = 0.0
        if mids is not None:
            mid_ref = to_float(mids.get(coin), 0.0)
        # Use same-snapshot book midpoint first to keep dist% aligned with visible DOM.
        # allMids is only a fallback when top-of-book is unavailable.
        distance_ref = spread_mid if spread_mid > 0 else mid_ref

        result: list[WallObservation] = []
        for side_name, side_levels in (("bid", levels[0]), ("ask", levels[1])):
            if not isinstance(side_levels, list):
                continue
            if cfg.side_filter != "all" and side_name != cfg.side_filter:
                continue
            spread_ref = distance_ref if distance_ref > 0 else (top_bid if side_name == "bid" else top_ask)
            if spread_ref <= 0:
                continue
            obs = detect_side_wall(
                side_levels=side_levels,
                side=side_name,
                market=market,
                coin=coin,
                display_symbol=display,
                day_volume_usd=day_volume.get(coin, 0.0),
                seen_at=now_ts,
                spread_ref_price=spread_ref,
                max_distance_pct=cfg.max_wall_distance_from_spread_pct,
            )
            if obs is None:
                continue
            if obs.wall_ratio < cfg.min_wall_ratio:
                continue
            if obs.wall_ratio > cfg.max_wall_ratio:
                continue
            if obs.wall_notional_usd > cfg.max_wall_notional_usd:
                continue
            if obs.wall_distance_from_spread_pct > cfg.max_wall_distance_from_spread_pct:
                continue
            result.append(obs)
        return result

    with ThreadPoolExecutor(max_workers=cfg.concurrency) as executor:
        futures = [executor.submit(task, asset) for asset in assets]
        for fut in as_completed(futures):
            try:
                observations.extend(fut.result())
            except Exception:
                continue

    return observations


def candidate_key(obs: WallObservation) -> str:
    if obs.tick_size > 0:
        bucket = int(round(obs.wall_price / obs.tick_size))
        return f"{obs.market}|{obs.coin}|{obs.side}|tick={obs.tick_size:.12f}|bucket={bucket}"
    return f"{obs.market}|{obs.coin}|{obs.side}|px={obs.wall_price:.10f}"


def round_level_score(price: float, tick_size: float) -> float:
    if price <= 0 or tick_size <= 0:
        return 0.0

    tick_bucket = int(round(price / tick_size))
    for modulus, score in (
        (1000, 1.00),
        (500, 0.85),
        (100, 0.65),
        (50, 0.45),
        (25, 0.30),
        (10, 0.15),
        (5, 0.08),
    ):
        if tick_bucket % modulus == 0:
            return score
    return 0.0


def state_metrics(state: CandidateState, now_ts: float) -> dict[str, Any]:
    peak_usd = max(state.max_wall_notional_usd, state.obs.wall_notional_usd)
    floor_usd = min(state.min_wall_notional_usd, state.obs.wall_notional_usd)
    mean_usd = state.sum_wall_notional_usd / max(state.seen_count, 1)
    hour_volume_usd_est = 0.0
    wall_to_hour_volume_pct = 0.0
    if state.obs.day_volume_usd > 0:
        hour_volume_usd_est = state.obs.day_volume_usd / 24.0
        if hour_volume_usd_est > 0:
            wall_to_hour_volume_pct = (state.obs.wall_notional_usd / hour_volume_usd_est) * 100.0

    notional_tail = state.notional_samples[-8:] if state.notional_samples else [state.obs.wall_notional_usd]
    distance_tail = state.distance_samples[-8:] if state.distance_samples else [state.obs.wall_distance_from_spread_pct]
    sample_ts = state.sample_timestamps[-8:] if state.sample_timestamps else [state.last_seen]
    volatility_ratio = _safe_std_ratio(notional_tail)
    distance_volatility_ratio = _safe_std_ratio(distance_tail)

    pre_touch_decay_ratio = 1.0
    if state.pre_touch_notional_1 is not None and state.pre_touch_notional_3 is not None and state.pre_touch_notional_3 > 0:
        pre_touch_decay_ratio = state.pre_touch_notional_1 / state.pre_touch_notional_3
    elif len(notional_tail) >= 3:
        oldest = max(notional_tail[-3], 1e-9)
        pre_touch_decay_ratio = notional_tail[-1] / oldest

    distance_compression_ratio = 1.0
    approach_speed_pct_per_sec = 0.0
    if len(distance_tail) >= 3:
        recent = distance_tail[-3:]
        recent_max = max(recent)
        if recent_max > 0:
            distance_compression_ratio = min(recent) / recent_max
    if len(distance_tail) >= 2 and len(sample_ts) >= 2:
        dt = max(sample_ts[-1] - sample_ts[0], 1e-6)
        approach_speed_pct_per_sec = (distance_tail[0] - distance_tail[-1]) / dt

    metrics: dict[str, Any] = {
        "visible_age_sec": max(0.0, now_ts - state.first_seen),
        "seen_count": state.seen_count,
        "wall_notional_peak_usd": peak_usd,
        "wall_notional_floor_usd": floor_usd,
        "wall_notional_mean_usd": mean_usd,
        "wall_notional_current_vs_peak_ratio": state.obs.wall_notional_usd / max(peak_usd, 1e-9),
        "wall_notional_stability_ratio": floor_usd / max(peak_usd, 1e-9),
        "round_level_score": round_level_score(state.obs.wall_price, state.obs.tick_size),
        "hour_volume_usd_est": hour_volume_usd_est,
        "wall_to_hour_volume_pct": wall_to_hour_volume_pct,
        "wall_notional_volatility_ratio": volatility_ratio,
        "distance_volatility_ratio": distance_volatility_ratio,
        "pre_touch_decay_ratio": pre_touch_decay_ratio,
        "distance_compression_ratio": distance_compression_ratio,
        "approach_speed_pct_per_sec": approach_speed_pct_per_sec,
        "liquidity_bucket": liquidity_bucket(state.obs.day_volume_usd),
        "rebuild_count": state.rebuild_count,
        "missing_episodes": state.missing_episodes,
        "touch_seen_count": state.touch_seen_count,
        "touch_attempt_count": state.touch_attempt_count,
        "updates_before_touch": state.updates_before_touch,
        "touch_wall_notional_usd": state.touch_wall_notional_usd,
        "touch_distance_pct": state.touch_distance_pct,
        "touch_mfe_pct": state.touch_mfe_pct,
        "touch_mae_pct": state.touch_mae_pct,
        "touch_best_mid": state.touch_best_mid if state.touch_best_mid is not None else 0.0,
        "touch_worst_mid": state.touch_worst_mid if state.touch_worst_mid is not None else 0.0,
    }
    if state.touched_at is not None:
        metrics["time_from_first_seen_to_touch_sec"] = max(0.0, state.touched_at - state.first_seen)
        metrics["time_since_touch_sec"] = max(0.0, now_ts - state.touched_at)
        metrics["touch_survival_sec"] = max(0.0, now_ts - state.touched_at)
    metrics["entry_score"] = entry_score_from_metrics(metrics)
    return metrics


def log_event(log_path: Path, payload: dict[str, Any]) -> None:
    global _COLLECTOR_LOG_SEQ
    _COLLECTOR_LOG_SEQ += 1
    enriched = dict(payload)
    if to_float(enriched.get("ts"), 0.0) <= 0:
        enriched["ts"] = time.time()
    enriched["collector_pid"] = os.getpid()
    enriched["collector_started_at_ts"] = COLLECTOR_STARTED_AT_TS
    enriched["collector_instance_id"] = COLLECTOR_INSTANCE_ID
    enriched["collector_log_seq"] = _COLLECTOR_LOG_SEQ
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(enriched, ensure_ascii=True) + "\n")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                check=False,
            )
            out = (proc.stdout or "").strip()
            if not out:
                return False
            no_task_markers = (
                "No tasks are running which match the specified criteria.",
                "Информация: задачи, соответствующие заданным критериям, не запущены.",
            )
            if any(marker in out for marker in no_task_markers):
                return False
            return True
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except Exception:
        return False
    return True


def acquire_collector_lock(lock_path: Path) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_payload = {
        "pid": os.getpid(),
        "collector_instance_id": COLLECTOR_INSTANCE_ID,
        "collector_started_at_ts": COLLECTOR_STARTED_AT_TS,
    }

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            existing_pid = 0
            try:
                existing = json.loads(lock_path.read_text(encoding="utf-8"))
                existing_pid = int(to_float(existing.get("pid"), 0.0))
            except Exception:
                existing_pid = 0
            if existing_pid > 0 and _pid_alive(existing_pid):
                safe_print(
                    f"Collector lock is busy: {lock_path} "
                    f"(active pid={existing_pid}). Stop the previous collector first."
                )
                return False
            try:
                lock_path.unlink()
            except OSError:
                try:
                    # Some Windows/OneDrive setups deny delete but allow overwrite.
                    lock_path.write_text(json.dumps(lock_payload, ensure_ascii=True), encoding="utf-8")
                    break
                except Exception:
                    safe_print(f"Collector lock exists but cannot be replaced: {lock_path}")
                    return False
            continue

        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            fp.write(json.dumps(lock_payload, ensure_ascii=True))
        break

    def _release() -> None:
        try:
            if not lock_path.exists():
                return
            existing = json.loads(lock_path.read_text(encoding="utf-8"))
            existing_pid = int(to_float(existing.get("pid"), 0.0))
            if existing_pid == os.getpid():
                lock_path.unlink(missing_ok=True)
        except Exception:
            return

    atexit.register(_release)
    return True


def print_dashboard(
    states: dict[str, CandidateState],
    cfg: ScreenerConfig,
    ml_ranker: LiveMLRanker | None = None,
) -> None:
    active = [s for s in states.values() if s.status in {"armed", "touched"}]
    now_ts = time.time()
    active.sort(key=lambda s: score_state_candidate(s, cfg, now_ts, ml_ranker=ml_ranker), reverse=True)
    safe_print("\n" + "=" * 120)
    safe_print(f"Active candidates: {len(active)} | top: {min(cfg.top_n, len(active))}")
    safe_print("-" * 120)
    safe_print(
        "score  market symbol                     side wall_px        wall_usd    ratio  dist%  vol24      status"
    )
    safe_print("-" * 120)
    for st in active[: cfg.top_n]:
        score = score_state_candidate(st, cfg, now_ts, ml_ranker=ml_ranker)
        safe_print(
            f"{score:5.1f}  "
            f"{st.obs.market:5s} "
            f"{st.obs.display_symbol[:24]:24s} "
            f"{st.obs.side:4s} "
            f"{st.obs.wall_price:12.8f} "
            f"{format_usd(st.obs.wall_notional_usd):10s} "
            f"{st.obs.wall_ratio:6.2f} "
            f"{st.obs.wall_distance_from_spread_pct:6.2f} "
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
    def touch_progress_pct(side: str, touch_price: float, current_mid: float) -> tuple[float, float]:
        if touch_price <= 0:
            return 0.0, 0.0
        if side == "bid":
            favorable = ((current_mid - touch_price) / touch_price) * 100.0
        else:
            favorable = ((touch_price - current_mid) / touch_price) * 100.0
        adverse = max(0.0, -favorable)
        return favorable, adverse

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
                    notional_tail = st.notional_samples[-3:] if st.notional_samples else [obs.wall_notional_usd]
                    if len(notional_tail) >= 1:
                        st.pre_touch_notional_1 = notional_tail[-1]
                    if len(notional_tail) >= 2:
                        st.pre_touch_notional_2 = notional_tail[-2]
                    if len(notional_tail) >= 3:
                        st.pre_touch_notional_3 = notional_tail[-3]
                    st.status = "touched"
                    st.touched_at = now_ts
                    st.touch_price = mid
                    st.touch_seen_count = st.seen_count
                    st.touch_attempt_count += 1
                    st.updates_before_touch = st.seen_count
                    st.touch_wall_notional_usd = obs.wall_notional_usd
                    st.touch_distance_pct = obs.wall_distance_from_spread_pct
                    st.touch_mfe_pct = 0.0
                    st.touch_mae_pct = 0.0
                    st.touch_best_mid = mid
                    st.touch_worst_mid = mid
                    log_event(
                        log_path,
                        {
                            "ts": now_ts,
                            "event": "touched",
                            "candidate_id": st.candidate_id,
                            "market": obs.market,
                            "coin": obs.coin,
                            "display_symbol": obs.display_symbol,
                            "side": obs.side,
                            "wall_price": obs.wall_price,
                            "spread_ref_price": obs.spread_ref_price,
                            "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                            "touch_price": mid,
                            "wall_ratio": obs.wall_ratio,
                            "wall_dominance_ratio": obs.wall_dominance_ratio,
                            "wall_level_index": obs.wall_level_index,
                            "wall_notional_usd": obs.wall_notional_usd,
                            "day_volume_usd": obs.day_volume_usd,
                            **state_metrics(st, now_ts),
                        },
                    )
            else:
                near_px = obs.wall_price - (cfg.approach_ticks * obs.tick_size)
                if mid >= near_px:
                    notional_tail = st.notional_samples[-3:] if st.notional_samples else [obs.wall_notional_usd]
                    if len(notional_tail) >= 1:
                        st.pre_touch_notional_1 = notional_tail[-1]
                    if len(notional_tail) >= 2:
                        st.pre_touch_notional_2 = notional_tail[-2]
                    if len(notional_tail) >= 3:
                        st.pre_touch_notional_3 = notional_tail[-3]
                    st.status = "touched"
                    st.touched_at = now_ts
                    st.touch_price = mid
                    st.touch_seen_count = st.seen_count
                    st.touch_attempt_count += 1
                    st.updates_before_touch = st.seen_count
                    st.touch_wall_notional_usd = obs.wall_notional_usd
                    st.touch_distance_pct = obs.wall_distance_from_spread_pct
                    st.touch_mfe_pct = 0.0
                    st.touch_mae_pct = 0.0
                    st.touch_best_mid = mid
                    st.touch_worst_mid = mid
                    log_event(
                        log_path,
                        {
                            "ts": now_ts,
                            "event": "touched",
                            "candidate_id": st.candidate_id,
                            "market": obs.market,
                            "coin": obs.coin,
                            "display_symbol": obs.display_symbol,
                            "side": obs.side,
                            "wall_price": obs.wall_price,
                            "spread_ref_price": obs.spread_ref_price,
                            "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                            "touch_price": mid,
                            "wall_ratio": obs.wall_ratio,
                            "wall_dominance_ratio": obs.wall_dominance_ratio,
                            "wall_level_index": obs.wall_level_index,
                            "wall_notional_usd": obs.wall_notional_usd,
                            "day_volume_usd": obs.day_volume_usd,
                            **state_metrics(st, now_ts),
                        },
                    )
            continue

        if st.status != "touched":
            continue
        if st.touched_at is None:
            continue

        if st.touch_price is not None and st.touch_price > 0:
            favorable_pct, adverse_pct = touch_progress_pct(obs.side, st.touch_price, mid)
            st.touch_mfe_pct = max(st.touch_mfe_pct, favorable_pct)
            st.touch_mae_pct = max(st.touch_mae_pct, adverse_pct)
            if st.touch_best_mid is None:
                st.touch_best_mid = mid
            else:
                if obs.side == "bid":
                    st.touch_best_mid = max(st.touch_best_mid, mid)
                else:
                    st.touch_best_mid = min(st.touch_best_mid, mid)
            if st.touch_worst_mid is None:
                st.touch_worst_mid = mid
            else:
                if obs.side == "bid":
                    st.touch_worst_mid = min(st.touch_worst_mid, mid)
                else:
                    st.touch_worst_mid = max(st.touch_worst_mid, mid)

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
                        "candidate_id": st.candidate_id,
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "spread_ref_price": obs.spread_ref_price,
                        "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                        "touch_price": st.touch_price,
                        "last_mid": mid,
                        "time_to_fail_sec": max(0.0, now_ts - st.touched_at),
                        "wall_ratio": obs.wall_ratio,
                        "wall_dominance_ratio": obs.wall_dominance_ratio,
                        "wall_level_index": obs.wall_level_index,
                        "wall_notional_usd": obs.wall_notional_usd,
                        **state_metrics(st, now_ts),
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
                        "candidate_id": st.candidate_id,
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "spread_ref_price": obs.spread_ref_price,
                        "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                        "touch_price": st.touch_price,
                        "exit_mid": mid,
                        "time_to_bounce_sec": max(0.0, now_ts - st.touched_at),
                        "fast_bounce_10s": bool((now_ts - st.touched_at) <= 10.0),
                        "fast_bounce_20s": bool((now_ts - st.touched_at) <= 20.0),
                        "fast_bounce_30s": bool((now_ts - st.touched_at) <= 30.0),
                        "bounce_pct_target": cfg.bounce_pct,
                        "wall_ratio": obs.wall_ratio,
                        "wall_dominance_ratio": obs.wall_dominance_ratio,
                        "wall_level_index": obs.wall_level_index,
                        "wall_notional_usd": obs.wall_notional_usd,
                        **state_metrics(st, now_ts),
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
                        "candidate_id": st.candidate_id,
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "spread_ref_price": obs.spread_ref_price,
                        "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                        "touch_price": st.touch_price,
                        "last_mid": mid,
                        "time_to_fail_sec": max(0.0, now_ts - st.touched_at),
                        "wall_ratio": obs.wall_ratio,
                        "wall_dominance_ratio": obs.wall_dominance_ratio,
                        "wall_level_index": obs.wall_level_index,
                        "wall_notional_usd": obs.wall_notional_usd,
                        **state_metrics(st, now_ts),
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
                        "candidate_id": st.candidate_id,
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "spread_ref_price": obs.spread_ref_price,
                        "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                        "touch_price": st.touch_price,
                        "exit_mid": mid,
                        "time_to_bounce_sec": max(0.0, now_ts - st.touched_at),
                        "fast_bounce_10s": bool((now_ts - st.touched_at) <= 10.0),
                        "fast_bounce_20s": bool((now_ts - st.touched_at) <= 20.0),
                        "fast_bounce_30s": bool((now_ts - st.touched_at) <= 30.0),
                        "bounce_pct_target": cfg.bounce_pct,
                        "wall_ratio": obs.wall_ratio,
                        "wall_dominance_ratio": obs.wall_dominance_ratio,
                        "wall_level_index": obs.wall_level_index,
                        "wall_notional_usd": obs.wall_notional_usd,
                        **state_metrics(st, now_ts),
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
                    "candidate_id": st.candidate_id,
                    "market": obs.market,
                    "coin": obs.coin,
                    "display_symbol": obs.display_symbol,
                    "side": obs.side,
                    "wall_price": obs.wall_price,
                    "spread_ref_price": obs.spread_ref_price,
                    "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                    "touch_price": st.touch_price,
                    "last_mid": mid,
                    "time_to_expire_sec": max(0.0, now_ts - st.touched_at),
                    "wall_ratio": obs.wall_ratio,
                    "wall_dominance_ratio": obs.wall_dominance_ratio,
                    "wall_level_index": obs.wall_level_index,
                    "wall_notional_usd": obs.wall_notional_usd,
                    **state_metrics(st, now_ts),
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
            states[key] = CandidateState(
                candidate_id=key,
                obs=obs,
                first_seen=now_ts,
                last_seen=now_ts,
                status="watching",
                seen_count=1,
                max_wall_notional_usd=obs.wall_notional_usd,
                min_wall_notional_usd=obs.wall_notional_usd,
                sum_wall_notional_usd=obs.wall_notional_usd,
                notional_samples=[obs.wall_notional_usd],
                distance_samples=[obs.wall_distance_from_spread_pct],
                sample_timestamps=[now_ts],
            )
            continue

        if existing.last_missing_started_at is not None:
            if (now_ts - existing.last_missing_started_at) <= max(18.0, cfg.scan_interval_sec * 2.2):
                existing.rebuild_count += 1
            existing.last_missing_started_at = None

        existing.obs = obs
        existing.last_seen = now_ts
        existing.seen_count += 1
        existing.max_wall_notional_usd = max(existing.max_wall_notional_usd, obs.wall_notional_usd)
        existing.min_wall_notional_usd = min(existing.min_wall_notional_usd, obs.wall_notional_usd)
        existing.sum_wall_notional_usd += obs.wall_notional_usd
        _append_limited(existing.notional_samples, obs.wall_notional_usd)
        _append_limited(existing.distance_samples, obs.wall_distance_from_spread_pct)
        _append_limited(existing.sample_timestamps, now_ts)
        if existing.status == "watching" and now_ts - existing.first_seen >= cfg.min_persistence_sec:
            existing.status = "armed"
            log_event(
                log_path,
                {
                    "ts": now_ts,
                    "event": "armed",
                    "candidate_id": existing.candidate_id,
                    "market": obs.market,
                    "coin": obs.coin,
                    "display_symbol": obs.display_symbol,
                    "side": obs.side,
                    "wall_price": obs.wall_price,
                    "spread_ref_price": obs.spread_ref_price,
                    "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                    "wall_ratio": obs.wall_ratio,
                    "wall_dominance_ratio": obs.wall_dominance_ratio,
                    "wall_level_index": obs.wall_level_index,
                    "wall_notional_usd": obs.wall_notional_usd,
                    "day_volume_usd": obs.day_volume_usd,
                    **state_metrics(existing, now_ts),
                },
            )
            continue

        if existing.status in {"armed", "touched"}:
            # Heartbeat keeps GUI/feed consumers synchronized while the wall is still present.
            log_event(
                log_path,
                {
                    "ts": now_ts,
                    "event": "heartbeat",
                    "candidate_id": existing.candidate_id,
                    "candidate_status": existing.status,
                    "market": obs.market,
                    "coin": obs.coin,
                    "display_symbol": obs.display_symbol,
                    "side": obs.side,
                    "wall_price": obs.wall_price,
                    "spread_ref_price": obs.spread_ref_price,
                    "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                    "wall_ratio": obs.wall_ratio,
                    "wall_dominance_ratio": obs.wall_dominance_ratio,
                    "wall_level_index": obs.wall_level_index,
                    "wall_notional_usd": obs.wall_notional_usd,
                    "day_volume_usd": obs.day_volume_usd,
                    **state_metrics(existing, now_ts),
                },
            )

    for key, state in list(states.items()):
        if key in seen_keys:
            continue
        if state.last_missing_started_at is None:
            state.last_missing_started_at = now_ts
            state.missing_episodes += 1
        stale_sec = now_ts - state.last_seen
        # Keep walls alive longer to avoid false dropouts on transient feed/API gaps.
        armed_missing_ttl = max(75.0, cfg.scan_interval_sec * 5.0)
        watching_missing_ttl = max(90.0, cfg.scan_interval_sec * 6.0)
        touched_missing_ttl = max(120.0, cfg.scan_interval_sec * 8.0)
        if state.status == "armed":
            ttl = armed_missing_ttl
        elif state.status == "touched":
            ttl = touched_missing_ttl
        else:
            ttl = watching_missing_ttl
        if stale_sec > ttl:
            if state.status in {"armed", "touched"}:
                obs = state.obs
                log_event(
                    log_path,
                    {
                        "ts": now_ts,
                        "event": "disarmed_missing",
                        "candidate_id": state.candidate_id,
                        "candidate_status": state.status,
                        "market": obs.market,
                        "coin": obs.coin,
                        "display_symbol": obs.display_symbol,
                        "side": obs.side,
                        "wall_price": obs.wall_price,
                        "spread_ref_price": obs.spread_ref_price,
                        "wall_distance_from_spread_pct": obs.wall_distance_from_spread_pct,
                        "wall_ratio": obs.wall_ratio,
                        "wall_dominance_ratio": obs.wall_dominance_ratio,
                        "wall_level_index": obs.wall_level_index,
                        "wall_notional_usd": obs.wall_notional_usd,
                        "day_volume_usd": obs.day_volume_usd,
                        "missing_for_sec": stale_sec,
                        **state_metrics(state, now_ts),
                    },
                )
            del states[key]


def log_selection_snapshot(
    states: dict[str, CandidateState],
    cfg: ScreenerConfig,
    now_ts: float,
    log_path: Path,
    ml_ranker: LiveMLRanker | None = None,
    event_name: str = "selection_snapshot",
    allowed_statuses: set[str] | None = None,
) -> None:
    statuses = allowed_statuses or {"armed", "touched"}
    active = [s for s in states.values() if s.status in statuses]
    if not active:
        return
    active.sort(key=lambda s: score_state_candidate(s, cfg, now_ts, ml_ranker=ml_ranker), reverse=True)
    selected: list[dict[str, Any]] = []
    for rank, st in enumerate(active[: cfg.top_n], start=1):
        score_info = score_state_breakdown(st, cfg, now_ts, ml_ranker=ml_ranker)
        selected.append(
            {
                "rank": rank,
                "candidate_id": st.candidate_id,
                "market": st.obs.market,
                "coin": st.obs.coin,
                "display_symbol": st.obs.display_symbol,
                "side": st.obs.side,
                "wall_price": st.obs.wall_price,
                "wall_notional_usd": st.obs.wall_notional_usd,
                "wall_distance_from_spread_pct": st.obs.wall_distance_from_spread_pct,
                "bounce_score": score_info["handcrafted_score"],
                "final_score": score_info["final_score"],
                "ml_proba": score_info["ml_proba"],
                "entry_score": score_info["entry_score"],
                "behavior_tag": score_info["behavior_tag"],
                "liquidity_bucket": score_info["liquidity_bucket"],
                "side_weighting_profile": score_info["side_weighting_profile"],
                "status": st.status,
            }
        )
    log_event(
        log_path,
        {
            "ts": now_ts,
            "event": event_name,
            "selection_size": len(selected),
            "selected": selected,
        },
    )


def run(args: argparse.Namespace) -> None:
    apply_quality_profile(args)
    cfg = ScreenerConfig(
        market_filter=args.market_filter,
        side_filter=args.side_filter,
        min_wall_ratio=args.min_wall_ratio,
        max_wall_ratio=args.max_wall_ratio,
        min_wall_notional_usd=args.min_wall_usd,
        max_wall_notional_usd=args.max_wall_usd,
        min_day_volume_usd=args.min_day_volume_usd,
        max_day_volume_usd=args.max_day_volume_usd,
        max_wall_distance_from_spread_pct=args.max_wall_distance_pct,
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
    ml_ranker = load_live_ml_ranker(Path(args.ml_model_path))
    if ml_ranker is not None:
        safe_print(f"ML ranker loaded: {args.ml_model_path}")
    else:
        safe_print("ML ranker unavailable, using handcrafted ranking only")
    ws_enabled = args.book_source in {"auto", "ws"} and websockets is not None
    ws_feed: WsMarketFeed | None = None
    ws_book_coins: list[str] = []
    log_path = Path(args.log_path)
    lock_path = log_path.with_suffix(log_path.suffix + ".lock")
    if not acquire_collector_lock(lock_path):
        return
    if args.book_source == "ws" and websockets is None:
        safe_print("WS mode requested, but python package 'websockets' is unavailable. Falling back to polling.")
    if ws_enabled:
        ws_feed = WsMarketFeed(args.ws_url, subscribe_mids=True)

    while True:
        try:
            assets, day_volume = read_universe(api)
            safe_print(f"Loaded assets: {len(assets)} | day volume keys: {len(day_volume)}")
            break
        except Exception as exc:  # noqa: BLE001
            safe_print(f"[{time.strftime('%H:%M:%S')}] init universe error: {exc}")
            time.sleep(5)

    def choose_ws_book_coins(
        universe_assets: list[tuple[str, str, str]],
        universe_day_volume: dict[str, float],
    ) -> list[str]:
        if ws_feed is None:
            return []
        coins = sorted({coin for _, coin, _ in universe_assets})
        if args.ws_max_books <= 0 or len(coins) <= args.ws_max_books:
            return coins
        ranked = sorted(coins, key=lambda coin: universe_day_volume.get(coin, 0.0), reverse=True)
        selected = ranked[: args.ws_max_books]
        safe_print(
            f"[{time.strftime('%H:%M:%S')}] ws l2 subscriptions limited: {len(selected)}/{len(coins)} (ws-max-books={args.ws_max_books})"
        )
        return selected

    if ws_feed is not None:
        ws_book_coins = choose_ws_book_coins(assets, day_volume)
        ws_feed.configure(ws_book_coins)
        safe_print(
            f"[{time.strftime('%H:%M:%S')}] ws feed enabled: mids=on, books={len(ws_book_coins)}"
        )

    states: dict[str, CandidateState] = {}
    next_metadata_refresh = 0.0
    next_scan = 0.0
    started_at = time.time()

    try:
        while True:
            now_ts = time.time()
            if args.run_seconds > 0 and now_ts - started_at >= args.run_seconds:
                safe_print(f"[{time.strftime('%H:%M:%S')}] run_seconds reached, exiting.")
                break
            if now_ts >= next_metadata_refresh:
                try:
                    assets, day_volume = read_universe(api)
                    safe_print(f"[{time.strftime('%H:%M:%S')}] universe refresh: {len(assets)} assets")
                    if ws_feed is not None:
                        ws_book_coins = choose_ws_book_coins(assets, day_volume)
                        ws_feed.configure(ws_book_coins)
                except Exception as exc:  # noqa: BLE001
                    safe_print(f"[{time.strftime('%H:%M:%S')}] universe refresh error: {exc}")
                next_metadata_refresh = now_ts + args.metadata_refresh_sec

            scan_mids: dict[str, float] = {}
            if now_ts >= next_scan:
                if ws_feed is not None:
                    mids_snapshot, mids_ts = ws_feed.mids_snapshot()
                    if mids_snapshot and (now_ts - mids_ts) <= args.ws_mids_stale_sec:
                        scan_mids = mids_snapshot
                if not scan_mids:
                    try:
                        scan_mids = api.all_mids()
                    except Exception:
                        scan_mids = {}

                scan_books: dict[str, dict[str, Any]] | None = None
                ws_books_used = 0
                if ws_feed is not None and ws_book_coins:
                    scan_coins = [coin for _, coin, _ in assets]
                    books_snapshot, books_ts = ws_feed.books_snapshot(scan_coins)
                    fresh_books: dict[str, dict[str, Any]] = {}
                    for coin, book in books_snapshot.items():
                        ts = books_ts.get(coin, 0.0)
                        if ts > 0 and (now_ts - ts) <= args.ws_book_stale_sec:
                            fresh_books[coin] = book
                    if fresh_books:
                        scan_books = fresh_books
                        ws_books_used = len(fresh_books)

                try:
                    observations = detect_observations(
                        api,
                        assets,
                        day_volume,
                        cfg,
                        now_ts,
                        mids=scan_mids,
                        books=scan_books,
                    )
                    update_candidates(states, observations, now_ts, cfg, log_path)
                    log_selection_snapshot(states, cfg, now_ts, log_path, ml_ranker=ml_ranker)
                    log_selection_snapshot(
                        states,
                        cfg,
                        now_ts,
                        log_path,
                        ml_ranker=ml_ranker,
                        event_name="selection_snapshot_pre_touch",
                        allowed_statuses={"armed"},
                    )
                    safe_print(
                        f"[{time.strftime('%H:%M:%S')}] scan complete: observations={len(observations)} states={len(states)} ws_books={ws_books_used}"
                    )
                except Exception as exc:  # noqa: BLE001
                    safe_print(f"[{time.strftime('%H:%M:%S')}] scan error: {exc}")
                next_scan = now_ts + cfg.scan_interval_sec

            mids_for_state = scan_mids
            if not mids_for_state and ws_feed is not None:
                mids_snapshot, mids_ts = ws_feed.mids_snapshot()
                if mids_snapshot and (time.time() - mids_ts) <= args.ws_mids_stale_sec:
                    mids_for_state = mids_snapshot
            if not mids_for_state:
                try:
                    mids_for_state = api.all_mids()
                except Exception as exc:  # noqa: BLE001
                    safe_print(f"[{time.strftime('%H:%M:%S')}] mids error: {exc}")
                    mids_for_state = {}
            if mids_for_state:
                process_mid_updates(mids_for_state, states, cfg, time.time(), log_path)

            print_dashboard(states, cfg, ml_ranker=ml_ranker)
            time.sleep(args.mids_poll_sec)
    finally:
        if ws_feed is not None:
            ws_feed.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hyperliquid density bounce screener")
    parser.add_argument("--min-wall-ratio", type=float, default=10.0)
    parser.add_argument("--max-wall-ratio", type=float, default=float("inf"))
    parser.add_argument("--min-wall-usd", type=float, default=0.0)
    parser.add_argument("--max-wall-usd", type=float, default=float("inf"))
    parser.add_argument("--min-day-volume-usd", type=float, default=1_000_000.0)
    parser.add_argument("--max-day-volume-usd", type=float, default=float("inf"))
    parser.add_argument("--max-wall-distance-pct", type=float, default=3.0)
    parser.add_argument("--market-filter", choices=["all", "perp", "spot"], default="all")
    parser.add_argument("--side-filter", choices=["all", "bid", "ask"], default="all")
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
    parser.add_argument("--book-source", choices=["auto", "polling", "ws"], default="auto")
    parser.add_argument("--ws-url", default=WS_ENDPOINT)
    parser.add_argument(
        "--ws-max-books",
        type=int,
        default=220,
        help="0 = subscribe l2Book for all assets; otherwise cap number of WS l2 subscriptions",
    )
    parser.add_argument("--ws-book-stale-sec", type=float, default=12.0)
    parser.add_argument("--ws-mids-stale-sec", type=float, default=6.0)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--log-path", default="data/signal_events.jsonl")
    parser.add_argument("--ml-model-path", default="data/models/bounce_model.joblib")
    parser.add_argument("--quality-rules-json", default="", help="Path to output of derive_quality_rules.py")
    parser.add_argument("--quality-profile", choices=["strict", "balanced", "flow", "actionable", "trash_bid", "trash_ask"], default="strict")
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
        profile = _quality_profile_overrides(args.quality_profile)
    if not isinstance(profile, dict):
        safe_print(f"quality profile skipped: profile '{args.quality_profile}' unavailable")
        return

    market = profile.get("market", "all")
    if market not in {"all", "perp", "spot"}:
        market = "all"
    side = profile.get("side", "all")
    if side not in {"all", "bid", "ask"}:
        side = "all"

    args.market_filter = market
    args.side_filter = side
    args.min_wall_ratio = float(profile.get("min_ratio", args.min_wall_ratio))
    args.max_wall_ratio = float(profile.get("max_ratio", args.max_wall_ratio))
    args.min_wall_usd = float(profile.get("min_notional_usd", args.min_wall_usd))
    args.max_wall_usd = float(profile.get("max_notional_usd", args.max_wall_usd))
    args.min_day_volume_usd = float(profile.get("min_day_volume_usd", args.min_day_volume_usd))
    args.max_day_volume_usd = float(profile.get("max_day_volume_usd", args.max_day_volume_usd))
    args.max_wall_distance_pct = float(
        profile.get("max_wall_distance_pct_from_spread", args.max_wall_distance_pct)
    )

    safe_print(
        "Applied quality profile "
        f"'{args.quality_profile}': market={args.market_filter}, side={args.side_filter}, "
        f"ratio=[{args.min_wall_ratio}, {args.max_wall_ratio}], "
        f"wall_usd=[{args.min_wall_usd}, {args.max_wall_usd}], "
        f"day_volume_usd=[{args.min_day_volume_usd}, {args.max_day_volume_usd}], "
        f"max_wall_distance_pct={args.max_wall_distance_pct}"
    )


if __name__ == "__main__":
    run(parse_args())
