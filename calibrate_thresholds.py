from __future__ import annotations

import argparse
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests


INFO_ENDPOINT = "https://api.hyperliquid.xyz/info"


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def quantile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    idx = (len(values) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def format_usd(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


class API:
    def __init__(self, timeout: int) -> None:
        self.session = requests.Session()
        self.session.trust_env = False
        self.timeout = timeout

    def post(self, payload: dict[str, Any]) -> Any:
        r = self.session.post(INFO_ENDPOINT, json=payload, timeout=self.timeout)
        r.raise_for_status()
        return r.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="One-shot density threshold calibration")
    parser.add_argument("--min-day-volume-usd", type=float, default=1_000_000)
    parser.add_argument("--min-wall-usd", type=float, default=50_000)
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--http-timeout-sec", type=int, default=12)
    parser.add_argument("--top", type=int, default=30)
    args = parser.parse_args()

    api = API(timeout=args.http_timeout_sec)

    perp_meta, perp_ctx = api.post({"type": "metaAndAssetCtxs"})
    spot_meta, spot_ctx = api.post({"type": "spotMetaAndAssetCtxs"})

    perp_uni = perp_meta.get("universe", [])
    spot_uni = spot_meta.get("universe", [])
    spot_tokens = spot_meta.get("tokens", [])

    assets: list[tuple[str, str, str]] = []
    day_volume: dict[str, float] = {}

    for i, item in enumerate(perp_uni):
        if not isinstance(item, dict):
            continue
        coin = item.get("name")
        if not isinstance(coin, str):
            continue
        assets.append(("perp", coin, coin))
        if i < len(perp_ctx) and isinstance(perp_ctx[i], dict):
            day_volume[coin] = to_float(perp_ctx[i].get("dayNtlVlm"))

    for item in spot_uni:
        if not isinstance(item, dict):
            continue
        coin = item.get("name")
        if not isinstance(coin, str):
            continue
        display = coin
        if coin.startswith("@"):
            try:
                base_idx, quote_idx = item["tokens"]
                base_name = spot_tokens[base_idx]["name"]
                quote_name = spot_tokens[quote_idx]["name"]
                display = f"{base_name}/{quote_name} ({coin})"
            except Exception:
                display = coin
        assets.append(("spot", coin, display))

    for ctx in spot_ctx:
        if isinstance(ctx, dict):
            coin = ctx.get("coin")
            if isinstance(coin, str):
                day_volume[coin] = to_float(ctx.get("dayNtlVlm"))

    rows: list[dict[str, Any]] = []

    def worker(asset: tuple[str, str, str]) -> list[dict[str, Any]]:
        market, coin, display = asset
        if day_volume.get(coin, 0.0) < args.min_day_volume_usd:
            return []

        try:
            book = api.post({"type": "l2Book", "coin": coin})
        except Exception:
            return []

        if not isinstance(book, dict):
            return []
        levels = book.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            return []

        local_rows: list[dict[str, Any]] = []
        for side_name, side in (("bid", levels[0]), ("ask", levels[1])):
            if not isinstance(side, list):
                continue
            points: list[tuple[float, float, float]] = []
            for lvl in side:
                px = to_float(lvl.get("px"))
                sz = to_float(lvl.get("sz"))
                if px <= 0 or sz <= 0:
                    continue
                points.append((px, sz, px * sz))
            if len(points) < 3:
                continue
            notionals = [x[2] for x in points]
            med = statistics.median(notionals)
            if med <= 0:
                continue
            idx = max(range(len(points)), key=lambda i: points[i][2])
            wall_px, _, wall_notional = points[idx]
            wall_ratio = wall_notional / med
            local_rows.append(
                {
                    "market": market,
                    "coin": coin,
                    "display_symbol": display,
                    "side": side_name,
                    "wall_price": wall_px,
                    "wall_notional_usd": wall_notional,
                    "wall_ratio": wall_ratio,
                    "day_volume_usd": day_volume.get(coin, 0.0),
                }
            )
        return local_rows

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = [ex.submit(worker, a) for a in assets]
        for fut in as_completed(futures):
            rows.extend(fut.result())

    print(f"Assets scanned: {len(assets)} | sides analyzed: {len(rows)}")

    for market in ("perp", "spot"):
        m = [r for r in rows if r["market"] == market and math.isfinite(r["wall_ratio"])]
        ratios = [r["wall_ratio"] for r in m]
        wall_usd = [r["wall_notional_usd"] for r in m]
        print(f"\n=== {market} ===")
        print(f"sides={len(m)}")
        print(
            "wall_ratio q50/q75/q90/q95/q99: "
            + ", ".join(f"{quantile(ratios, q):.2f}" for q in (0.5, 0.75, 0.9, 0.95, 0.99))
        )
        print(
            "wall_notional_usd q50/q75/q90/q95/q99: "
            + ", ".join(f"{quantile(wall_usd, q):.0f}" for q in (0.5, 0.75, 0.9, 0.95, 0.99))
        )

    candidates = [
        r
        for r in rows
        if r["wall_notional_usd"] >= args.min_wall_usd and math.isfinite(r["wall_ratio"])
    ]
    candidates.sort(key=lambda x: (x["wall_ratio"], x["wall_notional_usd"]), reverse=True)

    print(f"\nTop {min(args.top, len(candidates))} candidates:")
    print("ratio  wall_usd    vol24      market symbol                     side  wall_px")
    for row in candidates[: args.top]:
        print(
            f"{row['wall_ratio']:6.2f} "
            f"{format_usd(row['wall_notional_usd']):10s} "
            f"{format_usd(row['day_volume_usd']):10s} "
            f"{row['market']:5s} "
            f"{row['display_symbol'][:24]:24s} "
            f"{row['side']:4s} "
            f"{row['wall_price']:.8f}"
        )


if __name__ == "__main__":
    main()
