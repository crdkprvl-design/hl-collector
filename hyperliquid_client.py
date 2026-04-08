from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


INFO_ENDPOINT = "https://api.hyperliquid.xyz/info"


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    mark_price: float
    change_24h_pct: float
    funding_rate_pct: float
    open_interest: float
    volume_24h_usd: float
    premium_pct: float


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class HyperliquidClient:
    def __init__(self, timeout_seconds: int = 12) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def fetch_perp_market_snapshots(self, dex: str = "") -> list[MarketSnapshot]:
        payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            payload["dex"] = dex

        response = self.session.post(
            INFO_ENDPOINT,
            json=payload,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        body = response.json()

        if not isinstance(body, list) or len(body) != 2:
            raise RuntimeError("Unexpected API response shape for metaAndAssetCtxs")

        meta, asset_contexts = body
        if not isinstance(meta, dict) or not isinstance(asset_contexts, list):
            raise RuntimeError("Unexpected API payload types from Hyperliquid")

        universe = meta.get("universe", [])
        if not isinstance(universe, list):
            raise RuntimeError("Unexpected 'universe' format in API response")

        snapshots: list[MarketSnapshot] = []
        for index, ctx in enumerate(asset_contexts):
            if not isinstance(ctx, dict):
                continue

            symbol = self.resolve_symbol(universe=universe, index=index, ctx=ctx)
            mark_price = to_float(ctx.get("markPx"))
            prev_day_price = to_float(ctx.get("prevDayPx"))
            funding_rate_pct = to_float(ctx.get("funding")) * 100.0
            premium_pct = to_float(ctx.get("premium")) * 100.0
            open_interest = to_float(ctx.get("openInterest"))
            volume_24h_usd = to_float(ctx.get("dayNtlVlm"))

            change_24h_pct = 0.0
            if prev_day_price != 0:
                change_24h_pct = ((mark_price - prev_day_price) / prev_day_price) * 100.0

            snapshots.append(
                MarketSnapshot(
                    symbol=symbol,
                    mark_price=mark_price,
                    change_24h_pct=change_24h_pct,
                    funding_rate_pct=funding_rate_pct,
                    open_interest=open_interest,
                    volume_24h_usd=volume_24h_usd,
                    premium_pct=premium_pct,
                )
            )

        return snapshots

    @staticmethod
    def resolve_symbol(universe: list[Any], index: int, ctx: dict[str, Any]) -> str:
        coin = ctx.get("coin")
        if isinstance(coin, str) and coin:
            return coin

        if index < len(universe):
            universe_item = universe[index]
            if isinstance(universe_item, dict):
                name = universe_item.get("name")
                if isinstance(name, str) and name:
                    return name

        return f"PERP_{index}"
