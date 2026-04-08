from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from hyperliquid_client import MarketSnapshot


@dataclass
class ScreenerFilters:
    min_volume_usd: float = 0.0
    min_abs_change_pct: float = 0.0
    min_abs_funding_pct: float = 0.0
    symbol_contains: str = ""


def apply_filters(
    snapshots: Iterable[MarketSnapshot],
    filters: ScreenerFilters,
) -> list[MarketSnapshot]:
    query = filters.symbol_contains.strip().upper()
    result: list[MarketSnapshot] = []

    for snapshot in snapshots:
        if snapshot.volume_24h_usd < filters.min_volume_usd:
            continue
        if abs(snapshot.change_24h_pct) < filters.min_abs_change_pct:
            continue
        if abs(snapshot.funding_rate_pct) < filters.min_abs_funding_pct:
            continue
        if query and query not in snapshot.symbol.upper():
            continue
        result.append(snapshot)

    return result
