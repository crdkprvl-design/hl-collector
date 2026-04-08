from __future__ import annotations

import datetime as dt
import queue
import threading
import tkinter as tk
from tkinter import ttk
from typing import Callable

from hyperliquid_client import HyperliquidClient, MarketSnapshot
from screener import ScreenerFilters, apply_filters


class ScreenerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Hyperliquid Desktop Screener")
        self.root.geometry("1160x700")
        self.root.minsize(980, 620)

        self.client = HyperliquidClient()
        self.snapshots: list[MarketSnapshot] = []
        self.last_update_at: dt.datetime | None = None

        self.sort_column = "volume_24h_usd"
        self.sort_descending = True
        self.fetch_in_progress = False
        self.refresh_job: str | None = None
        self.result_queue: queue.Queue[tuple[str, object]] = queue.Queue()

        self.dex_var = tk.StringVar(value="")
        self.min_volume_var = tk.StringVar(value="1000000")
        self.min_change_var = tk.StringVar(value="2")
        self.min_funding_var = tk.StringVar(value="0.01")
        self.search_var = tk.StringVar(value="")
        self.interval_var = tk.StringVar(value="10")
        self.auto_refresh_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="Ready")

        self._build_ui()
        self._start_result_listener()
        self.root.after(100, self.refresh_now)
        self._schedule_next_refresh()

    def _build_ui(self) -> None:
        control_frame = ttk.Frame(self.root, padding=12)
        control_frame.pack(fill=tk.X)

        ttk.Label(control_frame, text="Dex (optional):").grid(row=0, column=0, sticky="w")
        ttk.Entry(control_frame, width=16, textvariable=self.dex_var).grid(
            row=1, column=0, padx=(0, 8), sticky="w"
        )

        ttk.Label(control_frame, text="Min volume 24h (USD):").grid(row=0, column=1, sticky="w")
        ttk.Entry(control_frame, width=16, textvariable=self.min_volume_var).grid(
            row=1, column=1, padx=(0, 8), sticky="w"
        )

        ttk.Label(control_frame, text="Min abs 24h change (%):").grid(row=0, column=2, sticky="w")
        ttk.Entry(control_frame, width=12, textvariable=self.min_change_var).grid(
            row=1, column=2, padx=(0, 8), sticky="w"
        )

        ttk.Label(control_frame, text="Min abs funding (%):").grid(row=0, column=3, sticky="w")
        ttk.Entry(control_frame, width=12, textvariable=self.min_funding_var).grid(
            row=1, column=3, padx=(0, 8), sticky="w"
        )

        ttk.Label(control_frame, text="Symbol search:").grid(row=0, column=4, sticky="w")
        ttk.Entry(control_frame, width=14, textvariable=self.search_var).grid(
            row=1, column=4, padx=(0, 8), sticky="w"
        )

        ttk.Label(control_frame, text="Refresh (sec):").grid(row=0, column=5, sticky="w")
        interval_entry = ttk.Entry(control_frame, width=8, textvariable=self.interval_var)
        interval_entry.grid(row=1, column=5, padx=(0, 8), sticky="w")
        interval_entry.bind("<Return>", lambda _: self._schedule_next_refresh())

        ttk.Checkbutton(
            control_frame,
            text="Auto refresh",
            variable=self.auto_refresh_var,
            command=self._schedule_next_refresh,
        ).grid(row=1, column=6, padx=(0, 10), sticky="w")

        ttk.Button(control_frame, text="Apply filters", command=self.render_table).grid(
            row=1, column=7, padx=(0, 6), sticky="w"
        )
        ttk.Button(control_frame, text="Refresh now", command=self.refresh_now).grid(
            row=1, column=8, padx=(0, 6), sticky="w"
        )

        columns = (
            ("symbol", "Symbol", 130, "w"),
            ("mark_price", "Mark price", 120, "e"),
            ("change_24h_pct", "24h %", 100, "e"),
            ("funding_rate_pct", "Funding %", 100, "e"),
            ("open_interest", "Open interest", 140, "e"),
            ("volume_24h_usd", "24h volume $", 160, "e"),
            ("premium_pct", "Premium %", 100, "e"),
        )

        table_frame = ttk.Frame(self.root, padding=(12, 0, 12, 0))
        table_frame.pack(fill=tk.BOTH, expand=True)

        self.table = ttk.Treeview(table_frame, columns=[c[0] for c in columns], show="headings")
        self.table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.table.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.table.configure(yscrollcommand=scrollbar.set)

        for key, title, width, anchor in columns:
            self.table.heading(key, text=title, command=self._heading_handler(key))
            self.table.column(key, width=width, anchor=anchor, stretch=True)

        self.table.tag_configure("positive", foreground="#0B7D2C")
        self.table.tag_configure("negative", foreground="#A12424")
        self.table.tag_configure("neutral", foreground="#333333")

        status_frame = ttk.Frame(self.root, padding=12)
        status_frame.pack(fill=tk.X)
        ttk.Label(status_frame, textvariable=self.status_var).pack(anchor="w")

    def _heading_handler(self, column: str) -> Callable[[], None]:
        def handler() -> None:
            if self.sort_column == column:
                self.sort_descending = not self.sort_descending
            else:
                self.sort_column = column
                self.sort_descending = column != "symbol"
            self.render_table()

        return handler

    def _start_result_listener(self) -> None:
        self.root.after(150, self._drain_result_queue)

    def _drain_result_queue(self) -> None:
        while True:
            try:
                event, payload = self.result_queue.get_nowait()
            except queue.Empty:
                break

            if event == "data":
                self.snapshots = payload if isinstance(payload, list) else []
                self.last_update_at = dt.datetime.now()
                self.render_table()
            elif event == "error":
                self.status_var.set(f"Fetch error: {payload}")
            elif event == "done":
                self.fetch_in_progress = False

        self.root.after(150, self._drain_result_queue)

    def refresh_now(self) -> None:
        if self.fetch_in_progress:
            return

        self.fetch_in_progress = True
        dex = self.dex_var.get().strip()
        self.status_var.set("Fetching markets from Hyperliquid...")

        worker = threading.Thread(target=self._fetch_worker, args=(dex,), daemon=True)
        worker.start()

    def _fetch_worker(self, dex: str) -> None:
        try:
            data = self.client.fetch_perp_market_snapshots(dex=dex)
            self.result_queue.put(("data", data))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("error", str(exc)))
        finally:
            self.result_queue.put(("done", None))

    def _schedule_next_refresh(self) -> None:
        if self.refresh_job is not None:
            self.root.after_cancel(self.refresh_job)
            self.refresh_job = None

        if not self.auto_refresh_var.get():
            return

        interval_seconds = self._safe_int(self.interval_var.get(), fallback=10, minimum=2)
        self.refresh_job = self.root.after(interval_seconds * 1000, self._auto_refresh_tick)

    def _auto_refresh_tick(self) -> None:
        self.refresh_now()
        self._schedule_next_refresh()

    def current_filters(self) -> ScreenerFilters:
        return ScreenerFilters(
            min_volume_usd=self._safe_float(self.min_volume_var.get(), fallback=0.0, minimum=0.0),
            min_abs_change_pct=self._safe_float(self.min_change_var.get(), fallback=0.0, minimum=0.0),
            min_abs_funding_pct=self._safe_float(self.min_funding_var.get(), fallback=0.0, minimum=0.0),
            symbol_contains=self.search_var.get().strip(),
        )

    def render_table(self) -> None:
        filtered = apply_filters(self.snapshots, self.current_filters())
        sorted_rows = self._sorted_snapshots(filtered)

        self.table.delete(*self.table.get_children())
        for row in sorted_rows:
            tag = "neutral"
            if row.change_24h_pct > 0:
                tag = "positive"
            elif row.change_24h_pct < 0:
                tag = "negative"

            self.table.insert(
                "",
                tk.END,
                values=(
                    row.symbol,
                    self._fmt_price(row.mark_price),
                    f"{row.change_24h_pct:+.2f}",
                    f"{row.funding_rate_pct:+.4f}",
                    self._fmt_num(row.open_interest),
                    self._fmt_num(row.volume_24h_usd),
                    f"{row.premium_pct:+.4f}",
                ),
                tags=(tag,),
            )

        if self.last_update_at is None:
            self.status_var.set("Ready")
            return

        updated_at = self.last_update_at.strftime("%H:%M:%S")
        self.status_var.set(
            f"Showing {len(sorted_rows)} of {len(self.snapshots)} markets | Updated {updated_at}"
        )

    def _sorted_snapshots(self, rows: list[MarketSnapshot]) -> list[MarketSnapshot]:
        if self.sort_column == "symbol":
            return sorted(rows, key=lambda x: x.symbol.lower(), reverse=self.sort_descending)

        return sorted(
            rows,
            key=lambda x: self._numeric_column_value(x, self.sort_column),
            reverse=self.sort_descending,
        )

    @staticmethod
    def _numeric_column_value(row: MarketSnapshot, column: str) -> float:
        if column == "mark_price":
            return row.mark_price
        if column == "change_24h_pct":
            return row.change_24h_pct
        if column == "funding_rate_pct":
            return row.funding_rate_pct
        if column == "open_interest":
            return row.open_interest
        if column == "volume_24h_usd":
            return row.volume_24h_usd
        if column == "premium_pct":
            return row.premium_pct
        return 0.0

    @staticmethod
    def _fmt_price(value: float) -> str:
        if value >= 1000:
            return f"{value:,.2f}"
        if value >= 1:
            return f"{value:,.4f}"
        return f"{value:.6f}"

    @staticmethod
    def _fmt_num(value: float) -> str:
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"{value / 1_000:.2f}K"
        return f"{value:.2f}"

    @staticmethod
    def _safe_float(value: str, fallback: float, minimum: float = 0.0) -> float:
        try:
            return max(float(value), minimum)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _safe_int(value: str, fallback: int, minimum: int = 1) -> int:
        try:
            return max(int(value), minimum)
        except (TypeError, ValueError):
            return fallback


def main() -> None:
    root = tk.Tk()
    try:
        ttk.Style(root).theme_use("clam")
    except tk.TclError:
        pass
    ScreenerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
