from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import pandas as pd
import requests


BINANCE_BASE_URL = "https://api.binance.com"


@dataclass(frozen=True)
class BinanceKlinesRequest:
    symbol: str                 # e.g. "BTCUSDT"
    interval: str               # e.g. "1d", "1h"
    start: datetime             # timezone-aware UTC recommended
    end: datetime               # timezone-aware UTC recommended
    limit: int = 1000           # max 1000 per request


def _to_millis(dt: datetime) -> int:
    """Convert datetime to milliseconds since epoch."""
    if dt.tzinfo is None:
        # Treat naive datetime as UTC to avoid silent timezone bugs
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_klines(req: BinanceKlinesRequest, sleep_s: float = 0.2) -> pd.DataFrame:
    """
    Fetch OHLCV klines from Binance Spot public API.

    Returns DataFrame indexed by UTC datetime with columns:
    ["open", "high", "low", "close", "volume"].
    """
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    start_ms = _to_millis(req.start)
    end_ms = _to_millis(req.end)

    all_rows: List[List[Any]] = []
    cur_start = start_ms

    while cur_start < end_ms:
        params = {
            "symbol": req.symbol.upper(),
            "interval": req.interval,
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": req.limit,
        }

        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        if not data:
            break

        all_rows.extend(data)

        # Next page: startTime = last_open_time + 1 ms
        last_open_time = data[-1][0]
        cur_start = last_open_time + 1

        # be polite to API / avoid hitting limits too fast
        time.sleep(sleep_s)

        # Safety: if API returns the same last_open_time repeatedly, stop
        if len(data) == 1 and last_open_time + 1 == cur_start:
            break

    if not all_rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(
        all_rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
    )

    # Types
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.set_index("open_time")[["open", "high", "low", "close", "volume"]].sort_index()

    # Drop duplicates just in case
    df = df[~df.index.duplicated(keep="last")]

    return df


def build_close_series(
    symbols: List[str],
    interval: str,
    start: datetime,
    end: datetime,
    sleep_s: float = 0.2,
) -> pd.DataFrame:
    """
    Download close prices for many symbols and return wide DataFrame:
    index=datetime(UTC), columns=symbol, values=close.
    """
    closes = []
    for sym in symbols:
        req = BinanceKlinesRequest(symbol=sym, interval=interval, start=start, end=end)
        df = fetch_klines(req, sleep_s=sleep_s)
        if df.empty:
            continue
        s = df["close"].rename(sym.upper())
        closes.append(s)

    if not closes:
        return pd.DataFrame()

    wide = pd.concat(closes, axis=1).sort_index()
    return wide
