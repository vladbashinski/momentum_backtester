from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List

import pandas as pd
import requests


MOEX_ISS_BASE = "https://iss.moex.com/iss"


@dataclass(frozen=True)
class MoexCandlesRequest:
    ticker: str                 # e.g. "SBER"
    start: datetime             # naive or tz-aware ok
    end: datetime               # naive or tz-aware ok
    board: str = "TQBR"         # most liquid shares board
    engine: str = "stock"
    market: str = "shares"
    interval: int = 24          # 24 = daily candles in MOEX ISS
    timeout_s: int = 30


def fetch_candles(req: MoexCandlesRequest, sleep_s: float = 0.2) -> pd.DataFrame:
    """
    Fetch daily candles from MOEX ISS for one security.
    Returns DataFrame indexed by datetime (naive) with columns:
    ["open","high","low","close","value","volume"] (subset depends on ISS response).
    """
    url = (
        f"{MOEX_ISS_BASE}/engines/{req.engine}/markets/{req.market}/boards/{req.board}"
        f"/securities/{req.ticker.upper()}/candles.json"
    )

    params = {
        "from": req.start.date().isoformat(),
        "till": req.end.date().isoformat(),
        "interval": req.interval,
        "iss.meta": "off",
    }

    r = requests.get(url, params=params, timeout=req.timeout_s)
    r.raise_for_status()
    js = r.json()

    candles = js.get("candles", {})
    cols: List[str] = candles.get("columns", [])
    data: List[List[Any]] = candles.get("data", [])

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=cols)

    # Expected columns include begin/end/open/high/low/close/volume/value
    if "begin" not in df.columns:
        return pd.DataFrame()

    df["begin"] = pd.to_datetime(df["begin"], errors="coerce").dt.tz_localize(None)

    # Convert numeric columns if present
    for c in ["open", "high", "low", "close", "volume", "value"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.set_index("begin").sort_index()

    # Be polite to API
    time.sleep(sleep_s)

    # Drop duplicates just in case
    df = df[~df.index.duplicated(keep="last")]

    return df


def build_close_series(
    tickers: List[str],
    start: datetime,
    end: datetime,
    board: str = "TQBR",
    sleep_s: float = 0.2,
) -> pd.DataFrame:
    """
    Download close prices for many MOEX tickers and return wide DataFrame:
    index=datetime, columns=ticker, values=close.
    """
    closes = []
    for t in tickers:
        req = MoexCandlesRequest(ticker=t, start=start, end=end, board=board)
        df = fetch_candles(req, sleep_s=sleep_s)
        if df.empty or "close" not in df.columns:
            continue
        s = df["close"].rename(t.upper())
        closes.append(s)

    if not closes:
        return pd.DataFrame()

    wide = pd.concat(closes, axis=1).sort_index()
    wide.index.name = "Date"
    return wide
