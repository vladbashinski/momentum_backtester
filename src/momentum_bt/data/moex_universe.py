from __future__ import annotations
import requests

MOEX_ISS = "https://iss.moex.com/iss"


def _fetch_json(url: str, params: dict | None = None) -> dict:
    p = {"iss.meta": "off"}
    if params:
        p.update(params)
    r = requests.get(url, params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def _extract(js: dict, table: str, col_name: str = "SECID") -> list[str]:
    tbl = js.get(table)
    if not tbl or "columns" not in tbl or "data" not in tbl:
        return []
    cols = [str(c).strip().upper() for c in tbl["columns"]]
    data = tbl["data"]
    if col_name.upper() not in cols:
        return []
    idx = cols.index(col_name.upper())
    out = sorted({row[idx] for row in data if row and len(row) > idx and row[idx]})
    return out


def load_imoex_universe() -> list[str]:
    """
    IMOEX constituents (index members) via MOEX ISS.

    Tries several endpoints; returns list of tickers (SECID).
    """
    # 1) Most reliable: /index/boards/.../securities/IMOEX
    # Some MOEX setups return constituents via 'analytics', others via 'securities'.
    url1 = f"{MOEX_ISS}/engines/stock/markets/index/boards/SNDX/securities/IMOEX.json"
    js1 = _fetch_json(url1, params={"iss.only": "securities"})
    tickers = _extract(js1, "securities", "SECID")
    if tickers:
        return tickers

    # 2) Fallback: analytics endpoint
    url2 = f"{MOEX_ISS}/statistics/engines/stock/markets/index/analytics/IMOEX.json"
    js2 = _fetch_json(url2)
    tickers = _extract(js2, "analytics", "SECID") or _extract(js2, "analytics_allowable", "SECID")
    if tickers:
        return tickers

    raise ValueError(f"Could not load IMOEX universe. Keys1={list(js1.keys())}, Keys2={list(js2.keys())}")
