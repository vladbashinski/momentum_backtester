from __future__ import annotations
import requests

MOEX_ISS = "https://iss.moex.com/iss"


def load_imoex_universe() -> list[str]:
    """
    Returns list of tickers included in IMOEX index (via MOEX ISS).
    """
    url = f"{MOEX_ISS}/statistics/engines/stock/markets/index/analytics/IMOEX.json"
    params = {"iss.meta": "off", "iss.only": "analytics"}

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()

    analytics = js.get("analytics", {})
    cols = analytics.get("columns", [])
    data = analytics.get("data", [])

    if not cols or not data or "SECID" not in cols:
        raise ValueError("Unexpected MOEX ISS response format for IMOEX universe")

    secid_idx = cols.index("SECID")
    tickers = sorted({row[secid_idx] for row in data if row and len(row) > secid_idx and row[secid_idx]})
    return tickers
