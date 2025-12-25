from __future__ import annotations
import requests

MOEX_ISS = "https://iss.moex.com/iss"


def load_imoex_universe() -> list[str]:
    """
    Load IMOEX constituents via MOEX ISS.
    Robust to ISS schema variations.
    """
    url = f"{MOEX_ISS}/statistics/engines/stock/markets/index/analytics/IMOEX.json"
    params = {"iss.meta": "off"}

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()

    # MOEX ISS sometimes changes keys
    block = None
    for key in ("analytics", "analytics_allowable", "data"):
        if key in js:
            block = js[key]
            break

    if not block or "columns" not in block or "data" not in block:
        raise ValueError(f"Unexpected MOEX ISS response keys: {list(js.keys())}")

    cols = block["columns"]
    data = block["data"]

    if "SECID" not in cols:
        raise ValueError("SECID not found in MOEX ISS columns")

    secid_idx = cols.index("SECID")

    tickers = sorted({
        row[secid_idx]
        for row in data
        if row and len(row) > secid_idx and row[secid_idx]
    })

    if not tickers:
        raise ValueError("IMOEX universe loaded but empty")

    return tickers
