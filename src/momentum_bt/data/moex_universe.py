from __future__ import annotations
import requests

MOEX_ISS = "https://iss.moex.com/iss"


def _fetch_json(url: str) -> dict:
    r = requests.get(url, params={"iss.meta": "off"}, timeout=30)
    r.raise_for_status()
    return r.json()


def _extract_tickers_from_table(js: dict, table_key: str) -> list[str]:
    """
    Extract tickers from ISS table with unknown SECID column case/name.
    """
    tbl = js.get(table_key)
    if not tbl or "columns" not in tbl or "data" not in tbl:
        return []

    cols = tbl["columns"]
    data = tbl["data"]

    # Find SECID column robustly (SECID / secid / SecId / etc.)
    secid_idx = None
    for i, c in enumerate(cols):
        if str(c).strip().upper() == "SECID":
            secid_idx = i
            break

    # Extra fallback: sometimes it's something like "SECID" absent but "SECID" part exists
    if secid_idx is None:
        for i, c in enumerate(cols):
            if "SECID" in str(c).strip().upper():
                secid_idx = i
                break

    if secid_idx is None:
        return []

    tickers = sorted({
        row[secid_idx]
        for row in data
        if row and len(row) > secid_idx and row[secid_idx]
    })
    return tickers


def load_imoex_universe() -> list[str]:
    """
    Load IMOEX constituents via MOEX ISS.
    Robust to schema variations (SECID casing & different table keys).
    """
    # Primary endpoint used commonly for IMOEX analytics
    url = f"{MOEX_ISS}/statistics/engines/stock/markets/index/analytics/IMOEX.json"
    js = _fetch_json(url)

    # Try likely tables
    for key in ("analytics", "analytics_allowable"):
        tickers = _extract_tickers_from_table(js, key)
        if tickers:
            return tickers

    # If we got here: endpoint exists but SECID column name differs OR table differs
    # Provide diagnostic hint
    keys = list(js.keys())
    raise ValueError(
        f"Could not find SECID column in expected tables. "
        f"Top-level keys: {keys}. "
        f"Tables tried: analytics, analytics_allowable."
    )
