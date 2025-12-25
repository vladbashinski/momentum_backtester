from __future__ import annotations
import requests

MOEX_ISS = "https://iss.moex.com/iss"


def _get_table(js: dict) -> tuple[list[str], list[list]]:
    """
    Return (columns, data) from either 'analytics' or 'analytics_allowable'.
    """
    for key in ("analytics", "analytics_allowable"):
        tbl = js.get(key)
        if tbl and "columns" in tbl and "data" in tbl:
            return tbl["columns"], tbl["data"]
    return [], []


def _find_secid_index(columns: list[str]) -> int | None:
    cols_u = [str(c).strip().upper() for c in columns]
    if "SECID" in cols_u:
        return cols_u.index("SECID")
    # fallback: column contains SECID
    for i, c in enumerate(cols_u):
        if "SECID" in c:
            return i
    return None


def load_imoex_universe() -> list[str]:
    """
    Load IMOEX constituents via MOEX ISS analytics endpoint WITH pagination.
    MOEX ISS often returns only 20 rows per page by default -> we must iterate start=0,20,40...
    """
    url = f"{MOEX_ISS}/statistics/engines/stock/markets/index/analytics/IMOEX.json"

    start = 0
    tickers: set[str] = set()

    while True:
        params = {
            "iss.meta": "off",
            "iss.only": "analytics,analytics_allowable",
            "start": start,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()

        columns, data = _get_table(js)
        if not columns or not data:
            break

        secid_idx = _find_secid_index(columns)
        if secid_idx is None:
            raise ValueError(f"SECID not found in columns: {columns}")

        before = len(tickers)
        for row in data:
            if row and len(row) > secid_idx and row[secid_idx]:
                tickers.add(str(row[secid_idx]).strip().upper())

        # Pagination: MOEX returns fixed page size (often 20). Move by number of rows received.
        got = len(data)
        start += got

        # stop condition: no new tickers or last page
        if got == 0 or len(tickers) == before:
            break

        # safety guard (should never hit)
        if start > 2000:
            break

    tickers = sorted(tickers)

    # Filter out the index itself if it appears (sometimes IMOEX can appear as SECID)
    tickers = [t for t in tickers if t != "IMOEX"]

    if not tickers:
        raise ValueError("IMOEX universe loaded but empty after pagination")

    return tickers
