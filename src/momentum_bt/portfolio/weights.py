from __future__ import annotations

import numpy as np
import pandas as pd


def build_long_short_weights(
    scores: pd.Series,
    top_n: int,
    bottom_n: int,
    gross_exposure: float = 2.0,
) -> pd.Series:
    """
    Build market-neutral long/short weights from a cross-section of scores.

    - Long top_n assets equally
    - Short bottom_n assets equally
    - Normalizes weights so that sum(abs(w)) == gross_exposure
      (gross_exposure=2 -> classic 100% long + 100% short)

    scores: Series indexed by asset, higher = stronger momentum
    """
    s = scores.dropna()
    if len(s) == 0:
        return pd.Series(dtype=float)

    if top_n <= 0 or bottom_n <= 0:
        raise ValueError("top_n and bottom_n must be positive")

    # If not enough assets, shrink counts
    top_n = min(top_n, len(s))
    bottom_n = min(bottom_n, len(s) - top_n) if len(s) > top_n else 0

    longs = s.nlargest(top_n).index
    shorts = s.nsmallest(bottom_n).index if bottom_n > 0 else []

    w = pd.Series(0.0, index=s.index)

    if top_n > 0:
        w.loc[longs] = 1.0 / top_n
    if bottom_n > 0:
        w.loc[shorts] = -1.0 / bottom_n

    # Normalize to desired gross exposure
    gross = float(np.abs(w).sum())
    if gross > 0:
        w = w * (gross_exposure / gross)

    return w
