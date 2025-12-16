import pandas as pd


def compute_momentum(prices: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """
    Momentum score = cumulative return over lookback window (close-to-close).
    Uses pct_change(lookback), which is equivalent to price[t]/price[t-lookback]-1.

    prices: wide DF (index datetime, columns assets)
    """
    if lookback <= 0:
        raise ValueError("lookback must be positive")

    return prices.pct_change(lookback)
