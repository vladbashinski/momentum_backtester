from __future__ import annotations

import numpy as np
import pandas as pd


def max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def sharpe_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    r = returns.dropna()
    if r.std() == 0 or len(r) < 2:
        return float("nan")
    return float((r.mean() / r.std()) * np.sqrt(periods_per_year))


def cagr(equity: pd.Series, periods_per_year: int = 252) -> float:
    e = equity.dropna()
    if len(e) < 2:
        return float("nan")
    total_return = e.iloc[-1] / e.iloc[0]
    years = (len(e) - 1) / periods_per_year
    if years <= 0:
        return float("nan")
    return float(total_return ** (1 / years) - 1)


def summary_stats(net_ret: pd.Series, equity: pd.Series, periods_per_year: int = 252) -> dict:
    return {
        "CAGR": cagr(equity, periods_per_year),
        "Sharpe": sharpe_ratio(net_ret, periods_per_year),
        "MaxDD": max_drawdown(equity),
        "Vol(ann.)": float(net_ret.dropna().std() * np.sqrt(periods_per_year)),
        "Mean(ann.)": float(net_ret.dropna().mean() * periods_per_year),
    }
