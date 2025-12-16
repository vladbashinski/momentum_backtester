from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd

from momentum_bt.features.momentum import compute_momentum
from momentum_bt.portfolio.weights import build_long_short_weights


@dataclass(frozen=True)
class BacktestParams:
    lookback: int = 20
    rebalance_days: int = 21
    top_n: int = 5
    bottom_n: int = 5
    transaction_cost: float = 0.0005  # 5 bps per 1.0 turnover
    gross_exposure: float = 2.0       # 2 = 100% long + 100% short


def run_momentum_backtest(prices: pd.DataFrame, params: BacktestParams) -> dict:
    """
    Академически корректный backtest:
    - signals computed on day t close
    - weights applied starting day t+1 (shift)
    - transaction costs from turnover of weights (sum(abs(w_t - w_{t-1})))
    """
    prices = prices.sort_index()
    returns = prices.pct_change()

    scores = compute_momentum(prices, params.lookback)

    # choose rebalance dates (every N trading days, after lookback)
    idx = prices.index
    start_i = params.lookback
    rebalance_idx = idx[start_i::params.rebalance_days]

    weights = pd.DataFrame(0.0, index=idx, columns=prices.columns)
    last_w = pd.Series(0.0, index=prices.columns)

    for t in idx:
        if t in rebalance_idx:
            w_t = build_long_short_weights(
                scores.loc[t],
                top_n=params.top_n,
                bottom_n=params.bottom_n,
                gross_exposure=params.gross_exposure,
            )
            # align to full universe columns
            last_w = pd.Series(0.0, index=prices.columns)
            last_w.loc[w_t.index] = w_t.values

        weights.loc[t] = last_w.values

    # Strategy return with execution lag (apply weights from t-1 to returns at t)
    gross_ret = (weights.shift(1) * returns).sum(axis=1)

    # Turnover & costs (weights-based)
    turnover = weights.diff().abs().sum(axis=1).fillna(0.0)
    costs = turnover * params.transaction_cost

    net_ret = gross_ret - costs
    equity = (1.0 + net_ret.fillna(0.0)).cumprod()

    # Baseline: equal-weight buy&hold (rebalanced daily)
    bh_ret = returns.mean(axis=1)
    bh_equity = (1.0 + bh_ret.fillna(0.0)).cumprod()

    return {
        "prices": prices,
        "returns": returns,
        "scores": scores,
        "weights": weights,
        "turnover": turnover,
        "costs": costs,
        "gross_ret": gross_ret,
        "net_ret": net_ret,
        "equity": equity,
        "rebalance_dates": rebalance_idx,
        "bh_ret": bh_ret,
        "bh_equity": bh_equity,

    }
