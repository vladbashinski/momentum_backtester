from __future__ import annotations

import pandas as pd
import matplotlib.pyplot as plt


def plot_equity(
    strategy_equity: pd.Series,
    bh_equity: pd.Series | None = None,
    title: str = "Equity curve",
):
    """
    Plot equity curves. IMPORTANT: does NOT call plt.show().
    Caller is responsible for plt.show() once (e.g., in main()).
    """
    plt.figure()
    strategy_equity.dropna().plot(label="Strategy")
    if bh_equity is not None:
        bh_equity.dropna().plot(label="Buy&Hold (EW)")
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Equity (growth of $1)")
    plt.legend()
    plt.tight_layout()


def plot_drawdown(equity: pd.Series, title: str = "Drawdown"):
    """
    Plot drawdown series. IMPORTANT: does NOT call plt.show().
    """
    peak = equity.cummax()
    dd = equity / peak - 1.0
    plt.figure()
    dd.dropna().plot()
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Drawdown")
    plt.tight_layout()


def plot_turnover(turnover: pd.Series, title: str = "Turnover"):
    """
    Plot turnover series. IMPORTANT: does NOT call plt.show().
    """
    plt.figure()
    turnover.dropna().plot()
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Turnover (sum abs weight changes)")
    plt.tight_layout()
