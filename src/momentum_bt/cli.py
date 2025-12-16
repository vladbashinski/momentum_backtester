from datetime import datetime, timezone

from momentum_bt.data.binance import build_close_series
from momentum_bt.backtest import BacktestParams, run_momentum_backtest
from momentum_bt.metrics import summary_stats
from momentum_bt.plots import plot_equity, plot_drawdown, plot_turnover

import matplotlib.pyplot as plt


def main():
    # 2022–2024 как вы и хотели (можно сузить для теста)
    start = datetime(2022, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    prices = build_close_series(
        symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
        interval="1d",
        start=start,
        end=end,
    )

    params = BacktestParams(
        lookback=60,
        rebalance_days=21,
        top_n=2,
        bottom_n=2,
        transaction_cost=0.0005,
        gross_exposure=2.0,
    )

    res = run_momentum_backtest(prices, params)
    stats = summary_stats(res["net_ret"], res["equity"], periods_per_year=365)  # крипта торгуется каждый день

    print("=== Summary (Crypto) ===")
    for k, v in stats.items():
        print(f"{k:10s}: {v:.4f}")

    # Графики — строго ПОСЛЕ печати статистики (и в нужной очередности)
    plot_equity(res["equity"], res["bh_equity"], title="Crypto momentum vs EW buy&hold")
    plot_drawdown(res["equity"], title="Strategy drawdown (crypto)")
    plot_turnover(res["turnover"], title="Turnover (crypto)")

    plt.show()


if __name__ == "__main__":
    main()
