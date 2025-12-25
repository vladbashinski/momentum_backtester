from datetime import datetime, timezone
import streamlit as st
import matplotlib.pyplot as plt

from momentum_bt.data.binance import build_close_series
from momentum_bt.data.moex import build_close_series as build_moex_close_series
from momentum_bt.backtest import BacktestParams, run_momentum_backtest
from momentum_bt.metrics import summary_stats
from momentum_bt.plots import plot_equity, plot_drawdown, plot_turnover

st.set_page_config(page_title="Momentum Backtester", layout="wide")
st.title("Momentum Backtester (Crypto / MOEX)")

with st.sidebar:
    st.header("Market & Data")

    market = st.selectbox("Market", ["Crypto (Binance)", "MOEX"])

    if market == "Crypto (Binance)":
        symbols = st.multiselect(
            "Crypto symbols",
            ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
            default=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
        )
    else:
        tickers_str = st.text_input(
            "MOEX tickers (TQBR, comma-separated)",
            value="SBER,GAZP,LKOH,GMKN,ROSN,NVTK",
        )
        tickers = [x.strip().upper() for x in tickers_str.split(",") if x.strip()]

    start = st.date_input("Start date", value=datetime(2022, 1, 1).date())
    end = st.date_input("End date", value=datetime(2024, 12, 31).date())

    st.header("Strategy parameters")
    lookback = st.number_input("Lookback (days)", 5, 365, 60, 5)
    rebalance_days = st.number_input("Rebalance (days)", 1, 90, 21, 1)
    top_n = st.number_input("Top N", 1, 10, 2, 1)
    bottom_n = st.number_input("Bottom N", 0, 10, 2, 1)
    tc = st.number_input("Transaction cost", 0.0, 0.01, 0.0005, 0.0001, format="%.4f")
    gross = st.number_input("Gross exposure", 0.5, 5.0, 2.0, 0.1)

    run_btn = st.button("Run backtest", type="primary")

if run_btn:
    start_dt = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_dt = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)

    with st.spinner("Downloading data & running backtest..."):
        if market == "Crypto (Binance)":
            prices = build_close_series(
                symbols=symbols,
                interval="1d",
                start=start_dt,
                end=end_dt,
            )
            periods_per_year = 365
        else:
            prices = build_moex_close_series(
                tickers=tickers,
                start=start_dt,
                end=end_dt,
            )
            periods_per_year = 252

        params = BacktestParams(
            lookback=int(lookback),
            rebalance_days=int(rebalance_days),
            top_n=int(top_n),
            bottom_n=int(bottom_n),
            transaction_cost=float(tc),
            gross_exposure=float(gross),
        )

        res = run_momentum_backtest(prices, params)
        stats = summary_stats(res["net_ret"], res["equity"], periods_per_year)

    c1, c2 = st.columns([1, 2], vertical_alignment="top")

    with c1:
        st.subheader("Summary")
        st.dataframe({k: [float(v)] for k, v in stats.items()}, use_container_width=True)

    with c2:
        st.subheader("Plots")

        plot_equity(res["equity"], res.get("bh_equity"), "Equity curve")
        st.pyplot(plt.gcf(), clear_figure=True)

        plot_drawdown(res["equity"], "Drawdown")
        st.pyplot(plt.gcf(), clear_figure=True)

        plot_turnover(res["turnover"], "Turnover")
        st.pyplot(plt.gcf(), clear_figure=True)

    st.success("Done.")
else:
    st.info("Set parameters in the sidebar and click **Run backtest**.")
