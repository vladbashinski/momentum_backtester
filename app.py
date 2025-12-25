from datetime import datetime, timezone
import streamlit as st
import matplotlib.pyplot as plt

from momentum_bt.data.binance import build_close_series as build_crypto_close_series
from momentum_bt.data.moex import build_close_series as build_moex_close_series
from momentum_bt.data.moex_universe import load_imoex_universe

from momentum_bt.backtest import BacktestParams, run_momentum_backtest
from momentum_bt.metrics import summary_stats
from momentum_bt.plots import plot_equity, plot_drawdown, plot_turnover


st.set_page_config(page_title="Momentum Backtester", layout="wide")
st.title("Momentum Backtester (Crypto / MOEX)")


@st.cache_data(ttl=6 * 60 * 60)  # 6 hours
def cached_imoex_universe() -> list[str]:
    return load_imoex_universe()


@st.cache_data(ttl=60 * 60)  # 1 hour
def cached_crypto_prices(symbols: tuple[str, ...], start_dt: datetime, end_dt: datetime):
    return build_crypto_close_series(
        symbols=list(symbols),
        interval="1d",
        start=start_dt,
        end=end_dt,
    )


@st.cache_data(ttl=60 * 60)  # 1 hour
def cached_moex_prices(tickers: tuple[str, ...], start_dt: datetime, end_dt: datetime, board: str):
    return build_moex_close_series(
        tickers=list(tickers),
        start=start_dt,
        end=end_dt,
        board=board,
    )


with st.sidebar:
    st.header("Market & Data")

    market = st.selectbox("Market", ["Crypto (Binance)", "MOEX"])

    start = st.date_input("Start date", value=datetime(2022, 1, 1).date())
    end = st.date_input("End date", value=datetime(2024, 12, 31).date())

    if market == "Crypto (Binance)":
        symbols = st.multiselect(
            "Crypto symbols",
            ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
            default=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
        )
        tickers = []
        board = "TQBR"
    else:
        st.subheader("MOEX universe")
        universe_mode = st.radio(
            "Universe",
            ["IMOEX (index constituents)", "Custom tickers"],
            index=0,
        )

        board = st.text_input("Board", value="TQBR", help="Most liquid shares board is usually TQBR.")

        if universe_mode == "IMOEX (index constituents)":
            try:
                tickers = cached_imoex_universe()
                st.caption(f"Loaded {len(tickers)} tickers from IMOEX.")
                with st.expander("Show tickers"):
                    st.write(", ".join(tickers))
            except Exception as e:
                tickers = []
                st.error(f"Failed to load IMOEX universe: {e}")
        else:
            tickers_str = st.text_input(
                "MOEX tickers (comma-separated)",
                value="SBER,GAZP,LKOH,GMKN,ROSN,NVTK",
            )
            tickers = [x.strip().upper() for x in tickers_str.split(",") if x.strip()]

        symbols = []

    st.header("Strategy parameters")
    lookback = st.number_input("Lookback (days)", min_value=5, max_value=365, value=60, step=5)
    rebalance_days = st.number_input("Rebalance (days)", min_value=1, max_value=90, value=21, step=1)
    top_n = st.number_input("Top N", min_value=1, max_value=50, value=10, step=1)
    bottom_n = st.number_input("Bottom N", min_value=0, max_value=50, value=10, step=1)
    tc = st.number_input("Transaction cost", min_value=0.0, max_value=0.01, value=0.0005, step=0.0001, format="%.4f")
    gross = st.number_input("Gross exposure", min_value=0.5, max_value=5.0, value=2.0, step=0.1)

    run_btn = st.button("Run backtest", type="primary")


def _to_utc_dt(d) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


if run_btn:
    start_dt = _to_utc_dt(start)
    end_dt = _to_utc_dt(end)

    if end_dt <= start_dt:
        st.error("End date must be after start date.")
        st.stop()

    with st.spinner("Downloading data & running backtest..."):
        if market == "Crypto (Binance)":
            if not symbols:
                st.error("Choose at least one crypto symbol.")
                st.stop()

            prices = cached_crypto_prices(tuple(symbols), start_dt, end_dt)
            periods_per_year = 365

        else:
            if not tickers:
                st.error("MOEX ticker list is empty. Choose IMOEX universe or provide tickers.")
                st.stop()

            prices = cached_moex_prices(tuple(tickers), start_dt, end_dt, board.strip().upper())
            periods_per_year = 252

        if prices is None or prices.empty:
            st.error("No price data returned. Check tickers/symbols and date range.")
            st.stop()

        prices = prices.dropna(axis=1, how="all")

        if prices.shape[1] < max(2, int(top_n) + int(bottom_n)):
            st.error(
                f"Not enough instruments with data: got {prices.shape[1]} columns, "
                f"but need at least {int(top_n) + int(bottom_n)}."
            )
            st.stop()

        params = BacktestParams(
            lookback=int(lookback),
            rebalance_days=int(rebalance_days),
            top_n=int(top_n),
            bottom_n=int(bottom_n),
            transaction_cost=float(tc),
            gross_exposure=float(gross),
        )

        res = run_momentum_backtest(prices, params)
        stats = summary_stats(res["net_ret"], res["equity"], periods_per_year=periods_per_year)

    c1, c2 = st.columns([1, 2], vertical_alignment="top")

    with c1:
        st.subheader("Summary")
        st.dataframe({k: [float(v)] for k, v in stats.items()}, use_container_width=True)

        st.subheader("Data info")
        st.write(f"Rows (dates): {prices.shape[0]}")
        st.write(f"Columns (instruments): {prices.shape[1]}")
        st.write(f"Periods/year: {periods_per_year}")

    with c2:
        st.subheader("Plots")

        plot_equity(res["equity"], res.get("bh_equity"), title="Equity: strategy vs buy&hold")
        st.pyplot(plt.gcf(), clear_figure=True)

        plot_drawdown(res["equity"], title="Drawdown")
        st.pyplot(plt.gcf(), clear_figure=True)

        plot_turnover(res["turnover"], title="Turnover")
        st.pyplot(plt.gcf(), clear_figure=True)

    st.success("Done.")
else:
    st.info("Set parameters in the sidebar and click **Run backtest**.")
