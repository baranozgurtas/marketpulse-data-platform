"""
MarketPulse — Analytics Dashboard
Connects to Snowflake CURATED layer and visualizes:
- Volatility trends per asset
- Market regime timeline
- Fear & Greed correlation
- Asset comparison
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector

# -----------------------------------------------
# Config
# -----------------------------------------------
st.set_page_config(
    page_title="MarketPulse Dashboard",
    page_icon="📊",
    layout="wide",
)

SNOWFLAKE_CONFIG = {
    "account": st.secrets["snowflake_account"],
    "user": st.secrets["snowflake_user"],
    "password": st.secrets["snowflake_password"],
    "database": "MARKETPULSE",
    "warehouse": "COMPUTE_WH",
}


@st.cache_data(ttl=3600)
def load_data():
    """Load mart_volatility data from Snowflake."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    query = """
        SELECT * FROM MARKETPULSE.CURATED.mart_volatility
        ORDER BY date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = df.columns.str.lower()
    df["date"] = pd.to_datetime(df["date"])
    return df


# For demo without Snowflake — generate sample data
@st.cache_data
def load_demo_data():
    """Generate realistic demo data for showcase."""
    import numpy as np

    np.random.seed(42)
    dates = pd.date_range("2023-06-01", periods=365, freq="D")
    assets = [
        ("bitcoin", "BTC"), ("ethereum", "ETH"), ("solana", "SOL"),
    ]

    records = []
    for asset_id, symbol in assets:
        base_price = {"BTC": 40000, "ETH": 2200, "SOL": 80}[symbol]
        price = base_price
        for date in dates:
            ret = np.random.normal(0.001, 0.03)
            price *= (1 + ret)
            vol_7d = abs(np.random.normal(0.03, 0.01))
            vol_30d = abs(np.random.normal(0.04, 0.015))
            regime = (
                "LOW" if vol_30d < 0.02 else
                "NORMAL" if vol_30d < 0.05 else
                "HIGH" if vol_30d < 0.08 else
                "EXTREME"
            )
            records.append({
                "asset_id": asset_id,
                "symbol": symbol,
                "name": asset_id.title(),
                "date": date,
                "close": round(price, 2),
                "return_1d": round(ret, 6),
                "fear_greed": np.random.randint(15, 85),
                "volatility_7d": round(vol_7d, 6),
                "volatility_30d": round(vol_30d, 6),
                "max_drawdown_30d": round(-abs(np.random.normal(0.1, 0.05)), 4),
                "volatility_regime": regime,
            })

    return pd.DataFrame(records)


# -----------------------------------------------
# Main App
# -----------------------------------------------
st.title("📊 MarketPulse Data Platform")
st.caption("Crypto Market Intelligence — Volatility Analytics Dashboard")

# Try Snowflake, fall back to demo
try:
    df = load_data()
    st.success("Connected to Snowflake")
except Exception:
    df = load_demo_data()
    st.info("Running with demo data — connect Snowflake for live data")

# Sidebar filters
st.sidebar.header("Filters")
assets = sorted(df["symbol"].unique())
selected_assets = st.sidebar.multiselect("Assets", assets, default=assets[:3])

date_range = st.sidebar.date_input(
    "Date Range",
    value=(df["date"].min().date(), df["date"].max().date()),
)

# Filter data
mask = (
    df["symbol"].isin(selected_assets)
    & (df["date"] >= pd.Timestamp(date_range[0]))
    & (df["date"] <= pd.Timestamp(date_range[1]))
)
filtered = df[mask].copy()

# -----------------------------------------------
# Row 1: Key Metrics
# -----------------------------------------------
col1, col2, col3, col4 = st.columns(4)

latest = filtered.sort_values("date").groupby("symbol").last().reset_index()
if not latest.empty:
    top = latest.iloc[0]
    col1.metric("Top Asset", top["symbol"], f"{top['return_1d']:.2%}")
    col2.metric("Avg Volatility (30d)", f"{latest['volatility_30d'].mean():.4f}")
    col3.metric("Fear & Greed", f"{latest['fear_greed'].mean():.0f}")
    extreme_count = (latest["volatility_regime"] == "EXTREME").sum()
    col4.metric("Extreme Vol Assets", extreme_count)

# -----------------------------------------------
# Row 2: Price + Volatility Chart
# -----------------------------------------------
st.subheader("Price & Volatility Trends")

fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    row_heights=[0.6, 0.4],
    subplot_titles=("Price (USD)", "30-Day Rolling Volatility"),
)

for symbol in selected_assets:
    asset_data = filtered[filtered["symbol"] == symbol]
    fig.add_trace(
        go.Scatter(x=asset_data["date"], y=asset_data["close"], name=f"{symbol} Price"),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=asset_data["date"], y=asset_data["volatility_30d"], name=f"{symbol} Vol"),
        row=2, col=1,
    )

fig.update_layout(height=600, showlegend=True)
st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------
# Row 3: Volatility Regime Timeline
# -----------------------------------------------
st.subheader("Volatility Regime Timeline")

regime_colors = {"LOW": "#22c55e", "NORMAL": "#3b82f6", "HIGH": "#f59e0b", "EXTREME": "#ef4444"}
regime_fig = px.scatter(
    filtered, x="date", y="symbol", color="volatility_regime",
    color_discrete_map=regime_colors,
    title="Daily Volatility Regime per Asset",
)
regime_fig.update_traces(marker=dict(size=4, symbol="square"))
regime_fig.update_layout(height=300)
st.plotly_chart(regime_fig, use_container_width=True)

# -----------------------------------------------
# Row 4: Fear & Greed vs Returns
# -----------------------------------------------
st.subheader("Fear & Greed Index vs Daily Returns")

scatter_fig = px.scatter(
    filtered, x="fear_greed", y="return_1d", color="symbol",
    opacity=0.5,
    labels={"fear_greed": "Fear & Greed Index", "return_1d": "Daily Return"},
)
scatter_fig.update_layout(height=400)
st.plotly_chart(scatter_fig, use_container_width=True)

# -----------------------------------------------
# Row 5: Max Drawdown Comparison
# -----------------------------------------------
st.subheader("30-Day Max Drawdown")

drawdown_data = latest[["symbol", "max_drawdown_30d"]].copy()
drawdown_fig = px.bar(
    drawdown_data, x="symbol", y="max_drawdown_30d",
    color="max_drawdown_30d",
    color_continuous_scale="RdYlGn",
)
drawdown_fig.update_layout(height=350)
st.plotly_chart(drawdown_fig, use_container_width=True)

# -----------------------------------------------
# Data Table
# -----------------------------------------------
with st.expander("Raw Data"):
    st.dataframe(
        filtered[["date", "symbol", "close", "return_1d", "volatility_7d",
                  "volatility_30d", "max_drawdown_30d", "volatility_regime", "fear_greed"]]
        .sort_values(["date", "symbol"], ascending=[False, True]),
        use_container_width=True,
    )