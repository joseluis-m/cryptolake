"""
Dashboard interactivo de CryptoLake — Streamlit.

Consume la API REST de FastAPI y muestra visualizaciones del mercado crypto.

Ejecución local:
    streamlit run src/serving/dashboard/app.py

En Docker: se levanta automáticamente en http://localhost:8501
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ── Configuración ───────────────────────────────────────────
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="CryptoLake Dashboard",
    page_icon="🏔️",
    layout="wide",
)


# ── Helpers ─────────────────────────────────────────────────
def api_get(endpoint: str):
    """Llama a la API y devuelve JSON o None si falla."""
    try:
        resp = requests.get(f"{API_URL}{endpoint}", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"Error calling API: {e}")
        return None


# ── Header ──────────────────────────────────────────────────
st.title("🏔️ CryptoLake — Crypto Analytics Dashboard")
st.caption("Powered by Apache Iceberg + Spark + dbt + FastAPI")

# ── Health Check ────────────────────────────────────────────
health = api_get("/api/v1/health")
if not health or health.get("status") != "healthy":
    st.warning("⚠️ API not available. Make sure the pipeline has run.")
    st.stop()

# ── Market Overview ─────────────────────────────────────────
st.header("📊 Market Overview")
overview = api_get("/api/v1/analytics/market-overview")

if overview:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Coins Tracked", overview.get("total_coins", 0))
    col2.metric("Fact Rows", f"{overview.get('total_fact_rows', 0):,}")
    col3.metric(
        "Fear & Greed",
        overview.get("latest_fear_greed", "—"),
        overview.get("latest_sentiment", ""),
    )
    col4.metric(
        "Date Range",
        f"{overview.get('date_range_start', '?')} → {overview.get('date_range_end', '?')}",
    )

# ── Coin Selector ───────────────────────────────────────────
st.header("📈 Price Analysis")
coins = api_get("/api/v1/analytics/coins")

if coins:
    coin_ids = [c["coin_id"] for c in coins]
    selected = st.selectbox("Select cryptocurrency:", coin_ids)

    # ── Price Chart ─────────────────────────────────────────
    prices = api_get(f"/api/v1/prices/{selected}?limit=365")

    if prices:
        df = pd.DataFrame(prices)
        df["price_date"] = pd.to_datetime(df["price_date"])
        df = df.sort_values("price_date")

        # Precio + Moving Averages
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df["price_date"],
                y=df["price_usd"],
                name="Price",
                line=dict(color="#4A90D9", width=2),
            )
        )
        if "moving_avg_7d" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["price_date"],
                    y=df["moving_avg_7d"],
                    name="MA 7d",
                    line=dict(color="#F5A623", dash="dash"),
                )
            )
        if "moving_avg_30d" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["price_date"],
                    y=df["moving_avg_30d"],
                    name="MA 30d",
                    line=dict(color="#7B68EE", dash="dot"),
                )
            )
        fig.update_layout(
            title=f"{selected.title()} — Price & Moving Averages",
            xaxis_title="Date",
            yaxis_title="Price (USD)",
            template="plotly_dark",
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Tabla de coins
        st.subheader("🪙 Coin Stats")
        coins_df = pd.DataFrame(coins)
        st.dataframe(coins_df, use_container_width=True)

# ── Fear & Greed ────────────────────────────────────────────
st.header("😱 Fear & Greed Index")
fg = api_get("/api/v1/analytics/fear-greed?limit=60")

if fg:
    fg_df = pd.DataFrame(fg)
    fg_df["index_date"] = pd.to_datetime(fg_df["index_date"])
    fg_df = fg_df.sort_values("index_date")

    color_map = {
        "Extreme Fear": "#DC3545",
        "Fear": "#FD7E14",
        "Neutral": "#FFC107",
        "Greed": "#28A745",
        "Extreme Greed": "#20C997",
    }

    fig2 = px.bar(
        fg_df,
        x="index_date",
        y="fear_greed_value",
        color="classification",
        color_discrete_map=color_map,
        title="Fear & Greed Index (last 60 days)",
        template="plotly_dark",
        height=350,
    )
    fig2.add_hline(y=50, line_dash="dash", line_color="gray")
    st.plotly_chart(fig2, use_container_width=True)

# ── Footer ──────────────────────────────────────────────────
st.divider()
st.caption(
    "CryptoLake — Data Engineering Portfolio Project | "
    "Apache Iceberg • Spark • dbt • Airflow • FastAPI • Streamlit"
)
