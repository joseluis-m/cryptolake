"""
CryptoLake Analytics Dashboard.

Consumes the FastAPI REST API and renders interactive market visualizations.

Local: streamlit run src/serving/dashboard/app.py
Docker: automatically served at http://localhost:8501
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# -- Configuration ------------------------------------------

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="CryptoLake Dashboard",
    page_icon=":mountain:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -- Custom CSS ---------------------------------------------

st.markdown(
    """
    <style>
    /* KPI card styling */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a4a;
        border-radius: 10px;
        padding: 16px 20px;
    }
    div[data-testid="stMetric"] label {
        color: #8892b0;
        font-size: 0.85rem;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #ccd6f6;
        font-size: 1.6rem;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px 6px 0 0;
        padding: 8px 20px;
    }

    /* Table header */
    thead tr th {
        background-color: #1a1a2e !important;
        color: #8892b0 !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0a0a1a 0%, #1a1a2e 100%);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -- Plotly theme -------------------------------------------

CHART_TEMPLATE = "plotly_dark"
COLOR_PRIMARY = "#4A90D9"
COLOR_SECONDARY = "#F5A623"
COLOR_TERTIARY = "#7B68EE"
COLOR_POSITIVE = "#00C853"
COLOR_NEGATIVE = "#FF5252"
COLOR_MUTED = "#546E7A"

SENTIMENT_COLORS = {
    "Extreme Fear": "#DC3545",
    "Fear": "#FD7E14",
    "Neutral": "#FFC107",
    "Greed": "#28A745",
    "Extreme Greed": "#20C997",
}


# -- API helpers --------------------------------------------


def api_get(endpoint: str, params: dict | None = None):
    """Call the API and return JSON or None on failure."""
    try:
        resp = requests.get(
            f"{API_URL}{endpoint}",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        st.error(f"API error: {e}")
        return None


@st.cache_data(ttl=300)
def fetch_market_overview():
    return api_get("/api/v1/analytics/market-overview")


@st.cache_data(ttl=300)
def fetch_coins():
    return api_get("/api/v1/analytics/coins")


@st.cache_data(ttl=300)
def fetch_prices(coin_id: str, limit: int = 365):
    return api_get(f"/api/v1/prices/{coin_id}", {"limit": limit})


@st.cache_data(ttl=300)
def fetch_fear_greed(limit: int = 90):
    return api_get("/api/v1/analytics/fear-greed", {"limit": limit})


# -- Sidebar ------------------------------------------------

with st.sidebar:
    st.title("CryptoLake")
    st.caption("Lakehouse Analytics Platform")
    st.divider()

    health = api_get("/api/v1/health")
    if health and health.get("status") == "healthy":
        st.success("API Connected")
    else:
        st.error("API Unreachable")
        st.info(
            "Ensure the pipeline has run:\n"
            "```\nmake pipeline\n```"
        )
        st.stop()

    st.divider()
    st.caption(
        "Stack: Apache Iceberg / Spark / dbt / "
        "Airflow / FastAPI / Streamlit"
    )

# -- Header -------------------------------------------------

st.title("CryptoLake -- Crypto Analytics Dashboard")

# -- Tab layout ---------------------------------------------

tab_overview, tab_prices, tab_comparison, tab_sentiment, tab_signals = (
    st.tabs([
        "Market Overview",
        "Price Analysis",
        "Coin Comparison",
        "Fear & Greed",
        "Trading Signals",
    ])
)

# ============================================================
# TAB 1: Market Overview
# ============================================================

with tab_overview:
    overview = fetch_market_overview()
    coins_data = fetch_coins()

    if overview:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Coins Tracked", overview.get("total_coins", 0))
        k2.metric("Fact Rows", f"{overview.get('total_fact_rows', 0):,}")

        fg_val = overview.get("latest_fear_greed")
        fg_label = overview.get("latest_sentiment", "N/A")
        k3.metric("Fear & Greed", f"{fg_val} -- {fg_label}" if fg_val else "N/A")

        date_start = overview.get("date_range_start", "?")
        date_end = overview.get("date_range_end", "?")
        k4.metric("Date Range", f"{date_start}  to  {date_end}")

    st.subheader("Coin Statistics")

    if coins_data:
        coins_df = pd.DataFrame(coins_data)

        display_cols = {
            "coin_id": "Coin",
            "all_time_high": "ATH (USD)",
            "all_time_low": "ATL (USD)",
            "avg_price": "Avg Price (USD)",
            "avg_daily_volume": "Avg Volume (USD)",
            "total_days_tracked": "Days Tracked",
            "price_range_pct": "Price Range %",
        }
        available = [c for c in display_cols if c in coins_df.columns]
        display_df = coins_df[available].rename(columns=display_cols)

        for col in display_df.columns:
            if col in ["ATH (USD)", "Avg Price (USD)", "Avg Volume (USD)"]:
                display_df[col] = display_df[col].apply(
                    lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A"
                )
            elif col == "ATL (USD)":
                display_df[col] = display_df[col].apply(
                    lambda x: f"${x:,.6f}" if pd.notna(x) else "N/A"
                )
            elif col == "Price Range %":
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:,.1f}%" if pd.notna(x) else "N/A"
                )

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Price range bar chart
        if "price_range_pct" in coins_df.columns:
            st.subheader("Historical Price Range by Coin")
            fig_range = px.bar(
                coins_df.sort_values("price_range_pct", ascending=True),
                x="price_range_pct",
                y="coin_id",
                orientation="h",
                template=CHART_TEMPLATE,
                color="price_range_pct",
                color_continuous_scale="Viridis",
                labels={
                    "price_range_pct": "Price Range %",
                    "coin_id": "",
                },
            )
            fig_range.update_layout(
                height=350,
                showlegend=False,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_range, use_container_width=True)

# ============================================================
# TAB 2: Price Analysis
# ============================================================

with tab_prices:
    coins_data = fetch_coins()
    if not coins_data:
        st.warning("No coin data available.")
        st.stop()

    coin_ids = [c["coin_id"] for c in coins_data]

    col_sel, col_days = st.columns([2, 1])
    with col_sel:
        selected_coin = st.selectbox(
            "Select cryptocurrency",
            coin_ids,
            key="price_coin",
        )
    with col_days:
        lookback = st.select_slider(
            "Lookback period",
            options=[30, 60, 90, 180, 365],
            value=90,
        )

    prices = fetch_prices(selected_coin, limit=lookback)

    if prices:
        df = pd.DataFrame(prices)
        df["price_date"] = pd.to_datetime(df["price_date"])
        df = df.sort_values("price_date")

        # KPI row
        latest = df.iloc[-1] if len(df) > 0 else None
        if latest is not None:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric(
                "Latest Price",
                f"${latest['price_usd']:,.2f}",
            )
            change = latest.get("price_change_pct_1d")
            m2.metric(
                "1d Change",
                f"{change:+.2f}%" if pd.notna(change) else "N/A",
            )
            vol = latest.get("volatility_7d")
            m3.metric(
                "7d Volatility",
                f"${vol:,.2f}" if pd.notna(vol) else "N/A",
            )
            signal = latest.get("ma30_signal", "N/A")
            m4.metric("MA30 Signal", signal)

        # Price + Moving Averages chart
        st.subheader(f"{selected_coin.title()} -- Price & Moving Averages")

        fig_price = go.Figure()
        fig_price.add_trace(
            go.Scatter(
                x=df["price_date"],
                y=df["price_usd"],
                name="Price",
                line=dict(color=COLOR_PRIMARY, width=2),
                hovertemplate="%{x|%Y-%m-%d}<br>$%{y:,.2f}<extra></extra>",
            )
        )
        if "moving_avg_7d" in df.columns:
            fig_price.add_trace(
                go.Scatter(
                    x=df["price_date"],
                    y=df["moving_avg_7d"],
                    name="MA 7d",
                    line=dict(color=COLOR_SECONDARY, dash="dash", width=1.5),
                )
            )
        if "moving_avg_30d" in df.columns:
            fig_price.add_trace(
                go.Scatter(
                    x=df["price_date"],
                    y=df["moving_avg_30d"],
                    name="MA 30d",
                    line=dict(color=COLOR_TERTIARY, dash="dot", width=1.5),
                )
            )
        fig_price.update_layout(
            template=CHART_TEMPLATE,
            height=420,
            xaxis_title="",
            yaxis_title="Price (USD)",
            legend=dict(orientation="h", y=1.12),
            margin=dict(t=40, b=40),
        )
        st.plotly_chart(fig_price, use_container_width=True)

        # Volume and Volatility side by side
        col_vol, col_vlty = st.columns(2)

        with col_vol:
            if "volume_24h_usd" in df.columns:
                st.subheader("Daily Trading Volume")
                fig_vol = px.bar(
                    df,
                    x="price_date",
                    y="volume_24h_usd",
                    template=CHART_TEMPLATE,
                    color_discrete_sequence=[COLOR_MUTED],
                    labels={"volume_24h_usd": "Volume (USD)", "price_date": ""},
                )
                fig_vol.update_layout(
                    height=300,
                    showlegend=False,
                    margin=dict(t=20, b=40),
                )
                st.plotly_chart(fig_vol, use_container_width=True)

        with col_vlty:
            if "volatility_7d" in df.columns:
                st.subheader("7-Day Rolling Volatility")
                fig_vlty = px.area(
                    df,
                    x="price_date",
                    y="volatility_7d",
                    template=CHART_TEMPLATE,
                    color_discrete_sequence=[COLOR_NEGATIVE],
                    labels={"volatility_7d": "Volatility (USD)", "price_date": ""},
                )
                fig_vlty.update_layout(
                    height=300,
                    showlegend=False,
                    margin=dict(t=20, b=40),
                )
                st.plotly_chart(fig_vlty, use_container_width=True)

        # Daily returns distribution
        if "price_change_pct_1d" in df.columns:
            st.subheader("Daily Returns Distribution")
            returns = df.dropna(subset=["price_change_pct_1d"])
            fig_hist = px.histogram(
                returns,
                x="price_change_pct_1d",
                nbins=30,
                template=CHART_TEMPLATE,
                color_discrete_sequence=[COLOR_PRIMARY],
                labels={"price_change_pct_1d": "Daily Return (%)"},
            )
            fig_hist.add_vline(x=0, line_dash="dash", line_color="white")
            fig_hist.update_layout(
                height=300,
                showlegend=False,
                margin=dict(t=20, b=40),
            )
            st.plotly_chart(fig_hist, use_container_width=True)

# ============================================================
# TAB 3: Coin Comparison
# ============================================================

with tab_comparison:
    coins_data = fetch_coins()
    if not coins_data:
        st.warning("No coin data available.")
    else:
        coin_ids = [c["coin_id"] for c in coins_data]

        selected_coins = st.multiselect(
            "Select coins to compare (2-5)",
            coin_ids,
            default=coin_ids[:3] if len(coin_ids) >= 3 else coin_ids,
            max_selections=5,
        )

        if len(selected_coins) >= 2:
            # Fetch all prices and normalize to 100
            st.subheader("Normalized Price Performance (base = 100)")

            frames = []
            for cid in selected_coins:
                p = fetch_prices(cid, limit=90)
                if p:
                    cdf = pd.DataFrame(p)
                    cdf["price_date"] = pd.to_datetime(cdf["price_date"])
                    cdf = cdf.sort_values("price_date")
                    if len(cdf) > 0:
                        base = cdf["price_usd"].iloc[0]
                        if base > 0:
                            cdf["normalized"] = (
                                cdf["price_usd"] / base * 100
                            )
                            cdf["coin"] = cid
                            frames.append(
                                cdf[["price_date", "normalized", "coin"]]
                            )

            if frames:
                combined = pd.concat(frames, ignore_index=True)
                fig_comp = px.line(
                    combined,
                    x="price_date",
                    y="normalized",
                    color="coin",
                    template=CHART_TEMPLATE,
                    labels={
                        "normalized": "Performance (base=100)",
                        "price_date": "",
                        "coin": "Coin",
                    },
                )
                fig_comp.add_hline(
                    y=100,
                    line_dash="dash",
                    line_color="gray",
                    opacity=0.5,
                )
                fig_comp.update_layout(
                    height=450,
                    legend=dict(orientation="h", y=1.12),
                    margin=dict(t=40, b=40),
                )
                st.plotly_chart(fig_comp, use_container_width=True)

            # Comparison table
            st.subheader("Side-by-Side Statistics")
            comp_rows = []
            for c in coins_data:
                if c["coin_id"] in selected_coins:
                    comp_rows.append(c)
            if comp_rows:
                comp_df = pd.DataFrame(comp_rows)
                st.dataframe(
                    comp_df,
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info("Select at least 2 coins to compare.")

# ============================================================
# TAB 4: Fear & Greed
# ============================================================

with tab_sentiment:
    col_fg_days, _ = st.columns([1, 3])
    with col_fg_days:
        fg_limit = st.select_slider(
            "History length",
            options=[30, 60, 90, 180, 365],
            value=90,
            key="fg_slider",
        )

    fg = fetch_fear_greed(limit=fg_limit)

    if fg:
        fg_df = pd.DataFrame(fg)
        fg_df["index_date"] = pd.to_datetime(fg_df["index_date"])
        fg_df = fg_df.sort_values("index_date")

        # Current value highlight
        latest_fg = fg_df.iloc[-1] if len(fg_df) > 0 else None
        if latest_fg is not None:
            fk1, fk2, fk3, fk4 = st.columns(4)
            fk1.metric("Current Value", int(latest_fg["fear_greed_value"]))
            fk2.metric("Classification", latest_fg["classification"])
            fk3.metric(
                "Period Average",
                f"{fg_df['fear_greed_value'].mean():.0f}",
            )
            fk4.metric(
                "Period Std Dev",
                f"{fg_df['fear_greed_value'].std():.1f}",
            )

        # Timeline bar chart
        st.subheader("Fear & Greed Timeline")
        fig_fg = px.bar(
            fg_df,
            x="index_date",
            y="fear_greed_value",
            color="classification",
            color_discrete_map=SENTIMENT_COLORS,
            template=CHART_TEMPLATE,
            labels={
                "fear_greed_value": "Index Value",
                "index_date": "",
                "classification": "Sentiment",
            },
        )
        fig_fg.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.5)
        fig_fg.add_hline(y=25, line_dash="dot", line_color=COLOR_NEGATIVE, opacity=0.3)
        fig_fg.add_hline(y=75, line_dash="dot", line_color=COLOR_POSITIVE, opacity=0.3)
        fig_fg.update_layout(
            height=380,
            legend=dict(orientation="h", y=1.12),
            margin=dict(t=40, b=40),
        )
        st.plotly_chart(fig_fg, use_container_width=True)

        # Distribution
        col_dist, col_stats = st.columns(2)

        with col_dist:
            st.subheader("Sentiment Distribution")
            dist_df = (
                fg_df["classification"]
                .value_counts()
                .reset_index()
            )
            dist_df.columns = ["classification", "days"]
            fig_pie = px.pie(
                dist_df,
                names="classification",
                values="days",
                color="classification",
                color_discrete_map=SENTIMENT_COLORS,
                template=CHART_TEMPLATE,
                hole=0.4,
            )
            fig_pie.update_layout(
                height=350,
                margin=dict(t=20, b=20),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_stats:
            st.subheader("Value Distribution")
            fig_fg_hist = px.histogram(
                fg_df,
                x="fear_greed_value",
                nbins=20,
                color="classification",
                color_discrete_map=SENTIMENT_COLORS,
                template=CHART_TEMPLATE,
                labels={"fear_greed_value": "Index Value"},
            )
            fig_fg_hist.update_layout(
                height=350,
                showlegend=False,
                margin=dict(t=20, b=20),
            )
            st.plotly_chart(fig_fg_hist, use_container_width=True)
    else:
        st.warning("Fear & Greed data not available.")

# ============================================================
# TAB 5: Trading Signals
# ============================================================

with tab_signals:
    st.subheader("Latest Trading Signals by Coin")
    st.caption(
        "Signals are derived from the MA30 trend indicator combined "
        "with market sentiment (Fear & Greed Index). "
        "This is for educational purposes only, not financial advice."
    )

    coins_data = fetch_coins()
    if coins_data:
        signal_rows = []
        for c in coins_data:
            cid = c["coin_id"]
            p = fetch_prices(cid, limit=7)
            if p and len(p) > 0:
                latest = p[0]  # API returns DESC
                signal_rows.append(
                    {
                        "Coin": cid,
                        "Price (USD)": latest.get("price_usd"),
                        "1d Change %": latest.get("price_change_pct_1d"),
                        "MA30 Signal": latest.get("ma30_signal", "N/A"),
                        "Sentiment": latest.get(
                            "market_sentiment", "N/A"
                        ),
                        "F&G Value": latest.get("fear_greed_value"),
                    }
                )

        if signal_rows:
            sig_df = pd.DataFrame(signal_rows)

            # Format columns
            sig_df["Price (USD)"] = sig_df["Price (USD)"].apply(
                lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A"
            )
            sig_df["1d Change %"] = sig_df["1d Change %"].apply(
                lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
            )

            st.dataframe(
                sig_df,
                use_container_width=True,
                hide_index=True,
            )

            # Signal distribution
            raw_signals = pd.DataFrame(signal_rows)
            if "MA30 Signal" in raw_signals.columns:
                st.subheader("Signal Distribution")
                sc1, sc2 = st.columns(2)

                with sc1:
                    ma30_dist = (
                        raw_signals["MA30 Signal"]
                        .value_counts()
                        .reset_index()
                    )
                    ma30_dist.columns = ["signal", "count"]
                    signal_colors = {
                        "ABOVE_MA30": COLOR_POSITIVE,
                        "BELOW_MA30": COLOR_NEGATIVE,
                        "AT_MA30": COLOR_MUTED,
                        "N/A": "#888888",
                    }
                    fig_sig = px.bar(
                        ma30_dist,
                        x="signal",
                        y="count",
                        color="signal",
                        color_discrete_map=signal_colors,
                        template=CHART_TEMPLATE,
                        title="MA30 Signal Breakdown",
                        labels={"signal": "", "count": "Coins"},
                    )
                    fig_sig.update_layout(
                        height=300,
                        showlegend=False,
                        margin=dict(t=40, b=40),
                    )
                    st.plotly_chart(fig_sig, use_container_width=True)

                with sc2:
                    sent_dist = (
                        raw_signals["Sentiment"]
                        .value_counts()
                        .reset_index()
                    )
                    sent_dist.columns = ["sentiment", "count"]
                    fig_sent = px.bar(
                        sent_dist,
                        x="sentiment",
                        y="count",
                        color="sentiment",
                        color_discrete_map=SENTIMENT_COLORS,
                        template=CHART_TEMPLATE,
                        title="Market Sentiment Breakdown",
                        labels={"sentiment": "", "count": "Coins"},
                    )
                    fig_sent.update_layout(
                        height=300,
                        showlegend=False,
                        margin=dict(t=40, b=40),
                    )
                    st.plotly_chart(fig_sent, use_container_width=True)

    else:
        st.warning("No coin data available.")

# -- Footer -------------------------------------------------

st.divider()
st.caption(
    "CryptoLake -- Data Engineering Portfolio Project  |  "
    "Apache Iceberg  /  Spark  /  dbt  /  Airflow  /  FastAPI  /  Streamlit"
)
