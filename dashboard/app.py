"""
dashboard/app.py — Connected Commerce Intelligence Dashboard

Three panels:
  1. Merchant Performance   — revenue KPIs, rolling trends, category breakdown
  2. Fraud Velocity Monitor — alert feed, signal heatmap, score distribution
  3. Pipeline Health        — batch stats, latency, quarantine rate, DLQ count

Run:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# Ensure repo root is importable even if Streamlit is launched from `dashboard/`.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

try:
    # When running from repo root (preferred).
    from dashboard.snowflake_connector import (
        get_merchant_performance,
        get_fraud_alerts,
        get_pipeline_health,
        get_customer_profiles,
        USE_SYNTHETIC,
    )
except ModuleNotFoundError:
    # When running with `cwd=dashboard/` or PYTHONPATH is missing repo root.
    from snowflake_connector import (  # type: ignore
        get_merchant_performance,
        get_fraud_alerts,
        get_pipeline_health,
        get_customer_profiles,
        USE_SYNTHETIC,
    )

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Connected Commerce Intelligence",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
        background-color: #0d0f14;
        color: #e2e8f0;
    }
    .main { background-color: #0d0f14; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: #161b27;
        border: 1px solid #1e2535;
        border-radius: 8px;
        padding: 16px 20px;
    }
    [data-testid="metric-container"] label {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.72rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.6rem;
        color: #f1f5f9;
    }

    /* Alert badge */
    .alert-high   { color: #f87171; font-weight: 600; }
    .alert-medium { color: #fb923c; font-weight: 600; }
    .alert-low    { color: #34d399; font-weight: 600; }

    /* Section headers */
    h2 { font-family: 'IBM Plex Mono', monospace; font-size: 1rem;
         color: #38bdf8; text-transform: uppercase; letter-spacing: 0.1em;
         border-bottom: 1px solid #1e2535; padding-bottom: 6px; }
    h3 { font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem;
         color: #94a3b8; text-transform: uppercase; letter-spacing: 0.08em; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #111520;
        border-right: 1px solid #1e2535;
    }

    /* Data tables */
    [data-testid="stDataFrame"] { border: 1px solid #1e2535; border-radius: 6px; }

    /* Synthetic banner */
    .synth-banner {
        background: #1a1f2e;
        border: 1px solid #334155;
        border-left: 3px solid #38bdf8;
        border-radius: 4px;
        padding: 8px 14px;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.75rem;
        color: #7dd3fc;
        margin-bottom: 16px;
    }
    .status-dot-green { display:inline-block; width:8px; height:8px;
                        border-radius:50%; background:#34d399; margin-right:6px; }
    .status-dot-red   { display:inline-block; width:8px; height:8px;
                        border-radius:50%; background:#f87171; margin-right:6px; }
</style>
""", unsafe_allow_html=True)

# ── Plotly dark theme ──────────────────────────────────────────────────────────
PLOTLY_THEME = dict(
    paper_bgcolor="#0d0f14",
    plot_bgcolor="#0d0f14",
    font=dict(family="IBM Plex Sans", color="#94a3b8", size=12),
    xaxis=dict(gridcolor="#1e2535", linecolor="#1e2535"),
    yaxis=dict(gridcolor="#1e2535", linecolor="#1e2535"),
)
COLOR_SEQ = ["#38bdf8","#818cf8","#34d399","#fb923c","#f472b6","#a78bfa","#fbbf24"]


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 💳 Connected Commerce")
    st.markdown("---")
    panel = st.radio(
        "Panel",
        ["Merchant Performance", "Fraud Velocity Monitor", "Pipeline Health", "Customer Profiles"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown("### Filters")
    days_back = st.slider("Days back", 1, 30, 14)
    st.markdown("---")

    health = get_pipeline_health()
    last_flush = health.get("last_kafka_flush", "—")
    if hasattr(last_flush, "strftime"):
        age_min = int((datetime.now() - last_flush).total_seconds() / 60)
        dot     = "green" if age_min < 10 else "red"
        st.markdown(
            f'<span class="status-dot-{dot}"></span>'
            f'<span style="font-family:IBM Plex Mono;font-size:0.72rem;color:#64748b;">'
            f'Last flush {age_min}m ago</span>',
            unsafe_allow_html=True,
        )
    if USE_SYNTHETIC:
        st.markdown(
            '<div class="synth-banner">⚡ SYNTHETIC DATA MODE<br>'
            'Set SNOWFLAKE_ACCOUNT env var to connect live.</div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 1 — MERCHANT PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════
if panel == "Merchant Performance":
    st.markdown("## Merchant Performance")

    df = get_merchant_performance()
    df["txn_date"] = pd.to_datetime(df["txn_date"])
    cutoff = datetime.now() - timedelta(days=days_back)
    df = df[df["txn_date"] >= cutoff]

    # ── KPI row ────────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue",    f"${df['total_revenue_usd'].sum():,.0f}")
    c2.metric("Total Transactions", f"{df['txn_count'].sum():,}")
    c3.metric("Avg Ticket",        f"${df['avg_ticket_usd'].mean():,.2f}")
    c4.metric("Active Merchants",  f"{df['merchant_name'].nunique()}")

    st.markdown("---")

    # ── Revenue by category (bar) ──────────────────────────────────────────────
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### Revenue by Category")
        cat_df = (
            df.groupby("merchant_category")["total_revenue_usd"]
              .sum().reset_index()
              .sort_values("total_revenue_usd", ascending=True)
        )
        fig = px.bar(
            cat_df, x="total_revenue_usd", y="merchant_category",
            orientation="h",
            color="total_revenue_usd",
            color_continuous_scale=[[0,"#1e2535"],[1,"#38bdf8"]],
            labels={"total_revenue_usd": "Revenue (USD)", "merchant_category": ""},
        )
        fig.update_layout(**PLOTLY_THEME, showlegend=False,
                          coloraxis_showscale=False,
                          margin=dict(l=0,r=0,t=10,b=0), height=320)
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Revenue Share by Segment")
        seg_df = df.groupby("merchant_segment")["total_revenue_usd"].sum().reset_index()
        fig = px.pie(
            seg_df, values="total_revenue_usd", names="merchant_segment",
            hole=0.55, color_discrete_sequence=COLOR_SEQ,
        )
        fig.update_layout(**PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0), height=320)
        fig.update_traces(textfont_color="#e2e8f0", textfont_size=11)
        st.plotly_chart(fig, use_container_width=True)

    # ── Daily revenue trend ────────────────────────────────────────────────────
    st.markdown("### Daily Revenue Trend")
    daily = df.groupby("txn_date")["total_revenue_usd"].sum().reset_index()
    fig = px.area(
        daily, x="txn_date", y="total_revenue_usd",
        labels={"txn_date": "", "total_revenue_usd": "Revenue (USD)"},
        color_discrete_sequence=["#38bdf8"],
    )
    fig.update_traces(fill="tozeroy", fillcolor="rgba(56,189,248,0.08)",
                      line=dict(width=2))
    fig.update_layout(**PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0), height=220)
    st.plotly_chart(fig, use_container_width=True)

    # ── Top merchants table ────────────────────────────────────────────────────
    st.markdown("### Top 20 Merchants by Revenue")
    top = (
        df.groupby(["merchant_name","merchant_category","state"])
          .agg(revenue=("total_revenue_usd","sum"), txns=("txn_count","sum"))
          .reset_index()
          .sort_values("revenue", ascending=False)
          .head(20)
    )
    top["revenue"] = top["revenue"].map("${:,.2f}".format)
    top["txns"]    = top["txns"].map("{:,}".format)
    st.dataframe(top, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 2 — FRAUD VELOCITY MONITOR
# ══════════════════════════════════════════════════════════════════════════════
elif panel == "Fraud Velocity Monitor":
    st.markdown("## Fraud Velocity Monitor")

    df = get_fraud_alerts()
    df["event_time"] = pd.to_datetime(df["event_time"])

    # ── KPI row ────────────────────────────────────────────────────────────────
    high   = (df["alert_score"] == 4).sum()
    medium = (df["alert_score"].isin([2, 3])).sum()
    intl   = df["is_international"].sum()
    cross  = df["cross_merchant_flag"].sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🔴 Critical Alerts (score=4)", f"{high:,}")
    c2.metric("🟠 Medium Alerts (score 2–3)", f"{medium:,}")
    c3.metric("🌍 International Txns",         f"{intl:,}")
    c4.metric("🔀 Cross-Merchant Events",       f"{cross:,}")

    st.markdown("---")

    col1, col2 = st.columns([2, 3])

    with col1:
        st.markdown("### Alert Score Distribution")
        score_df = df["alert_score"].value_counts().reset_index()
        score_df.columns = ["score", "count"]
        score_df["color"] = score_df["score"].map(
            {4:"#f87171", 3:"#fb923c", 2:"#fbbf24", 1:"#34d399"}
        )
        fig = px.bar(
            score_df.sort_values("score"), x="score", y="count",
            color="score",
            color_discrete_map={4:"#f87171", 3:"#fb923c", 2:"#fbbf24", 1:"#34d399"},
            labels={"score": "Alert Score", "count": "Count"},
        )
        fig.update_layout(**PLOTLY_THEME, showlegend=False,
                          margin=dict(l=0,r=0,t=10,b=0), height=280)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Signal Heatmap — Category × Risk Tier")
        heat = (
            df.groupby(["merchant_category","risk_tier"])
              .size().reset_index(name="count")
              .pivot(index="merchant_category", columns="risk_tier", values="count")
              .fillna(0)
        )
        fig = go.Figure(go.Heatmap(
            z=heat.values,
            x=heat.columns.tolist(),
            y=heat.index.tolist(),
            colorscale=[[0,"#161b27"],[0.5,"#1e3a5f"],[1,"#f87171"]],
            showscale=True,
        ))
        fig.update_layout(**PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0), height=280)
        st.plotly_chart(fig, use_container_width=True)

    # ── Velocity scatter ───────────────────────────────────────────────────────
    st.markdown("### Velocity Profile — Txn Count (1h) vs. Spend (24h)")
    fig = px.scatter(
        df,
        x="txn_count_last_1h", y="spend_velocity_24h",
        color="risk_tier",
        size="amount_usd",
        size_max=18,
        hover_data=["customer_id","merchant_name","channel","alert_score"],
        color_discrete_map={"high":"#f87171","medium":"#fb923c","low":"#34d399"},
        labels={"txn_count_last_1h":"Txn Count (Last 1h)",
                "spend_velocity_24h":"Spend Velocity (24h USD)"},
        opacity=0.75,
    )
    fig.update_layout(**PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0), height=300)
    st.plotly_chart(fig, use_container_width=True)

    # ── Alert feed table ───────────────────────────────────────────────────────
    st.markdown("### Live Alert Feed")
    feed = (
        df[["event_time","transaction_id","customer_id","merchant_name",
            "amount_usd","alert_score","risk_tier","cross_merchant_flag",
            "is_international","channel"]]
          .sort_values("alert_score", ascending=False)
          .head(50)
    )
    feed["amount_usd"] = feed["amount_usd"].map("${:,.2f}".format)
    st.dataframe(feed, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 3 — PIPELINE HEALTH
# ══════════════════════════════════════════════════════════════════════════════
elif panel == "Pipeline Health":
    st.markdown("## Pipeline Health")

    health = get_pipeline_health()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Records Loaded Today",     f"{health['total_loaded_today']:,}")
    c2.metric("Last Batch Size",          f"{health['records_last_batch']:,}")
    c3.metric("Snowflake Load Latency",   f"{health['snowflake_load_latency']}s")
    c4.metric("Quarantine Rate",          f"{health['quarantine_rate_pct']}%")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Data Quality Scorecard")

        checks = {
            "Schema Violations Today": health["schema_violations_today"],
            "DLQ Messages Today":      health["dlq_count_today"],
            "Quarantine Rate %":       health["quarantine_rate_pct"],
        }
        for label, val in checks.items():
            threshold = 5 if "Violations" in label or "DLQ" in label else 1.0
            color  = "#f87171" if val > threshold else "#34d399"
            status = "WARN" if val > threshold else "OK"
            st.markdown(
                f"""
                <div style="display:flex; justify-content:space-between;
                            align-items:center; padding:10px 14px;
                            background:#161b27; border:1px solid #1e2535;
                            border-radius:6px; margin-bottom:8px;">
                    <span style="font-family:IBM Plex Mono;font-size:0.8rem;
                                 color:#94a3b8;">{label}</span>
                    <span style="font-family:IBM Plex Mono;font-size:0.85rem;
                                 color:{color}; font-weight:600;">{val} [{status}]</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with col2:
        st.markdown("### Pipeline Stage Status")

        stages = [
            ("Kafka Producer",        True,  "Emitting ~50 txn/s"),
            ("Kafka Consumer",        True,  "Micro-batch flush every 5m"),
            ("S3 Raw Landing",        True,  "AVRO partitioned by hour"),
            ("PySpark Validate",      True,  "Last run: < 1h ago"),
            ("PySpark Enrich",        True,  "Velocity features OK"),
            ("Iceberg Write",         True,  "Curated layer current"),
            ("Snowflake COPY INTO",   True,  "Star schema up to date"),
            ("Streamlit Dashboard",   True,  "Connected"),
        ]
        for name, ok, note in stages:
            dot   = "status-dot-green" if ok else "status-dot-red"
            color = "#34d399" if ok else "#f87171"
            st.markdown(
                f"""
                <div style="display:flex; align-items:center; padding:7px 14px;
                            background:#161b27; border:1px solid #1e2535;
                            border-radius:6px; margin-bottom:6px; gap:10px;">
                    <span class="{dot}"></span>
                    <span style="font-family:IBM Plex Mono;font-size:0.8rem;
                                 color:#e2e8f0; flex:1;">{name}</span>
                    <span style="font-family:IBM Plex Mono;font-size:0.72rem;
                                 color:#475569;">{note}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ── Architecture flow diagram ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Architecture Flow")
    stages_flow = [
        "Kafka\nProducer", "Kafka\nTopic", "S3 Raw\n(AVRO)",
        "PySpark\nValidate", "PySpark\nEnrich", "S3 Curated\n(Iceberg)",
        "Snowflake\nSTAR SCHEMA", "Streamlit\nDashboard"
    ]
    x_pos  = list(range(len(stages_flow)))
    fig    = go.Figure()
    # Connector lines
    fig.add_trace(go.Scatter(
        x=x_pos, y=[0]*len(x_pos),
        mode="lines",
        line=dict(color="#1e2535", width=2),
        showlegend=False,
    ))
    # Nodes
    colors_node = ["#38bdf8","#38bdf8","#818cf8","#34d399","#34d399","#818cf8","#fb923c","#f472b6"]
    fig.add_trace(go.Scatter(
        x=x_pos, y=[0]*len(x_pos),
        mode="markers+text",
        marker=dict(size=24, color=colors_node, line=dict(width=2, color="#0d0f14")),
        text=stages_flow,
        textposition="top center",
        textfont=dict(family="IBM Plex Mono", size=9, color="#94a3b8"),
        showlegend=False,
    ))
    _theme_no_axes = {k: v for k, v in PLOTLY_THEME.items() if k not in ("xaxis", "yaxis")}
    fig.update_layout(
        **_theme_no_axes,
        height=160,
        margin=dict(l=20,r=20,t=50,b=10),
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, range=[-0.5, 0.8]),
    )
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PANEL 4 — CUSTOMER PROFILES
# ══════════════════════════════════════════════════════════════════════════════
elif panel == "Customer Profiles":
    st.markdown("## Customer Spend Profiles")

    df = get_customer_profiles()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Customers",     f"{len(df):,}")
    c2.metric("Avg Lifetime Spend",  f"${df['lifetime_spend_usd'].mean():,.0f}")
    c3.metric("High Risk Customers", f"{(df['risk_tier']=='high').sum():,}")
    c4.metric("Platinum Tier",       f"{(df['spend_segment']=='platinum').sum():,}")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Spend Segment Distribution")
        seg = df["spend_segment"].value_counts().reset_index()
        seg.columns = ["segment","count"]
        order = ["platinum","gold","silver","bronze"]
        seg["segment"] = pd.Categorical(seg["segment"], categories=order, ordered=True)
        seg = seg.sort_values("segment")
        fig = px.bar(
            seg, x="segment", y="count",
            color="segment",
            color_discrete_map={
                "platinum":"#f472b6","gold":"#fbbf24",
                "silver":"#94a3b8","bronze":"#fb923c"
            },
            labels={"segment":"Segment","count":"Customers"},
        )
        fig.update_layout(**PLOTLY_THEME, showlegend=False,
                          margin=dict(l=0,r=0,t=10,b=0), height=280)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Recency vs. Frequency")
        fig = px.scatter(
            df, x="recency_days", y="txn_frequency_per_day",
            color="spend_segment",
            color_discrete_map={
                "platinum":"#f472b6","gold":"#fbbf24",
                "silver":"#94a3b8","bronze":"#fb923c"
            },
            size="lifetime_spend_usd", size_max=16,
            opacity=0.7,
            labels={"recency_days":"Days Since Last Txn",
                    "txn_frequency_per_day":"Txns per Day"},
        )
        fig.update_layout(**PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0), height=280)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Customer Table")
    display_cols = [
        "customer_id","account_type","risk_tier","spend_segment",
        "lifetime_txn_count","lifetime_spend_usd","avg_txn_usd",
        "recency_days","distinct_merchants_visited",
    ]
    out = df[display_cols].copy()
    out["lifetime_spend_usd"] = out["lifetime_spend_usd"].map("${:,.2f}".format)
    out["avg_txn_usd"]        = out["avg_txn_usd"].map("${:,.2f}".format)
    st.dataframe(out.head(100), use_container_width=True, hide_index=True)