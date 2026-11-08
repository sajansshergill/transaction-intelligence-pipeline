"""
dashboard/app.py — Connected Commerce Transaction Intelligence Console

Kafka -> S3 -> PySpark -> Snowflake pipeline, served as an interactive
console. Runs standalone on demo data generated from the project's own
star schema and PySpark velocity logic (see dashboard/demo_data.py);
set SNOWFLAKE_ACCOUNT to serve the live Snowflake views instead.

Console: global sidebar filters that live-update every view, KPI cards
with period-over-period deltas, Plotly animated charts
(animation_frame) and a genuine 3D risk space (Scatter3d + surface).

Run:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# Community Cloud / repo-root launches: make the repo root importable.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from dashboard.demo_data import build_all
    from dashboard.snowflake_connector import USE_SYNTHETIC, get_pipeline_health
except ModuleNotFoundError:  # launched with cwd=dashboard/
    from demo_data import build_all  # type: ignore
    from snowflake_connector import USE_SYNTHETIC, get_pipeline_health  # type: ignore

st.set_page_config(
    page_title="Transaction Intelligence — Connected Commerce",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ----------------------------------------------------------------------------
# Custom CSS — dark glass / gradient console styling
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 600px at 15% -10%, #0b3a5c 0%, transparent 55%),
                    radial-gradient(1000px 500px at 90% 0%, #3b1158 0%, transparent 50%),
                    #0d0f14;
    }
    .tip-hero {
        padding: 1.1rem 1.4rem; border-radius: 16px; margin-bottom: 0.6rem;
        background: linear-gradient(120deg, rgba(14,90,140,.55), rgba(110,40,180,.45));
        border: 1px solid rgba(125,211,252,.35);
        box-shadow: 0 8px 32px rgba(0,0,0,.45);
    }
    .tip-hero h1 {
        margin: 0; font-size: 2.0rem; letter-spacing: .5px;
        background: linear-gradient(90deg,#7dd3fc,#c4b5fd,#6ee7b7);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .tip-hero p { margin: .25rem 0 0 0; color: #cfd8e3; font-size: .95rem; }
    div[data-testid="stMetric"] {
        background: rgba(22,27,39,.85); border: 1px solid rgba(56,189,248,.35);
        border-radius: 12px; padding: .6rem .8rem;
        box-shadow: 0 4px 18px rgba(0,0,0,.35);
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(196,181,253,.7); }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(22,27,39,.8); border-radius: 10px 10px 0 0; padding: 6px 14px;
    }
    section[data-testid="stSidebar"] { background: rgba(13,15,20,.92); }
    .synth-banner {
        background: rgba(26,31,46,.9); border: 1px solid #334155;
        border-left: 3px solid #38bdf8; border-radius: 8px;
        padding: 8px 14px; font-size: 0.78rem; color: #7dd3fc; margin-top: 12px;
    }
    .stage-row {
        display:flex; align-items:center; gap:10px; padding:7px 14px;
        background: rgba(22,27,39,.85); border: 1px solid rgba(56,189,248,.25);
        border-radius: 8px; margin-bottom: 6px; font-size: 0.82rem;
    }
    .dot-green { width:8px; height:8px; border-radius:50%; background:#34d399; flex:none; }
    </style>
    <div class="tip-hero">
      <h1>💳 Transaction Intelligence — Connected Commerce Console</h1>
      <p>Real-time fraud signal aggregation &amp; merchant analytics —
         Kafka → S3 → PySpark → Snowflake → this console. Drag the 3D risk
         space, press play on the animated charts, slice everything from
         the sidebar.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300)
def load_all() -> dict:
    return build_all()


DATA = load_all()
txns = DATA["transactions"]
daily = DATA["merchant_daily"]
customers = DATA["customers"]
pipe_daily = DATA["pipeline_daily"]
health = get_pipeline_health()


def _money(x: float) -> str:
    ax = abs(x)
    if ax >= 1e6:
        return f"${x / 1e6:,.2f}M"
    if ax >= 1e3:
        return f"${x / 1e3:,.1f}K"
    return f"${x:,.0f}"


# ----------------------------------------------------------------------------
# Sidebar — global dynamic filters (every view below reads filtered data)
# ----------------------------------------------------------------------------
st.sidebar.header("🎛️ Live filters")
dmin, dmax = txns["txn_date"].min(), txns["txn_date"].max()
picked = st.sidebar.date_input(
    "Date range", value=(dmin.date(), dmax.date()),
    min_value=dmin.date(), max_value=dmax.date(),
)
if isinstance(picked, (list, tuple)) and len(picked) == 2:
    start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])
else:
    start, end = dmin, dmax

all_cats = sorted(txns["merchant_category"].unique())
sel_cats = st.sidebar.multiselect("Merchant categories", all_cats, default=all_cats)
all_segs = sorted(txns["merchant_segment"].unique())
sel_segs = st.sidebar.multiselect("Segments", all_segs, default=all_segs)
all_channels = sorted(txns["channel"].unique())
sel_channels = st.sidebar.multiselect("Channels", all_channels, default=all_channels)
all_risks = ["low", "medium", "high"]
sel_risks = st.sidebar.multiselect("Risk tiers", all_risks, default=all_risks)
all_states = sorted(txns["state"].unique())
sel_states = st.sidebar.multiselect("States", all_states, default=all_states)
min_amount = st.sidebar.slider("Min transaction amount ($)", 0, 1000, 0, 25)
fraud_only = st.sidebar.checkbox("Fraud-flagged only", value=False)

if USE_SYNTHETIC:
    st.sidebar.markdown(
        '<div class="synth-banner">⚡ DEMO DATA MODE<br>'
        'Standalone data generated from the project star schema + '
        'PySpark velocity logic. Set SNOWFLAKE_ACCOUNT to query the '
        'live Snowflake views.</div>',
        unsafe_allow_html=True,
    )


def _apply(df: pd.DataFrame, *, date_col: str = "txn_date") -> pd.DataFrame:
    if df.empty:
        return df
    out = df[(df[date_col] >= start) & (df[date_col] <= end)]
    if "merchant_category" in out.columns and sel_cats:
        out = out[out["merchant_category"].isin(sel_cats)]
    if "merchant_segment" in out.columns and sel_segs:
        out = out[out["merchant_segment"].isin(sel_segs)]
    if "channel" in out.columns and sel_channels:
        out = out[out["channel"].isin(sel_channels)]
    if "risk_tier" in out.columns and sel_risks:
        out = out[out["risk_tier"].isin(sel_risks)]
    if "state" in out.columns and sel_states:
        out = out[out["state"].isin(sel_states)]
    if "amount_usd" in out.columns and min_amount > 0:
        out = out[out["amount_usd"] >= min_amount]
    return out


f_txns = _apply(txns)
if fraud_only:
    f_txns = f_txns[
        f_txns["high_velocity_flag"] | f_txns["cross_merchant_flag"]
        | f_txns["is_international"] | (f_txns["risk_tier"] == "high")
    ]
f_fraud = f_txns[
    f_txns["high_velocity_flag"] | f_txns["cross_merchant_flag"]
    | f_txns["is_international"] | (f_txns["risk_tier"] == "high")
]
f_daily = _apply(daily)
f_cust = customers.copy()
if sel_risks:
    f_cust = f_cust[f_cust["risk_tier"].isin(sel_risks)]
f_pipe = pipe_daily[(pipe_daily["txn_date"] >= start) & (pipe_daily["txn_date"] <= end)]

# ----------------------------------------------------------------------------
# KPI row — with deltas vs the prior equal-length period
# ----------------------------------------------------------------------------
if not f_txns.empty:
    revenue = float(f_txns["amount_usd"].sum())
    span_days = max((end - start).days, 1)
    prior_end = start - pd.Timedelta(days=1)
    prior_start = prior_end - pd.Timedelta(days=span_days)
    prior = txns[(txns["txn_date"] >= prior_start) & (txns["txn_date"] <= prior_end)]
    if sel_cats:
        prior = prior[prior["merchant_category"].isin(sel_cats)]
    prior_rev = float(prior["amount_usd"].sum()) if not prior.empty else 0.0
    prior_n = len(prior)
    alerts = int((f_txns["alert_score"] >= 3).sum())
    prior_alerts = int((prior["alert_score"] >= 3).sum()) if not prior.empty else 0
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue (filtered)", _money(revenue),
              delta=_money(revenue - prior_rev) + " vs prior period" if not prior.empty else None)
    c2.metric("Transactions", f"{len(f_txns):,}",
              delta=f"{len(f_txns) - prior_n:+,} vs prior period" if not prior.empty else None)
    c3.metric("Avg ticket", f"${f_txns['amount_usd'].mean():,.2f}",
              delta=f"{f_txns['merchant_name'].nunique()} active merchants")
    c4.metric("High-severity alerts", f"{alerts:,}",
              delta=f"{alerts - prior_alerts:+,} vs prior period" if not prior.empty else None,
              delta_color="inverse")
    c5.metric("High-risk share",
              f"{(f_txns['risk_tier'] == 'high').mean() * 100:.1f}%",
              delta=f"{f_txns['customer_id'].nunique()} customers in window")
else:
    st.warning("No transactions match the current filters.")

tab_merch, tab_anim, tab_3d, tab_fraud, tab_cust, tab_pipe = st.tabs(
    [
        "💰 Merchant performance",
        "🎬 Animated flows",
        "🧊 3D risk space",
        "🚨 Fraud velocity",
        "👥 Customer profiles",
        "⚙️ Pipeline health",
    ]
)

# ═══════════════════════════════════════════════════════════════════════════
with tab_merch:
    st.subheader("Revenue by category")
    if f_daily.empty:
        st.info("No merchant rows in the current filter.")
    else:
        left, right = st.columns([3, 2])
        with left:
            cat_df = (
                f_daily.groupby("merchant_category")["total_revenue_usd"]
                .sum().reset_index().sort_values("total_revenue_usd")
            )
            fig = px.bar(cat_df, x="total_revenue_usd", y="merchant_category",
                         orientation="h", color="total_revenue_usd",
                         color_continuous_scale="Blues",
                         labels={"total_revenue_usd": "Revenue (USD)", "merchant_category": ""},
                         template="plotly_dark")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              coloraxis_showscale=False, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")
        with right:
            st.subheader("Revenue share by segment")
            seg_df = f_daily.groupby("merchant_segment")["total_revenue_usd"].sum().reset_index()
            fig = px.pie(seg_df, values="total_revenue_usd", names="merchant_segment",
                         hole=0.55, template="plotly_dark")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")

        st.subheader("Daily revenue trend")
        trend = f_daily.groupby("txn_date")["total_revenue_usd"].sum().reset_index()
        fig = px.area(trend, x="txn_date", y="total_revenue_usd",
                      labels={"txn_date": "", "total_revenue_usd": "Revenue (USD)"},
                      template="plotly_dark")
        fig.update_traces(fill="tozeroy", fillcolor="rgba(56,189,248,0.10)",
                          line=dict(width=2, color="#38bdf8"))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          margin=dict(l=0, r=0, t=10, b=0), height=240)
        st.plotly_chart(fig, width="stretch")

        st.subheader("Top 20 merchants by revenue")
        top = (
            f_daily.groupby(["merchant_name", "merchant_category", "state"], as_index=False)
            .agg(revenue=("total_revenue_usd", "sum"), txns=("txn_count", "sum"),
                 avg_ticket=("avg_ticket_usd", "mean"))
            .sort_values("revenue", ascending=False).head(20)
        )
        st.dataframe(top, width="stretch", hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════
with tab_anim:
    st.subheader("Press play — daily revenue by category, evolving day by day")
    if f_daily.empty:
        st.info("No merchant rows in the current filter.")
    else:
        anim = (
            f_daily.groupby(["txn_date", "merchant_category"], as_index=False)
            ["total_revenue_usd"].sum()
        )
        anim["frame"] = anim["txn_date"].dt.strftime("%Y-%m-%d")
        anim["revenue_k"] = anim["total_revenue_usd"] / 1e3
        lim = float(anim["revenue_k"].max()) or 1.0
        fig_a = px.bar(
            anim.sort_values(["txn_date", "merchant_category"]),
            x="merchant_category", y="revenue_k", color="merchant_category",
            animation_frame="frame", range_y=[0, lim * 1.15],
            labels={"revenue_k": "Revenue ($K)", "merchant_category": "Category"},
            template="plotly_dark", title="Daily revenue by category — animated",
        )
        fig_a.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_a.layout.updatemenus:
            fig_a.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_a, width="stretch")

        cum = anim.copy()
        cum["cum_revenue_k"] = cum.groupby("merchant_category")["revenue_k"].cumsum()
        fig_c = px.line(
            cum.sort_values(["txn_date", "merchant_category"]),
            x="txn_date", y="cum_revenue_k", color="merchant_category",
            animation_frame="frame",
            labels={"cum_revenue_k": "Cumulative revenue ($K)", "txn_date": "Date"},
            template="plotly_dark", title="Cumulative revenue race — animated",
        )
        fig_c.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_c.layout.updatemenus:
            fig_c.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_c, width="stretch")

        st.subheader("▶ Animated — fraud velocity through time")
        if f_fraud.empty:
            st.info("No flagged transactions in the current filter.")
        else:
            fa = f_fraud.copy()
            fa["frame"] = fa["txn_date"].dt.strftime("%Y-%m-%d")
            fig_f = px.scatter(
                fa.sort_values("txn_date"),
                x="txn_count_last_1h", y="spend_velocity_24h",
                color="risk_tier", size="amount_usd", size_max=16,
                animation_frame="frame",
                hover_data=["customer_id", "merchant_name", "channel"],
                color_discrete_map={"high": "#f87171", "medium": "#fb923c", "low": "#34d399"},
                labels={"txn_count_last_1h": "Txn count (last 1h)",
                        "spend_velocity_24h": "Spend velocity 24h (USD)"},
                template="plotly_dark",
            )
            fig_f.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            if fig_f.layout.updatemenus:
                fig_f.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 350
            st.plotly_chart(fig_f, width="stretch")
    st.caption("Animations are native Plotly `animation_frame` charts — hit ▶ Play on each chart.")

# ═══════════════════════════════════════════════════════════════════════════
with tab_3d:
    st.subheader("🧊 3D risk space — drag to rotate, scroll to zoom")
    if f_txns.empty:
        st.info("No transactions in the current filter.")
    else:
        d3 = f_txns.copy()
        d3["size"] = np.clip(d3["amount_usd"] / 60, 2, 18)
        fig3 = px.scatter_3d(
            d3.sort_values("txn_date"),
            x="txn_count_last_1h", y="spend_velocity_24h", z="amount_usd",
            color="risk_tier", size="size", size_max=16, opacity=0.82,
            hover_data=["transaction_id", "merchant_name", "channel", "alert_score"],
            color_discrete_map={"high": "#f87171", "medium": "#fb923c", "low": "#34d399"},
            labels={"txn_count_last_1h": "Txn count (1h)",
                    "spend_velocity_24h": "Spend velocity 24h ($)",
                    "amount_usd": "Amount ($)"},
            template="plotly_dark",
            title="Every transaction in velocity × velocity × amount space "
                  "(PySpark enrich.py features)",
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            scene=dict(
                xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                zaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
            ),
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig3, width="stretch")

        st.subheader("Revenue surface — category × date terrain")
        surf = (
            f_daily.pivot_table(index="txn_date", columns="merchant_category",
                                values="total_revenue_usd", aggfunc="sum")
            .sort_index().fillna(0.0)
        )
        if surf.shape[0] > 2 and surf.shape[1] > 1:
            z = (surf / 1e3).to_numpy()
            fig_s = go.Figure(data=[go.Surface(
                z=z,
                x=[d.strftime("%Y-%m-%d") for d in surf.index],
                y=list(surf.columns),
                colorscale="Viridis",
                colorbar=dict(title="Revenue ($K)"),
            )])
            fig_s.update_layout(
                template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                scene=dict(xaxis_title="Date", yaxis_title="Category",
                           zaxis_title="Revenue ($K)",
                           camera=dict(eye=dict(x=1.5, y=-1.4, z=0.9))),
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig_s, width="stretch")
        else:
            st.info("Not enough category/date coverage in the current filter for a surface.")

# ═══════════════════════════════════════════════════════════════════════════
with tab_fraud:
    st.subheader("Fraud velocity monitor")
    if f_fraud.empty:
        st.info("No flagged transactions in the current filter.")
    else:
        high = int((f_fraud["alert_score"] == 4).sum())
        med = int(f_fraud["alert_score"].isin([2, 3]).sum())
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("🔴 Critical alerts (score 4)", f"{high:,}")
        k2.metric("🟠 Medium alerts (score 2–3)", f"{med:,}")
        k3.metric("🌍 International txns", f"{int(f_fraud['is_international'].sum()):,}")
        k4.metric("🔀 Cross-merchant events", f"{int(f_fraud['cross_merchant_flag'].sum()):,}")

        left, right = st.columns([2, 3])
        with left:
            st.subheader("Alert score distribution")
            score_df = f_fraud["alert_score"].value_counts().reset_index()
            score_df.columns = ["score", "count"]
            fig = px.bar(score_df.sort_values("score"), x="score", y="count",
                         color="score",
                         color_discrete_map={4: "#f87171", 3: "#fb923c", 2: "#fbbf24", 1: "#34d399"},
                         labels={"score": "Alert score", "count": "Count"},
                         template="plotly_dark")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              showlegend=False, margin=dict(l=0, r=0, t=10, b=0), height=280)
            st.plotly_chart(fig, width="stretch")
        with right:
            st.subheader("Signal heatmap — category × risk tier")
            heat = px.imshow(
                f_fraud.groupby(["merchant_category", "risk_tier"]).size()
                .unstack(fill_value=0),
                color_continuous_scale="RdBu_r", template="plotly_dark",
                labels={"color": "Alerts", "x": "Risk tier", "y": "Category"},
            )
            heat.update_layout(paper_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=10, b=0),
                               height=280)
            st.plotly_chart(heat, width="stretch")

        st.subheader("Velocity profile — txn count (1h) vs spend (24h)")
        fig = px.scatter(
            f_fraud, x="txn_count_last_1h", y="spend_velocity_24h",
            color="risk_tier", size="amount_usd", size_max=16, opacity=0.75,
            hover_data=["customer_id", "merchant_name", "channel", "alert_score"],
            color_discrete_map={"high": "#f87171", "medium": "#fb923c", "low": "#34d399"},
            labels={"txn_count_last_1h": "Txn count (last 1h)",
                    "spend_velocity_24h": "Spend velocity 24h (USD)"},
            template="plotly_dark",
        )
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

        st.subheader("Live alert feed — top 50 by score")
        feed = (
            f_fraud[["event_time", "transaction_id", "customer_id", "merchant_name",
                     "amount_usd", "alert_score", "risk_tier", "cross_merchant_flag",
                     "is_international", "channel"]]
            .sort_values(["alert_score", "event_time"], ascending=False).head(50)
        )
        st.dataframe(feed, width="stretch", hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════
with tab_cust:
    st.subheader("Customer spend profiles")
    if f_cust.empty:
        st.info("No customers in the current filter.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total customers", f"{len(f_cust):,}")
        k2.metric("Avg lifetime spend", f"${f_cust['lifetime_spend_usd'].mean():,.0f}")
        k3.metric("High-risk customers", f"{(f_cust['risk_tier'] == 'high').sum():,}")
        k4.metric("Platinum tier", f"{(f_cust['spend_segment'] == 'platinum').sum():,}")

        left, right = st.columns(2)
        with left:
            st.subheader("Spend segment distribution")
            seg = f_cust["spend_segment"].value_counts().reset_index()
            seg.columns = ["segment", "count"]
            order = ["platinum", "gold", "silver", "bronze"]
            seg["segment"] = pd.Categorical(seg["segment"], categories=order, ordered=True)
            seg = seg.sort_values("segment")
            fig = px.bar(seg, x="segment", y="count", color="segment",
                         color_discrete_map={"platinum": "#f472b6", "gold": "#fbbf24",
                                             "silver": "#94a3b8", "bronze": "#fb923c"},
                         labels={"segment": "Segment", "count": "Customers"},
                         template="plotly_dark")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              showlegend=False, margin=dict(l=0, r=0, t=10, b=0), height=280)
            st.plotly_chart(fig, width="stretch")
        with right:
            st.subheader("Recency vs frequency")
            fig = px.scatter(
                f_cust, x="recency_days", y="txn_frequency_per_day",
                color="spend_segment",
                color_discrete_map={"platinum": "#f472b6", "gold": "#fbbf24",
                                    "silver": "#94a3b8", "bronze": "#fb923c"},
                size="lifetime_spend_usd", size_max=14, opacity=0.7,
                labels={"recency_days": "Days since last txn",
                        "txn_frequency_per_day": "Txns per day"},
                template="plotly_dark",
            )
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              margin=dict(l=0, r=0, t=10, b=0), height=280)
            st.plotly_chart(fig, width="stretch")

        st.subheader("Customer table")
        cols = ["customer_id", "account_type", "risk_tier", "spend_segment",
                "lifetime_txn_count", "lifetime_spend_usd", "avg_txn_usd",
                "recency_days", "distinct_merchants_visited"]
        st.dataframe(f_cust[cols].head(100), width="stretch", hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════
with tab_pipe:
    st.subheader("Pipeline health")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Records loaded (window)", f"{int(f_pipe['records_loaded'].sum()):,}")
    k2.metric("Avg load latency", f"{f_pipe['load_latency_s'].mean():.1f}s")
    k3.metric("Avg quarantine rate", f"{f_pipe['quarantine_rate_pct'].mean():.2f}%")
    k4.metric("DLQ messages (window)", f"{int(f_pipe['dlq_count'].sum()):,}")

    if not f_pipe.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=f_pipe["txn_date"], y=f_pipe["records_loaded"],
                             name="Records loaded", marker_color="#38bdf8"))
        fig.add_trace(go.Scatter(x=f_pipe["txn_date"], y=f_pipe["load_latency_s"],
                                 name="Load latency (s)", yaxis="y2",
                                 mode="lines", line=dict(color="#f472b6", width=2)))
        fig.update_layout(
            template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(title="Records loaded"),
            yaxis2=dict(title="Latency (s)", overlaying="y", side="right"),
            legend=dict(orientation="h"), margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Data quality scorecard")
        checks = {
            "Schema violations (window)": int(f_pipe["schema_violations"].sum()),
            "DLQ messages (window)": int(f_pipe["dlq_count"].sum()),
            "Avg quarantine rate %": round(float(f_pipe["quarantine_rate_pct"].mean()), 2)
            if not f_pipe.empty else 0.0,
        }
        for label, val in checks.items():
            threshold = 5 if "Violations" in label or "DLQ" in label else 1.0
            status = "WARN" if float(val) > threshold else "OK"
            st.markdown(f"- **{label}:** {val} [{status}]")
    with right:
        st.subheader("Pipeline stage status")
        stages = [
            ("Kafka Producer", "Emitting ~50 txn/s"),
            ("Kafka Consumer", "Micro-batch flush every 5m"),
            ("S3 Raw Landing", "AVRO partitioned by hour"),
            ("PySpark Validate", "Schema + quality gates"),
            ("PySpark Enrich", "Velocity features OK"),
            ("Iceberg Write", "Curated layer current"),
            ("Snowflake COPY INTO", "Star schema up to date"),
            ("Streamlit Dashboard", "This console"),
        ]
        for name, note in stages:
            st.markdown(
                f'<div class="stage-row"><span class="dot-green"></span>'
                f'<span style="flex:1">{name}</span>'
                f'<span style="color:#64748b">{note}</span></div>',
                unsafe_allow_html=True,
            )

    st.subheader("Architecture flow")
    stages_flow = ["Kafka\nProducer", "Kafka\nTopic", "S3 Raw\n(AVRO)",
                   "PySpark\nValidate", "PySpark\nEnrich", "S3 Curated\n(Iceberg)",
                   "Snowflake\nSTAR SCHEMA", "Streamlit\nDashboard"]
    x_pos = list(range(len(stages_flow)))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x_pos, y=[0] * len(x_pos), mode="lines",
                             line=dict(color="#334155", width=2), showlegend=False))
    fig.add_trace(go.Scatter(
        x=x_pos, y=[0] * len(x_pos), mode="markers+text",
        marker=dict(size=24, color=["#38bdf8", "#38bdf8", "#818cf8", "#34d399",
                                    "#34d399", "#818cf8", "#fb923c", "#f472b6"],
                    line=dict(width=2, color="#0d0f14")),
        text=stages_flow, textposition="top center",
        textfont=dict(size=9, color="#94a3b8"), showlegend=False,
    ))
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                      height=160, margin=dict(l=20, r=20, t=50, b=10),
                      xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                      yaxis=dict(showgrid=False, showticklabels=False, zeroline=False,
                                 range=[-0.5, 0.8]))
    st.plotly_chart(fig, width="stretch")
