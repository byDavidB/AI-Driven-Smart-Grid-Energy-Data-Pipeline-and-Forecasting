"""Streamlit dashboard that surfaces status information and pipeline visuals."""

from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Tuple

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

st.set_page_config(page_title="Climate Dashboard", layout="wide", page_icon=":sunny:")

API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")
DEFAULT_SITE = os.getenv("SITE_NAME", "chicago_il")
HEALTH_ENDPOINT = f"{API_BASE_URL}/health"
ROOT_ENDPOINT = f"{API_BASE_URL}/"

# Tailwind-inspired theme to mimic the Weather Dashboard look
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;900&display=swap');

:root {
  --bg: #050d16;
  --panel: #101f30;
  --panel-soft: rgba(15, 30, 45, 0.85);
  --accent: #1193d4;
  --accent-soft: rgba(17, 147, 212, 0.18);
  --text-strong: #f8fbff;
  --text-muted: #9fb9d4;
}

body {
  font-family: "Inter", sans-serif;
}

[data-testid="stAppViewContainer"] {
  background: radial-gradient(circle at 12% 18%, #14314d 0%, #07111a 55%, #050b11 100%);
  color: var(--text-muted);
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #091522 0%, #050b11 100%);
  border-right: 1px solid rgba(17, 147, 212, 0.18);
}

.sidebar-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 32px;
  padding: 4px 6px;
}

.sidebar-logo {
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background: rgba(17, 147, 212, 0.18);
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-strong);
  font-weight: 700;
}

.sidebar-title {
  font-size: 1.05rem;
  font-weight: 700;
  color: var(--text-strong);
}

[data-testid="stSidebar"] div[role="radiogroup"] {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

[data-testid="stSidebar"] div[role="radiogroup"] > label {
  margin-bottom: 0;
}

[data-testid="stSidebar"] div[role="radio"] {
  background: rgba(10, 21, 32, 0.9);
  border: 1px solid rgba(17, 147, 212, 0.1);
  border-radius: 12px;
  padding: 10px 14px;
  color: var(--text-muted);
  font-weight: 600;
  transition: border 0.15s ease, background 0.15s ease, color 0.15s ease;
}

[data-testid="stSidebar"] div[role="radio"]:hover {
  border-color: rgba(17, 147, 212, 0.35);
}

[data-testid="stSidebar"] div[role="radio"][aria-checked="true"] {
  background: rgba(17, 147, 212, 0.22);
  border-color: rgba(17, 147, 212, 0.7);
  color: var(--text-strong);
}

.sidebar-footer {
  margin-top: 40px;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px 16px;
  border-radius: 14px;
  background: rgba(17, 147, 212, 0.12);
}

.sidebar-footer img {
  width: 42px;
  height: 42px;
  border-radius: 50%;
  object-fit: cover;
}

.hero-heading {
  color: var(--text-strong);
  font-size: 2rem;
  font-weight: 800;
  margin-bottom: 4px;
}

.hero-subtitle {
  color: var(--text-muted);
  font-size: 0.95rem;
}

.slider-wrap {
  background: rgba(10, 25, 37, 0.85);
  border-radius: 14px;
  padding: 10px 18px;
  border: 1px solid rgba(17, 147, 212, 0.18);
  margin-bottom: 12px;
}

.dashboard-card {
  border-radius: 18px;
  border: 1px solid rgba(17, 147, 212, 0.18);
  background: linear-gradient(180deg, rgba(15, 30, 45, 0.95) 0%, rgba(9, 20, 30, 0.95) 100%);
  box-shadow: 0 24px 40px -32px rgba(17, 147, 212, 0.9);
  padding: 22px 24px;
  margin-bottom: 16px;
}

.dashboard-card .metric-title {
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.78rem;
  color: var(--text-muted);
}

.dashboard-card .metric-value {
  color: var(--text-strong);
  font-size: 2.4rem;
  font-weight: 700;
  margin-top: 8px;
}

.metric-delta {
  font-size: 0.9rem;
  margin-left: 10px;
  padding: 2px 12px;
  border-radius: 999px;
  font-weight: 600;
}
.metric-delta.positive { background: rgba(34, 197, 94, 0.16); color: #22c55e; }
.metric-delta.negative { background: rgba(239, 68, 68, 0.16); color: #ef4444; }
.metric-delta.neutral { background: rgba(148, 163, 184, 0.18); color: #cbd5f5; }

.analysis-card {
  border-radius: 18px;
  border: 1px solid rgba(17, 147, 212, 0.18);
  background: linear-gradient(180deg, rgba(15, 30, 45, 0.95) 0%, rgba(7, 16, 24, 0.95) 100%);
  padding: 22px 24px;
  margin-bottom: 20px;
}

.analysis-card .card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;
}

.analysis-card h3 {
  color: var(--text-strong);
  font-size: 1.05rem;
  font-weight: 600;
  margin-bottom: 4px;
}

.analysis-card p.subtle {
  color: var(--text-muted);
  font-size: 0.85rem;
}

.analysis-card .legend-muted {
  display: flex;
  gap: 16px;
  color: var(--text-muted);
  font-size: 0.85rem;
}

.analysis-card .legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  display: inline-block;
  margin-right: 6px;
}

footer {visibility: hidden;}
</style>
"""


def inject_styles() -> None:
    if st.session_state.get("_custom_css_injected"):
        return
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    st.session_state["_custom_css_injected"] = True


@st.cache_data(ttl=30)
def fetch_json(url: str, params: Dict[str, Any] | None = None, timeout: int = 10) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


@st.cache_data(ttl=60)
def fetch_sites() -> List[str]:
    data = fetch_json(f"{API_BASE_URL}/weather/sites")
    return data.get("sites", [])


@st.cache_data(ttl=60)
def fetch_metrics(site: str) -> Dict[str, Any]:
    return fetch_json(f"{API_BASE_URL}/weather/metrics", params={"site": site})


@st.cache_data(ttl=60)
def fetch_raw_weather(site: str, hours: int) -> Dict[str, Any]:
    return fetch_json(f"{API_BASE_URL}/weather/raw", params={"site": site, "hours": hours})


@st.cache_data(ttl=60)
def fetch_hourly_weather(site: str, hours: int) -> Dict[str, Any]:
    return fetch_json(f"{API_BASE_URL}/weather/hourly", params={"site": site, "hours": hours})


# Mirror silver cleaning rules so dashboards can explain drops
def analyse_cleaning(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    if raw_df.empty:
        return raw_df, raw_df, {}

    df = raw_df.copy()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    df["ts_hour"] = df["ts_utc"].dt.floor("h")
    df["reason"] = "kept"

    for col in ("ghi_wm2", "t2m_c", "ws10_mps"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    invalid_ghi = (df["ghi_wm2"] < 0) & df["reason"].eq("kept")
    df.loc[invalid_ghi, "reason"] = "invalid_ghi"
    df.loc[invalid_ghi, "ghi_wm2"] = pd.NA

    invalid_temp = ((df["t2m_c"] < -80) | (df["t2m_c"] > 80)) & df["reason"].eq("kept")
    df.loc[invalid_temp, "reason"] = "invalid_temp"
    df.loc[invalid_temp, "t2m_c"] = pd.NA

    invalid_wind = (df["ws10_mps"] < 0) & df["reason"].eq("kept")
    df.loc[invalid_wind, "reason"] = "invalid_wind"
    df.loc[invalid_wind, "ws10_mps"] = pd.NA

    for col, label in (
        ("ghi_wm2", "missing_ghi"),
        ("t2m_c", "missing_temp"),
        ("ws10_mps", "missing_wind"),
    ):
        missing = df[col].isna() & df["reason"].eq("kept")
        df.loc[missing, "reason"] = label

    kept_df = df[df["reason"].eq("kept")].copy()
    if not kept_df.empty:
        kept_sorted = kept_df.sort_values(["site", "ts_hour", "ingested_at"])
        duplicate_mask = kept_sorted.duplicated(subset=["site", "ts_hour"], keep="last")
        dup_indices = kept_sorted.index[duplicate_mask]
        df.loc[dup_indices, "reason"] = "duplicate"
        kept_df = df[df["reason"].eq("kept")].copy()

    kept_df = kept_df.sort_values(["site", "ts_hour"])
    cleaned_df = kept_df[["site", "ts_hour", "ghi_wm2", "t2m_c", "ws10_mps"]].rename(
        columns={"ts_hour": "ts_utc", "t2m_c": "temp_c", "ws10_mps": "wind_mps"}
    )
    cleaned_df["ts_utc"] = pd.to_datetime(cleaned_df["ts_utc"], utc=True)

    drop_counts = df["reason"].value_counts().to_dict()
    drop_counts.setdefault("kept", len(cleaned_df))

    return df, cleaned_df, drop_counts


def _format_metric(value: float) -> str:
    if value is None or math.isnan(value):
        return "--"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def _format_delta(delta: float) -> Tuple[str, str]:
    if delta is None or math.isnan(delta):
        return "N/A", "neutral"
    sign = "+" if delta >= 0 else ""
    magnitude = f"{sign}{delta:.1f}%"
    status = "positive" if delta > 0 else "negative" if delta < 0 else "neutral"
    return magnitude, status


# KPI summary cards showing average clean vs raw values and deltas
def _render_metric_cards(raw_df: pd.DataFrame, fact_df: pd.DataFrame, hours: int) -> None:
    metric_specs = [
        ("Solar Irradiance (W/m^2)", "ghi_wm2", "ghi_wm2"),
        ("Temperature (degC)", "t2m_c", "temp_c"),
        ("Wind Speed (m/s)", "ws10_mps", "wind_mps"),
    ]
    card_cols = st.columns(len(metric_specs))
    for col, (title, raw_col, clean_col) in zip(card_cols, metric_specs):
        raw_mean = raw_df[raw_col].mean()
        clean_mean = fact_df[clean_col].mean() if not fact_df.empty and clean_col in fact_df.columns else math.nan
        delta_pct = ((clean_mean - raw_mean) / raw_mean * 100) if raw_mean not in (0, None) else math.nan
        formatted_value = _format_metric(clean_mean)
        delta_text, delta_status = _format_delta(delta_pct)
        col.markdown(
            f"""
            <div class="dashboard-card">
              <div class="metric-title">{title}</div>
              <div class="metric-value">{formatted_value}<span class="metric-delta {delta_status}">{delta_text}</span></div>
              <div style=\"color:var(--text-muted);font-size:0.82rem;margin-top:4px;\">Clean vs raw mean over {hours} hours</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# Reusable chart builder matching the mockup aesthetic
def _styled_chart(title: str, subtitle: str, raw_df: pd.DataFrame, fact_df: pd.DataFrame, raw_col: str, clean_col: str) -> None:
    fig = go.Figure()
    if not fact_df.empty and clean_col in fact_df.columns:
        fig.add_trace(
            go.Scatter(
                x=fact_df["ts_utc"],
                y=fact_df[clean_col],
                name="Clean Data",
                mode="lines",
                line=dict(color="#1fb9ff", width=3),
                fill="tozeroy",
                fillcolor="rgba(31, 185, 255, 0.20)",
            )
        )
    fig.add_trace(
        go.Scatter(
            x=raw_df["ts_utc"],
            y=raw_df[raw_col],
            name="Raw Data",
            mode="lines",
            line=dict(color="rgba(31, 185, 255, 0.38)", width=2, dash="dash"),
        )
    )
    fig.update_layout(
        height=320,
        margin=dict(l=8, r=8, t=8, b=8),
        plot_bgcolor="#102335",
        paper_bgcolor="#102335",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#9fb9d4")),
        xaxis=dict(color="#6da2c8", gridcolor="rgba(255,255,255,0.04)", showgrid=False),
        yaxis=dict(color="#6da2c8", gridcolor="rgba(255,255,255,0.06)")
    )
    fig.update_xaxes(showspikes=True, spikecolor="#1fb9ff", spikethickness=1)
    fig.update_yaxes(showspikes=True, spikecolor="#1fb9ff", spikethickness=1)

    st.markdown(
        f"""
        <div class="analysis-card">
          <div class="card-header">
            <div>
              <h3>{title}</h3>
              <p class="subtle">{subtitle}</p>
            </div>
            <div class="legend-muted">
              <span><span class="legend-dot" style="background:#1fb9ff;"></span>Clean Data</span>
              <span><span class="legend-dot" style="background:rgba(31,185,255,0.45);"></span>Raw Data</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.plotly_chart(fig, use_container_width=True, theme=None)


# Primary page: mimic the provided Weather Dashboard mockup
def render_weather_trends(site: str) -> None:
    st.markdown(
        """
        <div class="hero-heading">7-Day Weather Data Analysis</div>
        <div class="hero-subtitle">Comparing raw and clean weather data for the last 7 days.</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='slider-wrap'>", unsafe_allow_html=True)
    hours = st.slider(
        "Trend window (hours)",
        min_value=24,
        max_value=336,
        value=168,
        step=24,
        label_visibility="collapsed",
        help="Controls the time window shown in the cards and charts.",
    )
    st.markdown("</div>", unsafe_allow_html=True)

    raw_rows = fetch_raw_weather(site, hours).get("rows", [])
    fact_rows = fetch_hourly_weather(site, hours).get("rows", [])

    if not raw_rows:
        st.warning("No raw data for the selected window.")
        return

    raw_df = pd.DataFrame(raw_rows)
    raw_df["ts_utc"] = pd.to_datetime(raw_df["ts_utc"], utc=True)
    fact_df = pd.DataFrame(fact_rows)
    if not fact_df.empty:
        fact_df["ts_utc"] = pd.to_datetime(fact_df["ts_utc"], utc=True)

    _render_metric_cards(raw_df, fact_df, hours)

    chart_specs = [
        ("Solar Irradiance (W/m^2)", "Hourly irradiance with cleaning overlay", "ghi_wm2", "ghi_wm2"),
        ("Temperature (degC)", "Cleaned temperature remains within QC bounds", "t2m_c", "temp_c"),
        ("Wind Speed (m/s)", "Wind smoothing removes negative spikes", "ws10_mps", "wind_mps"),
    ]
    for title, subtitle, raw_col, clean_col in chart_specs:
        _styled_chart(title, subtitle, raw_df, fact_df, raw_col, clean_col)


def render_data_health(site: str) -> None:
    st.header("Data Health")
    hours = st.slider(
        "Hours of history",
        min_value=24,
        max_value=336,
        value=168,
        step=24,
    )

    raw_payload = fetch_raw_weather(site, hours)
    fact_payload = fetch_hourly_weather(site, hours)
    raw_df = pd.DataFrame(raw_payload.get("rows", []))
    fact_df = pd.DataFrame(fact_payload.get("rows", []))

    if raw_df.empty:
        st.warning("No raw data available for the selected window yet.")
        return

    analysed_df, simulated_clean_df, drop_counts = analyse_cleaning(raw_df)
    cleaned_rows = len(simulated_clean_df)
    raw_rows = len(raw_df)
    fact_rows = len(fact_df)
    kept_pct = (cleaned_rows / raw_rows) * 100 if raw_rows else 0

    metric_cols = st.columns(3)
    metric_cols[0].metric("Raw rows", raw_rows)
    metric_cols[1].metric("Clean rows", fact_rows, f"{kept_pct:.1f}% of raw")
    metric_cols[2].metric("Window", f"{hours} hours")

    drop_df = (
        pd.Series(drop_counts, name="count")
        .rename_axis("reason")
        .reset_index()
        .replace({"kept": "kept"})
    )
    drop_df = drop_df.sort_values("count", ascending=False)

    kept_value = drop_df.loc[drop_df["reason"] == "kept", "count"].sum()
    non_kept = drop_df[drop_df["reason"] != "kept"].copy()
    if not non_kept.empty:
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=["kept"] + non_kept["reason"].tolist(),
                    values=[kept_value] + non_kept["count"].tolist(),
                    hole=0.4,
                )
            ]
        )
        fig.update_layout(title="Row outcomes (raw -> silver)")
        st.plotly_chart(fig, use_container_width=True)

    comparison_df = pd.DataFrame(
        {
            "stage": ["raw_weather", "fact_weather"],
            "rows": [raw_rows, fact_rows],
        }
    )
    st.bar_chart(comparison_df.set_index("stage"))

    analysed_df["available"] = analysed_df[["ghi_wm2", "t2m_c", "ws10_mps"]].notna().all(axis=1)
    completeness = (
        analysed_df.groupby("ts_hour")[["ghi_wm2", "t2m_c", "ws10_mps"]]
        .agg(lambda x: int(x.notna().all()))
        .reset_index()
    )
    heatmap_df = completeness.melt(id_vars="ts_hour", var_name="variable", value_name="available")
    heatmap_df["available"] = heatmap_df["available"].astype(int)
    st.subheader("Data completeness heatmap")
    st.write(
        go.Figure(
            data=[
                go.Heatmap(
                    x=heatmap_df["ts_hour"],
                    y=heatmap_df["variable"],
                    z=heatmap_df["available"],
                    colorscale=[[0, "#e74c3c"], [1, "#27ae60"]],
                    colorbar=dict(title="Available"),
                )
            ]
        ).update_layout(height=260, margin=dict(l=0, r=0, t=30, b=0))
    )

    st.caption("Green = at least one record available for the hour; red = missing")


def render_schema_page(site: str) -> None:
    st.header("Pipeline Lineage")
    metrics = fetch_metrics(site)
    raw_rows = metrics.get("raw", {}).get("row_count", 0)
    fact_rows = metrics.get("fact", {}).get("row_count", 0)
    dropped_rows = metrics.get("dropped_rows", 0)

    node_labels = [
        "Bronze: raw_weather",
        "QC Dropped",
        "Silver: fact_weather",
        "Gold: mart_features",
        "Gold: mart_forecast",
        "Gold: mart_kpis",
    ]

    link_source = [0, 0, 2, 2, 2]
    link_target = [1, 2, 3, 4, 5]
    link_value = [max(dropped_rows, 0), max(fact_rows, 0), 0, 0, 0]

    sankey = go.Figure(
        data=[
            go.Sankey(
                node=dict(label=node_labels, pad=20, thickness=20),
                link=dict(source=link_source, target=link_target, value=link_value),
            )
        ]
    )
    sankey.update_layout(height=320, title="Row flow through the pipeline")
    st.plotly_chart(sankey, use_container_width=True)

    st.markdown(
        """
        ### Table lineage

        - **Bronze (`raw_weather`)**: raw NASA POWER payloads with full fidelity.
        - **Silver (`fact_weather`)**: hourly, cleaned observations for modelling.
        - **Gold (`mart_*`)**: feature, forecast, and KPI marts (future steps).
        """
    )


def main() -> None:
    inject_styles()

    sites = fetch_sites()
    if not sites:
        sites = [DEFAULT_SITE]
    default_index = sites.index(DEFAULT_SITE) if DEFAULT_SITE in sites else 0

    with st.sidebar:
        st.markdown(
            """
            <div class="sidebar-header">
              <div class="sidebar-logo">WD</div>
              <div class="sidebar-title">Weather Dashboard</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        site = st.selectbox("Site", options=sites, index=default_index, label_visibility="collapsed")
        page = st.radio("Page", ("Weather Trends", "Data Health", "Schema & Lineage"), index=0, label_visibility="collapsed")
        st.markdown(
            """
            <div class="sidebar-footer">
              <img src="https://lh3.googleusercontent.com/aida-public/AB6AXuBDKhQ6b3m2xPwrL5M61odopdNOqbYddBxwCmovxoIchVzJeD1djXQfKnu5n36agnHUIzEaJfnQxVfTJzjRQrgflYPStk9soMvfA6Fkh7fqvi54_xgvDzUAO7jjSSpfPrd9KBHDiiV6C7p29EMafLgcVFVLA1PfWhp04rECss7JLOlUbOOcKOrU1ibPby_-zM2Dq4y1cu7FlsLu4kZKLKHnUJkq_jK5hpls4234PVqVf6usEsK5UH_wZVeShiZvW4C-EC4-5RBacw4" alt="avatar" />
              <div>
                <div style="color:var(--text-strong);font-weight:600;">Alex Doe</div>
                <div style="color:var(--text-muted);font-size:0.85rem;">alex.doe@example.com</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if page == "Weather Trends":
        render_weather_trends(site)
    elif page == "Data Health":
        render_data_health(site)
    else:
        render_schema_page(site)


if __name__ == "__main__":
    main()
