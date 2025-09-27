"""Streamlit dashboard that surfaces status information from the FastAPI backend."""

import os
from typing import Any

import requests
import streamlit as st

# Configure base UI elements such as title and layout
st.set_page_config(page_title="Climate Dashboard", layout="wide")

# Discover service endpoints from environment with sensible defaults
API_BASE_URL = os.getenv("API_BASE_URL", "http://fastapi:8000")
HEALTH_ENDPOINT = f"{API_BASE_URL}/health"
ROOT_ENDPOINT = f"{API_BASE_URL}/"


@st.cache_data(ttl=30)
def fetch_json(url: str) -> dict[str, Any]:
    """Fetch and cache JSON responses to reduce chatter between containers."""
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


def render_header() -> None:
    """Render the dashboard header and supporting copy."""
    st.title("Climate Data Dashboard")
    st.caption("FastAPI + Streamlit demo stack")


def render_health() -> None:
    """Display the API health status and bubble up any errors."""
    st.subheader("Service Health")
    try:
        health = fetch_json(HEALTH_ENDPOINT)
        st.success(f"FastAPI service: {health.get('status', 'unknown')}")
    except Exception as exc:  # pylint: disable=broad-except
        st.error(f"Health check failed: {exc}")
        with st.expander("How to fix / run backend", expanded=False):
            st.markdown(
                """
                The dashboard is trying to reach the FastAPI backend at `API_BASE_URL`.
                
                Quick options:
                1. If you don't have the backend yet, ignore this — the rest of the UI still loads.
                2. To change the target, set an environment variable before launching Streamlit:
                   
                   Windows PowerShell:
                   
                   ```powershell
                   $env:API_BASE_URL = "http://localhost:8000"; streamlit run streamlit/app.py
                   ```
                   
                   macOS / Linux:
                   
                   ```bash
                   API_BASE_URL=http://localhost:8000 streamlit run streamlit/app.py
                   ```
                3. Once a FastAPI service exposes `/health` returning JSON, this panel will go green.
                """
            )


def render_overview() -> None:
    """Surface basic API metadata and payload examples."""
    st.subheader("API Overview")
    col1, col2 = st.columns(2)

    with col1:
        st.metric("API Base", API_BASE_URL)
    with col2:
        try:
            payload = fetch_json(ROOT_ENDPOINT)
            st.json(payload)
        except Exception as exc:  # pylint: disable=broad-except
            st.warning(f"Unable to reach API root: {exc}")
            st.caption(
                "Provide a running FastAPI service (e.g., start one on localhost:8000) to see metadata here."
            )


if __name__ == "__main__":
    # Streamlit executes top-to-bottom; keep the main flow explicit for clarity
    render_header()
    render_health()
    render_overview()
