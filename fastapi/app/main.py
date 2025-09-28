"""Entrypoint for the Climate API service used in the docker-compose stack."""

from __future__ import annotations

import datetime as dt
import os
from contextlib import asynccontextmanager
from typing import Any, Generator, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request
import httpx
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

API_TIMEOUT_SECONDS = 10
DEFAULT_DB_MIN_CONN = 1
DEFAULT_DB_MAX_CONN = 5


def _isoformat(value: Optional[dt.datetime]) -> Optional[str]:
    return value.isoformat() if isinstance(value, dt.datetime) else None


def _parse_hours_to_start_end(hours: int) -> dt.timedelta:
    return dt.timedelta(hours=hours)


# FastAPI lifespan initialises connection pool and default site once per process
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise shared resources (environment, database pool)."""

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required")

    min_conn = int(os.getenv("DB_MIN_CONNECTIONS", DEFAULT_DB_MIN_CONN))
    max_conn = max(min_conn, int(os.getenv("DB_MAX_CONNECTIONS", DEFAULT_DB_MAX_CONN)))

    pool = SimpleConnectionPool(min_conn, max_conn, dsn=database_url)

    # Verify the connection eagerly so we fail fast on startup issues.
    test_conn = pool.getconn()
    try:
        with test_conn.cursor() as cur:
            cur.execute("SELECT 1")
    finally:
        pool.putconn(test_conn)

    app.state.db_pool = pool
    app.state.default_site = os.getenv("SITE_NAME", "chicago_il")

    try:
        yield
    finally:
        pool.closeall()


def get_db_conn(request: Request) -> Generator[psycopg2.extensions.connection, None, None]:
    """Provide a database connection from the pool for each request."""

    pool: SimpleConnectionPool = request.app.state.db_pool
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


# Helper query to list available ingestion sites for dropdowns
def fetch_sites(conn: psycopg2.extensions.connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT site FROM raw_weather ORDER BY site ASC")
        return [row[0] for row in cur.fetchall()]


# Summary aggregation reused across endpoints to avoid duplication
def fetch_weather_summary(
    conn: psycopg2.extensions.connection, site: Optional[str], *, table: str = "fact_weather"
) -> dict[str, Any]:
    sql = """
        SELECT COUNT(*) AS row_count,
               MIN(ts_utc) AS first_ts,
               MAX(ts_utc) AS latest_ts
        FROM {table}
    """.format(table=table)
    params: tuple[Any, ...] = ()
    if site:
        sql += " WHERE site = %s"
        params = (site,)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    row_count = int(row["row_count"]) if row and row["row_count"] is not None else 0
    return {
        "row_count": row_count,
        "first_ts": _isoformat(row["first_ts"]) if row_count else None,
        "latest_ts": _isoformat(row["latest_ts"]) if row_count else None,
    }


# Pull recent hourly silver rows for charts
def fetch_hourly_rows(
    conn: psycopg2.extensions.connection, site: str, hours: int
) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT site, ts_utc, ghi_wm2, temp_c, wind_mps
            FROM fact_weather
            WHERE site = %s
            ORDER BY ts_utc DESC
            LIMIT %s
            """,
            (site, hours),
        )
        rows = cur.fetchall()

    rows.reverse()  # Present data chronologically (oldest -> newest)
    return [
        {
            "site": row["site"],
            "ts_utc": _isoformat(row["ts_utc"]),
            "ghi_wm2": row["ghi_wm2"],
            "temp_c": row["temp_c"],
            "wind_mps": row["wind_mps"],
        }
        for row in rows
    ]


# Pull raw bronze rows so the UI can show before/after comparisons
def fetch_raw_rows(
    conn: psycopg2.extensions.connection, site: str, hours: int
) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT site, ts_utc, ghi_wm2, t2m_c, ws10_mps, ingested_at
            FROM raw_weather
            WHERE site = %s
            ORDER BY ts_utc DESC
            LIMIT %s
            """,
            (site, hours),
        )
        rows = cur.fetchall()

    rows.reverse()
    return [
        {
            "site": row["site"],
            "ts_utc": _isoformat(row["ts_utc"]),
            "ghi_wm2": row["ghi_wm2"],
            "t2m_c": row["t2m_c"],
            "ws10_mps": row["ws10_mps"],
            "ingested_at": _isoformat(row["ingested_at"]),
        }
        for row in rows
    ]


app = FastAPI(title="Climate API", version="0.3.0", lifespan=lifespan)


@app.get("/")
def root(
    request: Request,
    conn: psycopg2.extensions.connection = Depends(get_db_conn),
    site: Optional[str] = None,
) -> dict[str, Any]:
    """Return API greeting plus quick summaries of warehouse contents."""

    target_site = site or request.app.state.default_site
    sites = fetch_sites(conn)
    fact_summary = fetch_weather_summary(conn, target_site)
    raw_summary = fetch_weather_summary(conn, target_site, table="raw_weather")

    return {
        "status": "ok",
        "message": "Welcome to the Climate API",
        "site": target_site,
        "sites": sites,
        "summary": fact_summary,
        "raw_summary": raw_summary,
    }


@app.get("/health")
def healthcheck() -> dict[str, str]:
    """Expose a simple container liveness probe."""

    return {"status": "healthy"}


@app.get("/weather/sites")
def list_sites(
    conn: psycopg2.extensions.connection = Depends(get_db_conn),
) -> dict[str, list[str]]:
    """Return the list of sites that currently have raw weather data."""

    return {"sites": fetch_sites(conn)}


@app.get("/weather/hourly")
def weather_hourly(
    request: Request,
    conn: psycopg2.extensions.connection = Depends(get_db_conn),
    site: Optional[str] = Query(None, description="Site identifier (defaults to SITE_NAME)"),
    hours: int = Query(24, ge=1, le=336, description="Number of hours to return"),
) -> dict[str, Any]:
    """Return the most recent hourly fact_weather rows for the requested site."""

    target_site = site or request.app.state.default_site
    sites = fetch_sites(conn)
    if sites and target_site not in sites:
        raise HTTPException(status_code=404, detail=f"Unknown site '{target_site}'")

    rows = fetch_hourly_rows(conn, target_site, hours)
    summary = fetch_weather_summary(conn, target_site)

    return {
        "site": target_site,
        "hours": hours,
        "rows": rows,
        "summary": summary,
    }


@app.get("/weather/raw")
def weather_raw(
    request: Request,
    conn: psycopg2.extensions.connection = Depends(get_db_conn),
    site: Optional[str] = Query(None, description="Site identifier (defaults to SITE_NAME)"),
    hours: int = Query(24, ge=1, le=336, description="Number of hours to return"),
) -> dict[str, Any]:
    """Return the most recent raw_weather rows for the requested site."""

    target_site = site or request.app.state.default_site
    sites = fetch_sites(conn)
    if sites and target_site not in sites:
        raise HTTPException(status_code=404, detail=f"Unknown site '{target_site}'")

    rows = fetch_raw_rows(conn, target_site, hours)
    summary = fetch_weather_summary(conn, target_site, table="raw_weather")

    return {
        "site": target_site,
        "hours": hours,
        "rows": rows,
        "summary": summary,
    }


# Combine raw vs silver counts for quick KPIs
@app.get("/weather/metrics")
def weather_metrics(
    request: Request,
    conn: psycopg2.extensions.connection = Depends(get_db_conn),
    site: Optional[str] = Query(None, description="Site identifier (defaults to SITE_NAME)"),
) -> dict[str, Any]:
    """Return aggregate counts comparing raw and fact tables for a site."""

    target_site = site or request.app.state.default_site
    sites = fetch_sites(conn)
    if sites and target_site not in sites:
        raise HTTPException(status_code=404, detail=f"Unknown site '{target_site}'")

    raw_summary = fetch_weather_summary(conn, target_site, table="raw_weather")
    fact_summary = fetch_weather_summary(conn, target_site, table="fact_weather")
    raw_rows = raw_summary["row_count"]
    fact_rows = fact_summary["row_count"]
    kept_pct = (fact_rows / raw_rows * 100.0) if raw_rows else None
    dropped_rows = raw_rows - fact_rows if raw_rows else 0

    return {
        "site": target_site,
        "raw": raw_summary,
        "fact": fact_summary,
        "dropped_rows": max(dropped_rows, 0),
        "kept_percentage": kept_pct,
    }


@app.get("/streamlit-proxy")
async def streamlit_proxy() -> dict[str, str]:
    """Confirm the dashboard is reachable from the API container."""

    streamlit_url = os.getenv("STREAMLIT_SERVER_URL", "http://streamlit:8501")

    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS) as client:
            response = await client.get(streamlit_url)
            response.raise_for_status()
    except Exception as exc:  # pylint: disable=broad-except
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"status": "available", "streamlit_url": streamlit_url}
