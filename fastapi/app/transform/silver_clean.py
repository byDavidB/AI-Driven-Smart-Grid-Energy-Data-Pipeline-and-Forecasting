"""Bronze -> Silver transformer: raw_weather -> fact_weather."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


# -------- helpers ----------

# Convenience wrapper to load environment variables with optional defaults
def env(name: str, default: Optional[str] = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and value is None:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(env("DATABASE_URL", required=True))


def yyyymmdd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()


# -------- load raw ----------

RAW_SQL = """
SELECT site, ts_utc, ghi_wm2, t2m_c, ws10_mps, ingested_at
FROM raw_weather
WHERE site = %(site)s
  AND ts_utc >= %(start_ts)s
  AND ts_utc <  %(end_ts)s;
"""


# -------- write silver (upsert) ----------

UPSERT_SQL = """
INSERT INTO fact_weather (site, ts_utc, ghi_wm2, temp_c, wind_mps)
VALUES %s
ON CONFLICT (site, ts_utc) DO UPDATE
SET ghi_wm2 = EXCLUDED.ghi_wm2,
    temp_c  = EXCLUDED.temp_c,
    wind_mps= EXCLUDED.wind_mps;
"""


# Pull candidate Bronze rows for the requested site/date window
def fetch_raw(site: str, start_dt: dt.datetime, end_dt: dt.datetime) -> pd.DataFrame:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(RAW_SQL, {"site": site, "start_ts": start_dt, "end_ts": end_dt})
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=["site", "ts_utc", "ghi_wm2", "t2m_c", "ws10_mps", "ingested_at"])

    df = pd.DataFrame(rows, columns=["site", "ts_utc", "ghi_wm2", "t2m_c", "ws10_mps", "ingested_at"])
    df["ghi_wm2"] = pd.to_numeric(df["ghi_wm2"], errors="coerce")
    df["t2m_c"] = pd.to_numeric(df["t2m_c"], errors="coerce")
    df["ws10_mps"] = pd.to_numeric(df["ws10_mps"], errors="coerce")
    return df


# Apply quality rules and collapse multiple raw samples into hourly silver rows
def clean_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    # 1) ensure UTC tz
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)

    # 2) force top-of-hour bins (e.g., 14:37 -> 14:00)
    df["ts_hour"] = df["ts_utc"].dt.floor("h")

    # 3) validate ranges
    df.loc[df["ghi_wm2"] < 0, "ghi_wm2"] = pd.NA
    df.loc[df["t2m_c"] < -80, "t2m_c"] = pd.NA
    df.loc[df["t2m_c"] > 80, "t2m_c"] = pd.NA
    df.loc[df["ws10_mps"] < 0, "ws10_mps"] = pd.NA

    # 4) drop rows missing any critical value
    df = df.dropna(subset=["ghi_wm2", "t2m_c", "ws10_mps"])

    if df.empty:
        return df

    # 5) de-duplicate -> one row per (site, ts_hour) keeping latest ingested_at
    df = df.sort_values(["site", "ts_hour", "ingested_at"])
    df = df.groupby(["site", "ts_hour"], as_index=False).last()

    # Drop original ts_utc column; the floored timestamp becomes the canonical hour stamp
    df = df.drop(columns=["ts_utc"], errors="ignore")

    # 6) rename to silver schema columns
    df = df.rename(
        columns={
            "ts_hour": "ts_utc",
            "t2m_c": "temp_c",
            "ws10_mps": "wind_mps",
        }
    )[["site", "ts_utc", "ghi_wm2", "temp_c", "wind_mps"]]

    # 7) final sanity: hourly alignment and types
    ts_series = pd.to_datetime(df["ts_utc"], utc=True)
    assert (ts_series.dt.minute == 0).all(), "Non-hourly timestamp found"
    assert ts_series.dt.tz is not None, "Timestamps must be timezone-aware (UTC)"

    df["ts_utc"] = ts_series
    return df


# Load the cleaned rows into fact_weather, leveraging UPSERT for idempotency
def upsert_fact_weather(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, tuples, page_size=1000)
    return len(tuples)


# Orchestrate the end-to-end transform from Bronze window to Silver upsert
def run(site: str, start_str: str, end_str: str) -> None:
    start_dt = dt.datetime.combine(yyyymmdd(start_str), dt.time(0, 0, tzinfo=dt.timezone.utc))
    end_dt = dt.datetime.combine(yyyymmdd(end_str), dt.time(0, 0, tzinfo=dt.timezone.utc)) + dt.timedelta(days=1)

    raw = fetch_raw(site, start_dt, end_dt)
    cleaned = clean_to_hourly(raw)
    rows_written = upsert_fact_weather(cleaned)
    print(f"[silver_clean] site={site} rows_written={rows_written}")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Bronze -> Silver: raw_weather -> fact_weather")
    parser.add_argument("--site", default=os.getenv("SITE_NAME", "chicago_il"))
    parser.add_argument("--start", required=True, help="YYYYMMDD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYYMMDD (inclusive)")
    args = parser.parse_args()

    run(args.site, args.start, args.end)


if __name__ == "__main__":
    main()
