"""NASA POWER hourly ingestion utilities."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# ---- Config helpers ---------------------------------------------------------


# Retrieve environment variables with optional defaults, raising if missing
def env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


# ---- NASA POWER fetch & parse -----------------------------------------------

POWER_BASE = "https://power.larc.nasa.gov/api/temporal/hourly/point"
PARAMS = ["ALLSKY_SFC_SW_DWN", "T2M", "WS10M"]  # GHI, temperature, wind speed


# Compose the NASA POWER endpoint with consistent query parameters
def build_power_url(lat: float, lon: float, start_yyyymmdd: str, end_yyyymmdd: str) -> str:
    query = {
        "parameters": ",".join(PARAMS),
        "community": "RE",
        "longitude": lon,
        "latitude": lat,
        "start": start_yyyymmdd,
        "end": end_yyyymmdd,
        "format": "JSON",
        "time-standard": "UTC",
    }
    query_string = "&".join(f"{key}={query[key]}" for key in query)
    return f"{POWER_BASE}?{query_string}"


# Execute the HTTP request and raise if NASA responds with an error
def fetch_power(lat: float, lon: float, start_yyyymmdd: str, end_yyyymmdd: str) -> dict:
    url = build_power_url(lat, lon, start_yyyymmdd, end_yyyymmdd)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


# Normalise NASA parameter structures into a timestamp-keyed dictionary
def _series_from_param(param_dict: Dict) -> Dict[dt.datetime, Optional[float]]:
    """Normalise multiple parameter formats into a timestamp->value mapping."""

    series: Dict[dt.datetime, Optional[float]] = {}
    for key, value in param_dict.items():
        if isinstance(value, list):
            date = dt.datetime.strptime(str(key), "%Y%m%d").date()
            for hour, hourly_value in enumerate(value):
                ts = dt.datetime(date.year, date.month, date.day, hour, tzinfo=dt.timezone.utc)
                series[ts] = None if hourly_value is None else float(hourly_value)
        else:
            key_str = str(key)
            if len(key_str) >= 10:
                try:
                    ts = dt.datetime.strptime(key_str[:10], "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)
                except ValueError:
                    continue
                series[ts] = None if value is None else float(value)
    return series


# Extract only the parameters we care about and convert to time-indexed series
def parse_power_json(payload: dict) -> Dict[str, Dict[dt.datetime, Optional[float]]]:
    properties = payload.get("properties", {})
    params = properties.get("parameter", {})

    parsed: Dict[str, Dict[dt.datetime, Optional[float]]] = {}
    for parameter in PARAMS:
        parsed[parameter] = _series_from_param(params.get(parameter, {}))
    return parsed


# Align all parameters on the same timestamp so we can bulk insert rows
def merge_params_to_rows(
    site: str,
    series_map: Dict[str, Dict[dt.datetime, Optional[float]]],
) -> List[Tuple[str, dt.datetime, Optional[float], Optional[float], Optional[float], Dict[str, Optional[float]]]]:
    """Align timestamps across parameters and construct DB row tuples."""

    timestamps: set[dt.datetime] = set()
    for series in series_map.values():
        timestamps.update(series.keys())

    rows = []
    for ts in sorted(timestamps):
        ghi = series_map["ALLSKY_SFC_SW_DWN"].get(ts)
        t2m = series_map["T2M"].get(ts)
        ws = series_map["WS10M"].get(ts)
        raw = {"source": "NASA_POWER", "ghi_wm2": ghi, "t2m_c": t2m, "ws10_mps": ws}
        rows.append((site, ts, ghi, t2m, ws, raw))
    return rows


# ---- DB write ----------------------------------------------------------------


# Lazily create a new psycopg2 connection using DATABASE_URL
def get_conn():
    dsn = env("DATABASE_URL")
    return psycopg2.connect(dsn)


def _jsonify_rows(rows: Iterable[Tuple[str, dt.datetime, Optional[float], Optional[float], Optional[float], Dict[str, Optional[float]]]]):
    for site, ts, ghi, t2m, ws, raw in rows:
        yield (site, ts, ghi, t2m, ws, psycopg2.extras.Json(raw))


# Use execute_values for efficiency and UPSERT to keep reruns idempotent
def bulk_upsert_raw_weather(rows: List[Tuple[str, dt.datetime, Optional[float], Optional[float], Optional[float], Dict[str, Optional[float]]]]):
    if not rows:
        return 0

    sql = """
    INSERT INTO raw_weather (site, ts_utc, ghi_wm2, t2m_c, ws10_mps, raw_json)
    VALUES %s
    ON CONFLICT (site, ts_utc) DO UPDATE
    SET ghi_wm2 = EXCLUDED.ghi_wm2,
        t2m_c   = EXCLUDED.t2m_c,
        ws10_mps= EXCLUDED.ws10_mps,
        raw_json = EXCLUDED.raw_json;
    """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(
                cursor,
                sql,
                _jsonify_rows(rows),
                template="(%s,%s,%s,%s,%s,%s)",
                page_size=1000,
            )
    return len(rows)


# ---- CLI ---------------------------------------------------------------------


def yyyymmdd_to_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y%m%d").date()


# Fetch data in manageable windows, clean it, then write to the Bronze table
def run_ingest(lat: float, lon: float, site: str, start: str, end: str, chunk_days: int = 7):
    start_date = yyyymmdd_to_date(start)
    end_date = yyyymmdd_to_date(end)

    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    total_inserted = 0
    cursor = start_date
    while cursor <= end_date:
        chunk_end = min(cursor + dt.timedelta(days=chunk_days - 1), end_date)
        payload = fetch_power(lat, lon, cursor.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"))
        series_map = parse_power_json(payload)
        rows = merge_params_to_rows(site, series_map)
        total_inserted += bulk_upsert_raw_weather(rows)
        cursor = chunk_end + dt.timedelta(days=1)

    print(f"[nasa] inserted/updated rows: {total_inserted}")


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Ingest NASA POWER hourly data into raw_weather")
    parser.add_argument("--site", default=os.getenv("SITE_NAME", "chicago_il"))
    parser.add_argument("--lat", type=float, default=float(os.getenv("SITE_LAT", "41.8781")))
    parser.add_argument("--lon", type=float, default=float(os.getenv("SITE_LON", "-87.6298")))
    parser.add_argument("--start", required=True, help="Start date (YYYYMMDD)")
    parser.add_argument("--end", required=True, help="End date (YYYYMMDD)")
    parser.add_argument("--chunk-days", type=int, default=7, help="Number of days per API request")
    args = parser.parse_args()

    run_ingest(args.lat, args.lon, args.site, args.start, args.end, args.chunk_days)


if __name__ == "__main__":
    main()
