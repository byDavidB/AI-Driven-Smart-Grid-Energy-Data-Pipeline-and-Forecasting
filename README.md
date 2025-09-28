# AI-Driven Smart Grid Energy Data Pipeline and Forecasting

This repository hosts a local development stack for an AI-driven smart grid energy data pipeline and forecasting platform. It includes a Streamlit dashboard, a FastAPI service (also home to ETL jobs), and a Postgres warehouse provisioned with Bronze/Silver/Gold tables.

## Repository layout

- `docker-compose.yml` - defines `db` (Postgres), `api` (FastAPI/ETL), and `streamlit` services.
- `infra/init.sql` - creates the raw/fact/mart tables on the first database start.
- `fastapi/` - FastAPI application code, ETL modules, Docker build context, and requirements.
- `streamlit/` - Streamlit dashboard code, Docker build context, and runtime requirements.
- `.env.example` - sample configuration; copy to `.env` for local use.
- `tests/` - pytest suite covering utility functions (e.g. Streamlit helpers).

## Quick start

```bash
cp .env.example .env
# (optional) tweak credentials or site coordinates in .env

docker compose down -v
docker compose up --build -d
```

Services:
- FastAPI docs: http://localhost:8000/docs
- Streamlit UI: http://localhost:8501
- Postgres: localhost:5432 (`db` from inside containers)

## NASA POWER ingest

```bash
# run inside the api container; adjust dates/site as needed
docker compose exec api python -m app.etl.nasa --start 20250101 --end 20250107
```

The CLI fetches hourly NASA POWER data, aligns the parameters, and upserts them into `raw_weather` (Bronze layer). Re-running the same window updates rows without duplication.

## Climate API endpoints

- `GET /weather/sites` - list the sites currently present in `raw_weather`.
- `GET /weather/hourly?site=...&hours=24` - return the most recent hourly data for the selected site (idempotent, supports up to 336 hours).
- `GET /` - summary payload including row counts and latest timestamps (used by the dashboard overview).

## Running tests

```bash
pytest -q
```

## Continuous Integration

`.github/workflows/ci.yml` installs dependencies, executes pytest, and runs a Streamlit smoke test on every push or pull request.

## Next steps

1. Build cleaners and feature jobs that populate Silver/Gold tables.
2. Expand the API with aggregate and forecasting endpoints.
3. Add richer dashboard visuals (forecasts, KPIs, comparisons).

## License

License to be added (MIT recommended).


## Silver cleaner

~~~bash
# run inside the api container; cleans Bronze -> Silver for the window
docker compose exec api python -m app.transform.silver_clean --start 20250101 --end 20250107
~~~

The transformer enforces hourly UTC timestamps, validates ranges, and upserts into fact_weather.

## Dashboard pages

1. **Data Health** — metrics tile (raw vs clean), drop reasons, completeness heatmap.
2. **Weather Trends** — raw vs cleaned line charts and minute histogram to verify hourly alignment.
3. **Schema & Lineage** — Sankey-style pipeline view and table descriptions.
