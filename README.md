# AI-Driven Smart Grid Energy Data Pipeline and Forecasting

This repository hosts a Streamlit dashboard (and future supporting services) for an AI‑driven smart grid energy data pipeline and forecasting platform.

## Components (current)

- `streamlit/app.py` — Front-end dashboard querying a FastAPI backend (expected at `API_BASE_URL`).

## Quick Start (Local)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
API_BASE_URL=http://localhost:8000 streamlit run streamlit/app.py --server.headless true
```

If you don't have the FastAPI backend running yet, the dashboard will show error messages in the health sections but will still load.

## Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `API_BASE_URL` | Base URL of the FastAPI service the dashboard queries | `http://fastapi:8000` |

## Testing

```bash
pytest -q
```

## CI

GitHub Actions workflow runs on every push / PR:
1. Installs dependencies
2. Runs tests
3. Boots Streamlit headlessly and curls the root page as a smoke check

## Next Ideas

- Add FastAPI backend service
- Add forecasting model service (Prophet / XGBoost / LSTM)
- Docker multi-service compose stack
- Deployment workflow (Streamlit Community Cloud or container registry)

## License

Add a license (MIT recommended) — not yet included.
# ClimateProject Stack

## Getting started

1. Copy .env.example to .env and tailor the values (especially database credentials).
2. Build and start the stack:
   `ash
   docker compose up --build
   `
3. Access the services:
   - FastAPI: http://localhost:8000/docs
   - Streamlit: http://localhost:8501

## Service layout

- docker-compose.yml wires Postgres, FastAPI, and Streamlit services.
- astapi/ contains the API app and Docker build context.
- streamlit/ contains the Streamlit dashboard and Docker build context.

## Development notes

- Hot reloading is enabled via the bind mounts in docker-compose.yml. Edit the local source files to see changes without rebuilding images.
- For database tooling, connect to Postgres on localhost:5432 using the credentials defined in your .env file.
