"""Entrypoint for the Climate API service used in the docker-compose stack."""

from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, HTTPException
import httpx

# Bounding timeout for outbound requests to the Streamlit service
API_TIMEOUT_SECONDS = 10


@asynccontextmanager
def lifespan(_: FastAPI):
    """Hook for startup/shutdown events (e.g. database connections)."""
    # No resources yet, but keeping the structure makes future wiring trivial
    yield


app = FastAPI(title="Climate API", version="0.1.0", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    """Return a human-friendly welcome payload."""
    return {"status": "ok", "message": "Welcome to the Climate API"}


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    """Expose a simple container liveness probe."""
    return {"status": "healthy"}


@app.get("/streamlit-proxy")
async def streamlit_proxy() -> dict[str, str]:
    """Confirm the dashboard is reachable from the API container."""
    streamlit_url = os.getenv("STREAMLIT_SERVER_URL", "http://streamlit:8501")

    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS) as client:
            response = await client.get(streamlit_url)
            response.raise_for_status()
    except Exception as exc:  # pylint: disable=broad-except
        # Convert transport errors into an HTTP 503 for downstream clients
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {"status": "available", "streamlit_url": streamlit_url}
