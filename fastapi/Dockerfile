# syntax=docker/dockerfile:1.6
# Build the FastAPI image starting from a slim Python base
FROM python:3.11-slim AS base

# Avoid writing .pyc files and ensure immediate stdout flushing
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# All application assets live under /app inside the container
WORKDIR /app

# Install compilation toolchain required for some dependencies, then clean up
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency list first to leverage Docker layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Bring the FastAPI source code into the image
COPY app ./app

# Document the port uvicorn will bind to
EXPOSE 8000

# Launch the ASGI server when the container starts
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
