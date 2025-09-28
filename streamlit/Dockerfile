# syntax=docker/dockerfile:1.6
# Build the Streamlit image from the lightweight Python base
FROM python:3.11-slim

# Ensure Python behaves consistently in containers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# All subsequent actions happen from /app
WORKDIR /app

# Install compilation tooling needed by some Python wheels, then clean cache
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifest first for layer caching and install packages
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Bring in the remainder of the Streamlit project
COPY . .

# Expose Streamlit's default port
EXPOSE 8501

# Launch the dashboard bound to all interfaces
CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
