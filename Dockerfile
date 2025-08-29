FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Copy the equity-aggregator project
COPY . /app

# sync the uv environment; asserting lockfile up-to-date
RUN uv sync --locked

# Use System Python Environment by default
ENV UV_SYSTEM_PYTHON=1

# Set the environment file for equity-aggregator
ENV UV_ENV_FILE=".env"

# Aggregate and seed canonical equities database
CMD ["uv", "run", "equity-aggregator", "seed"]
