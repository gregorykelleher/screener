FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Copy the packages
COPY packages/database /app/packages/database
COPY packages/equity-aggregator /app/packages/equity-aggregator

# Use System Python Environment by default
ENV UV_SYSTEM_PYTHON=1

# Set the environment file for equity-aggregator
ENV UV_ENV_FILE="/app/packages/equity-aggregator/.env"

# Install database and equity-aggregator
RUN uv pip install -e /app/packages/database /app/packages/equity-aggregator

# Run equity-aggregator
CMD ["uv", "run", "equity-aggregator"]