FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Copy the packages
COPY packages/database /app/packages/database
COPY packages/data-aggregator /app/packages/data-aggregator

# Use System Python Environment by default
ENV UV_SYSTEM_PYTHON=1

# Set the environment file for data-aggregator
ENV UV_ENV_FILE="/app/packages/data-aggregator/.env"

# Install database and data-aggregator
RUN uv pip install -e /app/packages/database /app/packages/data-aggregator

# Run data-aggregator
CMD ["uv", "run", "data-aggregator"]