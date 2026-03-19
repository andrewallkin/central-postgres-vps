FROM python:3.11-slim

# Install Postgres client tools for pg_dump and timezone data
RUN apt-get update && \
    apt-get install -y postgresql-client gzip tzdata && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Create virtual environment and install dependencies with uv
RUN uv venv && \
    uv export --extra backup --no-dev --frozen > requirements.txt && \
    uv pip install --python .venv/bin/python -r requirements.txt && \
    rm requirements.txt

# Copy backup scripts
COPY backup ./backup

# Run the backup scheduler using uv
WORKDIR /app/backup
CMD ["uv", "run", "backup_script.py"]
