# Local Development

## Prerequisites

- Python 3.11
- `uv`
- Docker with Docker Compose

## Python environment

```bash
uv sync
uv run pytest
```

## Fast local Dagster loop

For Python-only development, run:

```bash
uv run dagster dev -m data_pipeline.definitions
```

This mode uses a local temporary Dagster instance and does not reproduce the production PostgreSQL topology.

## Production-shaped local stack

```bash
cp .env.example .env
```

Change `POSTGRES_PASSWORD` in `.env`, then run:

```bash
docker compose up --build -d
docker compose ps
```

Open `http://localhost:3000`, materialize `deployment_healthcheck`, and confirm the run succeeds.

Stop services without deleting persistent data:

```bash
docker compose down
```

Do not use `docker compose down --volumes` unless deleting local Dagster history is intentional.

