# My Distributed Data Pipeline

Self-hosted Dagster application for a multi-source data pipeline running on a personal VPS.

## Current phase

Phase 1 establishes the deployment foundation only:

- Dagster webserver, daemon, and gRPC code server.
- PostgreSQL-backed Dagster metadata.
- One deployment healthcheck asset.
- Docker Compose deployment for local verification and the VPS.

Stock ingestion, fund ingestion, Bronze migrations, notebooks, and dbt are intentionally deferred.

## Local setup

```bash
uv sync
uv run pytest
```

Run Dagster without containers:

```bash
uv run dagster dev -m data_pipeline.definitions
```

Run the production-shaped stack locally:

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Dagster UI is available at `http://localhost:3000` by default.

## Documentation

- [Implementation plan](implementation_plan.md)
- [Architecture](docs/architecture.md)
- [Local development](docs/local-development.md)
- [VPS deployment](docs/vps-deployment.md)
- [Module 4 proposal](module-4-proposal.md)
- [Bronze design review](module-4-bronze-design-review.md)

