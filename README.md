# My Distributed Data Pipeline

Self-hosted Dagster application for a multi-source data pipeline running on a personal VPS.

## Current phase

Phase 1 establishes the deployment foundation only:

- Dagster webserver, daemon, and gRPC code server.
- PostgreSQL-backed Dagster metadata.
- One deployment healthcheck asset.
- Docker Compose deployment for local verification and the VPS.

Stock ingestion, fund ingestion, Bronze migrations, notebooks, and dbt are intentionally deferred.

Standalone source probes are available before database persistence and Dagster assets are introduced. They call reusable ingestion modules that future Dagster assets can invoke directly.

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

## Test the upstream data sources

Fetch daily stock OHLCV through `vnstock` (KBS by default):

```bash
uv run python scripts/test_stock_source.py --symbol FPT --start 2026-09-01 --end 2026-09-19
```

Use `--symbol` more than once to test multiple tickers, or pass `--provider vci` to test the VCI provider.

Fetch daily ETF data for `E1VFVN30` through the SSI public-facing history endpoint:

```bash
uv run python scripts/test_fund_source.py --symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19
```

The scripts write JSON envelopes under `storage/raw/vnstock/` and `storage/raw/ssi/`. Runtime data in these directories is ignored by Git.

Source defaults are managed in `.env`: vnstock provider and telemetry, SSI endpoint,
HTTP timeout/retry policy, and the raw storage root. Copy `.env.example` to `.env`
for local use. Explicit CLI arguments such as `--provider`, `--timeout`, `--attempts`,
and `--output` override those defaults for an individual run; symbols and date ranges
remain run parameters rather than environment configuration.

## Documentation

- [Implementation plan](artifacts/devlog/implementation_plan.md)
- [Architecture](docs/architecture.md)
- [Local development](docs/local-development.md)
- [VPS deployment](docs/vps-deployment.md)
- [Module 4 proposal](artifacts/devlog/module-4-proposal.md)
- [Bronze design review](artifacts/devlog/module-4-bronze-design-review.md)
