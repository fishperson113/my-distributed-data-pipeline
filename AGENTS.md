# Repository Guidelines

## Current scope

The repository is in Phase 1. Work should prioritize a reliable self-hosted Dagster deployment on the VPS.

Do not implement stock ingestion, fund ingestion, Bronze migrations, or dbt unless the active task explicitly expands the phase scope.

## Architecture

- `data_pipeline.definitions:defs` is the canonical Dagster code-location entry point.
- Dagster assets should be thin orchestration wrappers.
- Future ingestion modules must remain testable without importing Dagster.
- Dagster metadata and future pipeline data must not share a database schema.
- Secrets belong in environment variables and must not be committed.

## Verification

- Run `uv run pytest` after Python changes.
- Validate Compose with `docker compose config` after deployment configuration changes.
- Do not mark VPS-only checks complete until they have actually run on the VPS.

