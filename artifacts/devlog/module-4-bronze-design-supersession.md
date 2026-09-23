# Module 4 Bronze Design Supersession

## What changed

`module-4-bronze-design-review.md` recommended a Postgres-only Bronze: a shared `ingestion_batch` table plus `bronze.stock` and `bronze.fund` JSONB tables, with dbt connecting directly to that Postgres instance.

The Bronze layer actually implemented for local ELT development uses MongoDB instead of Postgres:

- MongoDB holds dataset-aligned raw record collections (`stock_daily`, `fund_daily`), mirrored from `storage/raw` JSON envelopes.
- DuckDB reads those Mongo collections into `raw.*` tables and runs dbt staging/marts models against them.
- A separate warehouse Postgres instance receives the finished dbt marts as a serving layer.
- Dagster's own Postgres, used only for Dagster run/event-log metadata, stays untouched and does not host any Bronze data, matching `AGENTS.md`'s schema-isolation rule.

## Why

This was an explicit product decision, not a technical rejection of the reviewed design.
The per-record JSONB-payload pattern from the review still applies; it now lives in Mongo documents and DuckDB `JSON` columns instead of Postgres `JSONB` columns.
The `ingestion_batch` audit table and its append-only/versioning policy (Policy A) were not carried over.
The Mongo mirror upserts by a deterministic natural key (`symbol:trade_date`) instead, which is a simpler, lighter-weight idempotency model appropriate for the current "lightweight small ELT pipeline" scope.
Reintroducing batch-level audit lineage remains an option if reprocessing or source-correction history becomes a real requirement.

## Current state

See `docs/local-elt.md` for the concrete implementation: `storage/raw` JSON stays the source of truth, `src/data_pipeline/warehouse/` provides the Mongo/DuckDB/Postgres loaders, and `src/data_pipeline/dbt/` holds the dbt project.
This flow is standalone (mirrors the existing source probes) and is not yet wired into Dagster assets, jobs, or schedules.
