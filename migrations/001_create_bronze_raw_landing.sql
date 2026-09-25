CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.ingestion_batch (
    batch_id UUID PRIMARY KEY,
    source_name TEXT NOT NULL,
    dataset_name TEXT NOT NULL CHECK (dataset_name IN ('stock_daily', 'fund_daily')),
    provider_name TEXT NOT NULL,
    partition_key TEXT,
    requested_from DATE,
    requested_to DATE,
    request_metadata JSONB NOT NULL,
    dagster_run_id TEXT,
    raw_object_path TEXT NOT NULL,
    payload_checksum CHAR(64) NOT NULL,
    schema_version TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed', 'partial')),
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    error_message TEXT,
    CHECK (requested_to IS NULL OR requested_from IS NULL OR requested_to >= requested_from),
    UNIQUE (source_name, dataset_name, partition_key, payload_checksum)
);

CREATE TABLE IF NOT EXISTS bronze.stock (
    stock_record_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES bronze.ingestion_batch(batch_id),
    source_record_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_checksum CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_id, source_record_key)
);

CREATE TABLE IF NOT EXISTS bronze.fund (
    fund_record_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES bronze.ingestion_batch(batch_id),
    source_record_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_checksum CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_id, source_record_key)
);
