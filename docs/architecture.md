# Phase 1 Architecture

The repository is deployed as a self-hosted Dagster application on a personal VPS.

```text
dagster-webserver ─┐
                   ├── dagster-code (gRPC) ── data_pipeline.definitions
dagster-daemon ────┘
        │
        └── postgres (Dagster metadata)
```

## Services

- `postgres` stores Dagster run, event-log, and schedule metadata.
- `dagster-code` loads the repository's Python `Definitions` over gRPC.
- `dagster-webserver` serves the UI and launches runs.
- `dagster-daemon` provides the production daemon topology required by future schedules and sensors.

The three Dagster services use the same application image. They are separated by process responsibility and communicate over the internal Compose network.

## Persistence

- `dagster-postgres-data` preserves Dagster metadata.
- `dagster-compute-logs` preserves compute logs shared by the Dagster services.

Future Bronze data will use a separate database or schema boundary and is not part of Phase 1.

## Security boundary

The Phase 1 Compose stack does not provide authentication or TLS for the Dagster UI. On the VPS, restrict access with a firewall, private network, or SSH tunnel until a reverse proxy and authentication layer are explicitly added.

