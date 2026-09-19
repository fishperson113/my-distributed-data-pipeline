"""Deployment verification asset."""

from datetime import UTC, datetime

import dagster as dg


@dg.asset(
    group_name="platform",
    description="Verifies that the deployed Dagster code location can execute an asset.",
)
def deployment_healthcheck() -> dg.MaterializeResult:
    """Return deployment metadata without touching external systems."""

    checked_at = datetime.now(UTC).isoformat()
    return dg.MaterializeResult(
        metadata={
            "status": "ok",
            "checked_at": checked_at,
            "application": "my-distributed-data-pipeline",
        }
    )

