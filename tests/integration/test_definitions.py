"""Tests for the Dagster code-location boundary."""

import dagster as dg

from data_pipeline.assets.healthcheck import deployment_healthcheck
from data_pipeline.definitions import defs


def test_definitions_are_loadable() -> None:
    """Validate that Dagster can load the repository definitions."""

    dg.Definitions.validate_loadable(defs)


def test_healthcheck_asset_is_registered() -> None:
    """Ensure the deployment healthcheck is visible to Dagster."""

    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()
    assert dg.AssetKey("deployment_healthcheck") in asset_keys


def test_healthcheck_asset_materializes_locally() -> None:
    """Exercise the asset without external services."""

    result = dg.materialize([deployment_healthcheck])
    assert result.success
