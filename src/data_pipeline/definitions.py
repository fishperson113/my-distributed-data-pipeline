"""Canonical Dagster code-location entry point."""

import dagster as dg

from data_pipeline.assets.healthcheck import deployment_healthcheck


defs = dg.Definitions(assets=[deployment_healthcheck])

