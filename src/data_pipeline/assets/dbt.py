"""dagster-dbt integration.

Exposes the dbt project as Dagster assets so the daily job runs
`dbt build` right after the Bronze batches land. The bronze dbt sources are
remapped onto the in-repo Bronze asset keys so lineage flows
bronze -> staging -> marts inside the Dagster UI.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


class BronzeDbtTranslator(DagsterDbtTranslator):
    """Map the dbt `bronze` sources onto the pipeline's Bronze asset keys."""

    _SOURCE_ASSET_KEYS = {
        "stock": dg.AssetKey(["bronze", "stock_daily"]),
        "fund": dg.AssetKey(["bronze", "fund_daily"]),
    }

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            mapped = self._SOURCE_ASSET_KEYS.get(dbt_resource_props.get("name"))
            if mapped is not None:
                return mapped
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=BronzeDbtTranslator(),
)
def dbt_market_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Build the dbt staging and marts models downstream of Bronze."""

    yield from dbt.cli(["build"], context=context).stream()
