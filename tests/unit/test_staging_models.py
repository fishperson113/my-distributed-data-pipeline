"""Guards on the staging SQL that a database test would only catch in CI.

A single pre-conversion Bronze row once failed the entire dbt build on the VPS.
The staging models now skip rows they cannot type, and that filter is easy to
drop by accident while editing the SQL, so its presence is asserted here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

STAGING_DIR = Path("src/data_pipeline/dbt/models/staging")
STAGING_MODELS = ("stg_vnstock__stock_daily.sql", "stg_ssi__fund_daily.sql")


@pytest.mark.parametrize("model", STAGING_MODELS)
def test_staging_skips_rows_without_an_instant(model: str) -> None:
    sql = (STAGING_DIR / model).read_text(encoding="utf-8")

    assert "payload ? 'ts_epoch'" in sql, (
        f"{model} must skip payloads with no ts_epoch, or one pre-conversion "
        "row will fail the not_null tests and block the whole dbt build."
    )


@pytest.mark.parametrize("model", STAGING_MODELS)
def test_staging_keeps_only_the_newest_version_of_each_instant(model: str) -> None:
    sql = (STAGING_DIR / model).read_text(encoding="utf-8")

    assert "version_rank = 1" in sql
    assert "ts_epoch" in sql
