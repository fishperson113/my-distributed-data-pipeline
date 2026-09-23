"""Mirror storage/raw JSON envelopes into MongoDB (Bronze raw landing)."""

from __future__ import annotations

import argparse
from pathlib import Path

from data_pipeline.config import InfrastructureSettings
from data_pipeline.warehouse.mongo_sink import load_raw_files_to_mongo
from data_pipeline.warehouse.settings import WarehouseSettings


def main() -> int:
    """Upsert selected raw extraction JSON files from storage/raw into Mongo."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        action="append",
        type=Path,
        help="JSON file or directory to scan; repeat for multiple paths.",
    )
    args = parser.parse_args()

    infrastructure = InfrastructureSettings.from_env()
    warehouse = WarehouseSettings.from_env()
    upserted = load_raw_files_to_mongo(
        raw_storage_path=infrastructure.raw_storage_path,
        mongo_uri=warehouse.mongo_uri,
        mongo_database=warehouse.mongo_database,
        json_paths=args.path,
    )
    print(f"mongo load succeeded: upserted={upserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
