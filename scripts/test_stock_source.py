"""Fetch raw intraday stock bars from vnstock without running Dagster."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from data_pipeline.config import InfrastructureSettings, PipelineConfig, load_pipeline_config
from data_pipeline.ingestion.common.raw_output import write_raw_extraction
from data_pipeline.ingestion.stock import extract_stock_intraday


def _default_date() -> str:
    """Probe the previous day, which is the partition the schedule crawls."""

    return (date.today() - timedelta(days=1)).isoformat()


def build_parser(
    infrastructure: InfrastructureSettings, policy: PipelineConfig
) -> argparse.ArgumentParser:
    """Build the stock source-test CLI parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", action="append", dest="symbols", default=[])
    parser.add_argument("--date", default=_default_date(), help="Partition day, YYYY-MM-DD.")
    parser.add_argument("--start-time", default=None, help="Exchange-local clock, e.g. 13:00.")
    parser.add_argument("--end-time", default=None, help="Exchange-local clock, e.g. 14:00.")
    parser.add_argument(
        "--granularity",
        type=int,
        default=policy.intraday.granularity_minutes,
        choices=(1, 5, 15, 30, 60),
    )
    parser.add_argument(
        "--provider", default=policy.stock.provider, choices=("kbs", "vci")
    )
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    """Run a vnstock extraction and persist its raw JSON envelope."""

    infrastructure = InfrastructureSettings.from_env()
    policy = load_pipeline_config()
    args = build_parser(infrastructure, policy).parse_args()
    symbols = args.symbols or list(policy.stock.symbols)
    extraction = extract_stock_intraday(
        symbols=symbols,
        partition_date=args.date,
        granularity_minutes=args.granularity,
        provider=args.provider,
        start_time=args.start_time,
        end_time=args.end_time,
        retention_days=policy.intraday.retention_days,
        timezone_name=policy.daily_partition.timezone,
    )
    output = args.output or infrastructure.raw_storage_path / Path(
        f"vnstock/stock_{args.granularity}m_{args.date}.json"
    )
    write_raw_extraction(extraction, output)
    print(f"stock crawl succeeded: records={extraction.record_count} output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
