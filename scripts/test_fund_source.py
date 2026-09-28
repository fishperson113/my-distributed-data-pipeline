"""Fetch raw intraday ETF/fund bars from SSI without running Dagster."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from data_pipeline.config import InfrastructureSettings, PipelineConfig, load_pipeline_config
from data_pipeline.ingestion.common.raw_output import write_raw_extraction
from data_pipeline.ingestion.fund import extract_fund_intraday


def _default_date() -> str:
    """Probe the previous day, which is the partition the schedule crawls."""

    return (date.today() - timedelta(days=1)).isoformat()


def build_parser(
    infrastructure: InfrastructureSettings, policy: PipelineConfig
) -> argparse.ArgumentParser:
    """Build the fund source-test CLI parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=policy.fund.symbol)
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
        "--timeout", type=float, default=infrastructure.http_timeout_seconds
    )
    parser.add_argument(
        "--attempts", type=int, default=infrastructure.http_max_attempts
    )
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    """Run an SSI extraction and persist its raw JSON envelope."""

    infrastructure = InfrastructureSettings.from_env()
    policy = load_pipeline_config()
    args = build_parser(infrastructure, policy).parse_args()
    extraction = extract_fund_intraday(
        symbol=args.symbol,
        partition_date=args.date,
        granularity_minutes=args.granularity,
        start_time=args.start_time,
        end_time=args.end_time,
        retention_days=policy.intraday.retention_days,
        timezone_name=policy.daily_partition.timezone,
        timeout_seconds=args.timeout,
        max_attempts=args.attempts,
        endpoint_url=infrastructure.ssi_history_url,
    )
    output = args.output or infrastructure.raw_storage_path / Path(
        f"ssi/{args.symbol.upper()}_{args.granularity}m_{args.date}.json"
    )
    write_raw_extraction(extraction, output)
    print(f"fund crawl succeeded: records={extraction.record_count} output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
