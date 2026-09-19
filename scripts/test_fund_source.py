"""Fetch raw daily ETF/fund rows from SSI without running Dagster."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from data_pipeline.config import InfrastructureSettings, PipelineConfig, load_pipeline_config
from data_pipeline.ingestion.common.raw_output import write_raw_extraction
from data_pipeline.ingestion.fund import extract_fund_daily


def _default_dates() -> tuple[str, str]:
    end = date.today()
    return (end - timedelta(days=30)).isoformat(), end.isoformat()


def build_parser(
    infrastructure: InfrastructureSettings, policy: PipelineConfig
) -> argparse.ArgumentParser:
    """Build the fund source-test CLI parser."""

    default_start, default_end = _default_dates()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=policy.fund.symbol)
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
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
    extraction = extract_fund_daily(
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        timeout_seconds=args.timeout,
        max_attempts=args.attempts,
        endpoint_url=infrastructure.ssi_history_url,
    )
    output = args.output or infrastructure.raw_storage_path / Path(
        f"ssi/{args.symbol.upper()}_daily_{args.start}_{args.end}.json"
    )
    write_raw_extraction(extraction, output)
    print(f"fund crawl succeeded: records={extraction.record_count} output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
