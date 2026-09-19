"""Fetch raw daily stock rows from vnstock without running Dagster."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from data_pipeline.config import InfrastructureSettings, PipelineConfig, load_pipeline_config
from data_pipeline.ingestion.common.raw_output import write_raw_extraction
from data_pipeline.ingestion.stock import extract_stock_daily


def _default_dates() -> tuple[str, str]:
    end = date.today()
    return (end - timedelta(days=30)).isoformat(), end.isoformat()


def build_parser(
    infrastructure: InfrastructureSettings, policy: PipelineConfig
) -> argparse.ArgumentParser:
    """Build the stock source-test CLI parser."""

    default_start, default_end = _default_dates()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", action="append", dest="symbols", default=[])
    parser.add_argument("--start", default=default_start)
    parser.add_argument("--end", default=default_end)
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
    extraction = extract_stock_daily(
        symbols=symbols,
        start=args.start,
        end=args.end,
        provider=args.provider,
    )
    output = args.output or infrastructure.raw_storage_path / Path(
        f"vnstock/stock_daily_{args.start}_{args.end}.json"
    )
    write_raw_extraction(extraction, output)
    print(f"stock crawl succeeded: records={extraction.record_count} output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
