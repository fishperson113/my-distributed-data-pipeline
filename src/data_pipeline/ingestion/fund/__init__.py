"""Fund/ETF extraction through SSI iBoard."""

from data_pipeline.ingestion.fund.client import extract_fund_intraday

__all__ = ["extract_fund_intraday"]
