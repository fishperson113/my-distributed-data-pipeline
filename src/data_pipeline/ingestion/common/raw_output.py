"""Raw extraction output helpers."""

from __future__ import annotations

import json
from pathlib import Path

from data_pipeline.ingestion.common.models import RawExtraction


def write_raw_extraction(extraction: RawExtraction, output_path: Path) -> Path:
    """Write an extraction atomically as UTF-8 JSON."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temporary_path.write_text(
        json.dumps(extraction.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(output_path)
    return output_path

