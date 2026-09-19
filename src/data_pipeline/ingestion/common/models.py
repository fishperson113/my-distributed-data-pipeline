"""Source-neutral extraction result contract."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class RawExtraction:
    """Represent one source extraction before Bronze persistence."""

    source: str
    dataset: str
    provider: str
    request: dict[str, Any]
    records: list[dict[str, Any]]
    source_payload: dict[str, Any] | list[Any] | None = None
    fetched_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def record_count(self) -> int:
        """Return the number of logical records extracted."""

        return len(self.records)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the extraction into a JSON-compatible dictionary."""

        payload = asdict(self)
        payload["record_count"] = self.record_count
        return payload

