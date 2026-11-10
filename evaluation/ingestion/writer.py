"""Use the public authenticated API with native extraction and update semantics."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from jiuwen_memory.api import MemoryAPI, RequestSecurityContext

from .records import SourceRecord, digest, timestamp


def pipeline_fingerprint(spec: Mapping[str, Any]) -> str:
    # Caller identifies the effective pipeline, code revision, and persistent store.
    if not all(spec.get(key) for key in ("revision", "storage_id", "config")):
        raise ValueError("pipeline spec requires revision, storage_id and effective config")
    return digest({"ingestion_schema": 1, "infer": True, "pipeline": dict(spec)})


class MemoryAPIWriter:
    def __init__(self, api: MemoryAPI,
                 security_provider: Callable[[], RequestSecurityContext],
                 pipeline_spec: Mapping[str, Any], *, close: Callable[[], None] | None = None):
        self.api = api
        self.security_provider = security_provider
        self.fingerprint = pipeline_fingerprint(pipeline_spec)
        self._close = close

    def write(self, record: SourceRecord) -> list[str]:
        units = self.api.add(
            record.content, record.scope, security=self.security_provider(),
            occurred_at=timestamp(record.observed_at),
            system_metadata={"infer": True, "observation_date": record.observed_at[:10],
                             "project": record.project},
            user_metadata={"ingestion_record_id": record.record_id,
                           "source_session": record.source_session},
        )
        # add blocks through construction/dedup/update/indexing. Empty extraction is visible.
        return [unit.id for unit in units]

    def close(self) -> None:
        if self._close is not None:
            self._close()
