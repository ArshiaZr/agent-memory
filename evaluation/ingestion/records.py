"""Validated source records; benchmark labels never enter the write contract."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from jiuwen_memory.api import Scope

MAX_RECORD_BYTES = 2 * 1024 * 1024


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("observed_at must be a timezone-aware ISO timestamp")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("observed_at must include a timezone")
    return result.astimezone(UTC)


@dataclass(frozen=True)
class SourceRecord:
    record_id: str
    content: str
    scope: Scope
    observed_at: str
    project: str = ""
    source_session: str = ""

    def payload(self) -> dict:
        return asdict(self)

    @classmethod
    def parse(cls, value: dict) -> SourceRecord:
        allowed = {"record_id", "content", "scope", "observed_at", "project", "source_session"}
        if not isinstance(value, dict) or set(value) - allowed:
            raise ValueError("source record contains unsupported fields (including QA labels)")
        for key in ("record_id", "content", "observed_at"):
            if not isinstance(value.get(key), str) or not value[key].strip():
                raise ValueError(f"{key} must be a nonempty string")
        coordinates = value.get("scope")
        scope_keys = {"org", "space", "user", "agent", "session"}
        if not isinstance(coordinates, dict) or set(coordinates) - scope_keys:
            raise ValueError("scope must contain only native Scope fields")
        if not coordinates or any(not isinstance(v, str) for v in coordinates.values()):
            raise ValueError("scope must contain string coordinates")
        for key in ("project", "source_session"):
            if key in value and not isinstance(value[key], str):
                raise ValueError(f"{key} must be a string")
        if len(value["record_id"]) > 512:
            raise ValueError("record_id exceeds 512 characters")
        if any(len(v) > 512 for v in coordinates.values()):
            raise ValueError("scope coordinate exceeds 512 characters")
        record = cls(value["record_id"], value["content"], Scope(**coordinates),
                     timestamp(value["observed_at"]).isoformat(),
                     value.get("project", ""), value.get("source_session", ""))
        if len(canonical(record.payload()).encode()) > MAX_RECORD_BYTES:
            raise ValueError("record exceeds 2 MiB; split at source turn boundaries")
        return record
