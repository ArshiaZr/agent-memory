"""Stream complete source histories. Never select by answer/session annotations."""
from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, tzinfo
from pathlib import Path

from jiuwen_memory.api import Scope

from .records import MAX_RECORD_BYTES, SourceRecord, digest


def jsonl_records(path: str | Path) -> Iterator[SourceRecord]:
    with Path(path).open("rb") as source:
        while True:
            line = source.readline(MAX_RECORD_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_RECORD_BYTES:
                raise ValueError("JSONL line exceeds 2 MiB")
            if not line.strip():
                raise ValueError("blank source record")
            yield SourceRecord.parse(json.loads(line))


def longmemeval_records(path: str | Path, *, run_id: str,
                       org: str = "memory-eval", agent: str = "jiuwen",
                       source_timezone: tzinfo = UTC) -> Iterator[SourceRecord]:
    """At most one sample resident; ijson parses the large top-level JSON array.

    Every session and message is used. Adjacent user+assistant messages share one
    write, matching the online turn path; unpaired/system/tool messages remain.
    The source has minute-level session dates, not invented per-turn timestamps.
    Each sample has a unique user scope shared across that sample's sessions.
    """
    import ijson

    if not run_id:
        raise ValueError("a new isolated run_id is required")
    with Path(path).open("rb") as source:
        # Fail rather than silently treating a root object / wrong dataset as no input.
        while True:
            first = source.read(1)
            if not first or not first.isspace():
                break
        if first != b"[":
            raise ValueError("LongMemEval input must be a JSON array")
        source.seek(0)
        for sample in ijson.items(source, "item", use_float=True):
            if not isinstance(sample, dict):
                raise ValueError("invalid LongMemEval source sample")
            qid = sample.get("question_id")
            sessions = sample.get("haystack_sessions")
            dates = sample.get("haystack_dates")
            ids = sample.get("haystack_session_ids")
            if not isinstance(qid, str) or not qid or not isinstance(sessions, list):
                raise ValueError("invalid LongMemEval source sample")
            if not isinstance(dates, list) or len(dates) != len(sessions):
                raise ValueError("every source session requires its original date")
            if not isinstance(ids, list) or len(ids) != len(sessions):
                raise ValueError("every source session requires its source ID")
            scope = Scope(org=org, user=f"{run_id}:{qid}", agent=agent)
            dated = []
            for index, date in enumerate(dates):
                observed = datetime.strptime(date, "%Y/%m/%d (%a) %H:%M")
                dated.append((observed.replace(tzinfo=source_timezone), index))
            for observed, session_index in sorted(dated):
                session = sessions[session_index]
                if not isinstance(session, list) or not session:
                    raise ValueError("empty or invalid source session")
                if any(not isinstance(message, dict) for message in session):
                    raise ValueError("invalid source message")
                source_id = ids[session_index]
                if not isinstance(source_id, str) or not source_id:
                    raise ValueError("invalid source session ID")
                turn = 0
                while turn < len(session):
                    start = turn
                    messages = [session[turn]]
                    turn += 1
                    if (messages[0].get("role") == "user" and turn < len(session)
                            and session[turn].get("role") == "assistant"):
                        messages.append(session[turn])
                        turn += 1
                    parts = []
                    for message in messages:
                        role, content = message.get("role"), message.get("content")
                        if role not in {"user", "assistant", "system", "tool"}:
                            raise ValueError("unsupported source message role")
                        if not isinstance(content, str) or not content.strip():
                            raise ValueError("empty or non-text source message")
                        parts.append(f"{role}: {content}")
                    # Opaque IDs prevent answer-prefixed benchmark session IDs leaking
                    # into native metadata. Raw source alignment stays in the dataset.
                    record = SourceRecord(
                        f"{qid}:{session_index}:{start}", "\n".join(parts), scope,
                        observed.astimezone(UTC).isoformat(),
                        source_session=digest(source_id),
                    )
                    yield SourceRecord.parse(record.payload())
