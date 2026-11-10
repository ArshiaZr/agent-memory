"""Durable, bounded ingestion jobs and a fail-closed benchmark barrier.

The journal and native stores cannot share a transaction. An interrupted native
write is therefore uncertain, never automatically replayed as if it were atomic.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TypeVar
from uuid import uuid4

from .records import SourceRecord, canonical, digest

T = TypeVar("T")


class Writer(Protocol):
    fingerprint: str

    def write(self, record: SourceRecord) -> list[str]: ...


class IngestionBlocked(RuntimeError):
    """A job is incomplete, uncertain, already running or from another pipeline."""


class IngestionService:
    def __init__(self, journal: str | Path, writer: Writer | None = None):
        self.path = Path(journal)
        self.writer = writer
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS ingestion_jobs(
                    id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, state TEXT NOT NULL,
                    input_digest TEXT, token TEXT, completed_at TEXT);
                CREATE TABLE IF NOT EXISTS ingestion_records(
                    job TEXT NOT NULL REFERENCES ingestion_jobs(id), seq INTEGER NOT NULL,
                    record_id TEXT NOT NULL, payload TEXT NOT NULL, hash TEXT NOT NULL,
                    scope_key TEXT NOT NULL, observed_at TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending', unit_ids TEXT, error_type TEXT,
                    PRIMARY KEY(job,seq), UNIQUE(job,record_id));
                CREATE INDEX IF NOT EXISTS ingestion_timeline
                    ON ingestion_records(job,scope_key,seq DESC);
            """)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _job(db, job_id: str):
        row = db.execute("SELECT * FROM ingestion_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return row

    def _check_pipeline(self, job) -> None:
        if self.writer is None or job["fingerprint"] != self.writer.fingerprint:
            raise IngestionBlocked("matching writer/pipeline/storage fingerprint required")

    def create(self, dataset_id: str) -> str:
        if not isinstance(dataset_id, str) or not dataset_id.strip() or self.writer is None:
            raise ValueError("dataset identity and writer required")
        job_id = uuid4().hex
        with self._db() as db:
            db.execute("INSERT INTO ingestion_jobs VALUES(?,?,?,'uploading',NULL,NULL,NULL)",
                       (job_id, dataset_id, self.writer.fingerprint))
        return job_id

    def append(self, job_id: str, records: Iterable[SourceRecord | dict], *, offset: int) -> None:
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        page = []
        size = 0
        for record in records:
            value = record.payload() if isinstance(record, SourceRecord) else record
            parsed = SourceRecord.parse(value)
            payload = canonical(parsed.payload())
            size += len(payload.encode())
            if len(page) >= 100 or size > 8 * 1024 * 1024:
                raise ValueError("append page exceeds 100 records / 8 MiB")
            page.append((parsed, payload))
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            job = self._job(db, job_id)
            if job["state"] != "uploading":
                raise IngestionBlocked("sealed jobs are immutable")
            count = db.execute("SELECT count(*) FROM ingestion_records WHERE job=?",
                               (job_id,)).fetchone()[0]
            if offset > count:
                raise ValueError("append offset leaves a gap")
            for seq, (record, payload) in enumerate(page, offset):
                payload_hash = digest(record.payload())
                previous = db.execute("SELECT hash FROM ingestion_records WHERE job=? AND seq=?",
                                      (job_id, seq)).fetchone()
                if previous:
                    if previous[0] != payload_hash:
                        raise ValueError("replayed page differs from saved source")
                    continue
                scope_key = canonical(record.payload()["scope"])
                last = db.execute("SELECT observed_at FROM ingestion_records "
                                  "WHERE job=? AND scope_key=? ORDER BY seq DESC LIMIT 1",
                                  (job_id, scope_key)).fetchone()
                if last and record.observed_at < last[0]:
                    raise ValueError("records must be chronological within each scope")
                db.execute("INSERT INTO ingestion_records "
                           "(job,seq,record_id,payload,hash,scope_key,observed_at) "
                           "VALUES(?,?,?,?,?,?,?)",
                           (job_id, seq, record.record_id, payload, payload_hash,
                            scope_key, record.observed_at))

    def seal(self, job_id: str) -> dict:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            job = self._job(db, job_id)
            if job["state"] == "uploading":
                checksum = hashlib.sha256()
                count = 0
                for row in db.execute("SELECT hash FROM ingestion_records WHERE job=? ORDER BY seq",
                                      (job_id,)):
                    checksum.update(row[0].encode())
                    count += 1
                if not count:
                    raise ValueError("cannot seal empty ingestion")
                db.execute("UPDATE ingestion_jobs SET state='sealed',input_digest=? WHERE id=?",
                           (checksum.hexdigest(), job_id))
        return self.status(job_id)

    def status(self, job_id: str) -> dict:
        with self._db() as db:
            job = dict(self._job(db, job_id))
            job.pop("token")
            counts = dict(db.execute("SELECT state,count(*) FROM ingestion_records "
                                     "WHERE job=? GROUP BY state", (job_id,)))
            job["counts"] = counts
            job["source_records"] = sum(counts.values())
            job["returned_units"] = 0
            job["zero_extractions"] = 0
            for row in db.execute("SELECT unit_ids FROM ingestion_records "
                                  "WHERE job=? AND state='succeeded'", (job_id,)):
                count = len(json.loads(row[0]))
                job["returned_units"] += count
                job["zero_extractions"] += count == 0
            # Safe diagnostics: no source content, provider errors or credential values.
            problem = db.execute("SELECT seq,record_id,state,error_type FROM ingestion_records "
                                 "WHERE job=? AND state IN ('running','uncertain') "
                                 "ORDER BY seq LIMIT 1", (job_id,)).fetchone()
            job["problem"] = dict(problem) if problem else None
            return job

    def run(self, job_id: str, *, max_records: int | None = None) -> dict:
        if max_records is not None and (type(max_records) is not int or max_records < 1):
            raise ValueError("max_records must be positive")
        token = uuid4().hex
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            job = self._job(db, job_id)
            self._check_pipeline(job)
            if job["state"] == "complete":
                return self.status(job_id)
            if job["state"] not in ("sealed", "blocked"):
                raise IngestionBlocked("seal first; concurrent/restarted workers require recovery")
            if db.execute("SELECT 1 FROM ingestion_records WHERE job=? "
                          "AND state IN ('running','uncertain')", (job_id,)).fetchone():
                raise IngestionBlocked("uncertain native write requires operator review")
            db.execute("UPDATE ingestion_jobs SET state='running',token=? WHERE id=?",
                       (token, job_id))
        processed = 0
        while True:
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                self._check_owner(db, job_id, token)
                row = db.execute("SELECT seq,payload FROM ingestion_records WHERE job=? "
                                 "AND state='pending' ORDER BY seq LIMIT 1", (job_id,)).fetchone()
                if row is None:
                    db.execute("UPDATE ingestion_jobs SET state='complete',token=NULL,"
                               "completed_at=? WHERE id=?", (datetime.now(UTC).isoformat(), job_id))
                    break
                if max_records is not None and processed >= max_records:
                    db.execute("UPDATE ingestion_jobs SET state='sealed',token=NULL WHERE id=?",
                               (job_id,))
                    break
                db.execute("UPDATE ingestion_records SET state='running' WHERE job=? AND seq=?",
                           (job_id, row["seq"]))
            try:
                ids = self.writer.write(SourceRecord.parse(json.loads(row["payload"])))
                if not isinstance(ids, list) or any(
                    not isinstance(uid, str) or not uid for uid in ids
                ):
                    raise ValueError("writer returned invalid memory IDs")
                with self._db() as db:
                    db.execute("BEGIN IMMEDIATE")
                    self._check_owner(db, job_id, token)
                    db.execute("UPDATE ingestion_records SET state='succeeded',unit_ids=? "
                               "WHERE job=? AND seq=?", (canonical(ids), job_id, row["seq"]))
            except BaseException as exc:
                with self._db() as db:
                    db.execute("BEGIN IMMEDIATE")
                    self._check_owner(db, job_id, token)
                    db.execute("UPDATE ingestion_records SET state='uncertain',error_type=? "
                               "WHERE job=? AND seq=?", (type(exc).__name__, job_id, row["seq"]))
                    db.execute("UPDATE ingestion_jobs SET state='blocked',token=NULL WHERE id=?",
                               (job_id,))
                raise
            processed += 1
        return self.status(job_id)

    def _check_owner(self, db, job_id: str, token: str) -> None:
        job = self._job(db, job_id)
        if job["state"] != "running" or job["token"] != token:
            raise IngestionBlocked("worker lost ownership; native side effects require review")

    def recover(self, job_id: str, *, worker_stopped: bool = False) -> dict:
        if worker_stopped is not True:
            raise IngestionBlocked("confirm the old worker has stopped before recovery")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            job = self._job(db, job_id)
            if job["state"] != "running":
                raise IngestionBlocked("only interrupted running jobs need recovery")
            db.execute("UPDATE ingestion_records SET state='uncertain',"
                       "error_type='InterruptedWrite' WHERE job=? AND state='running'", (job_id,))
            db.execute("UPDATE ingestion_jobs SET state='blocked',token=NULL WHERE id=?", (job_id,))
        return self.status(job_id)

    def retry_uncertain(self, job_id: str, *, acknowledge_partial_write: bool = False) -> None:
        if acknowledge_partial_write is not True:
            raise IngestionBlocked("native write may already have persisted; inspect/rebuild first")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            job = self._job(db, job_id)
            self._check_pipeline(job)
            if job["state"] != "blocked":
                raise IngestionBlocked("only blocked jobs can be reviewed for retry")
            db.execute("UPDATE ingestion_records SET state='pending',error_type=NULL "
                       "WHERE job=? AND state='uncertain'", (job_id,))

    def results(self, job_id: str, *, offset: int = 0, limit: int = 100) -> list[dict]:
        """Paginate the source-to-unit mapping; never materialize a huge job in RAM."""
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("results require offset >= 0 and limit 1..100")
        with self._db() as db:
            self._job(db, job_id)
            rows = db.execute("SELECT seq,record_id,scope_key,observed_at,state,unit_ids "
                              "FROM ingestion_records WHERE job=? ORDER BY seq LIMIT ? OFFSET ?",
                              (job_id, limit, offset))
            return [{"seq": row["seq"], "record_id": row["record_id"],
                     "scope": json.loads(row["scope_key"]), "observed_at": row["observed_at"],
                     "state": row["state"], "unit_ids": json.loads(row["unit_ids"] or "[]")}
                    for row in rows]

    def require_ready(self, job_id: str) -> dict:
        status = self.status(job_id)
        self._check_pipeline(status)
        if (status["state"] != "complete" or not status["source_records"]
                or status["counts"] != {"succeeded": status["source_records"]}
                or not status["input_digest"]):
            raise IngestionBlocked("all sealed source records must finish before benchmark queries")
        return status

    def run_benchmark(self, job_ids: list[str], benchmark: Callable[[list[dict]], T]) -> T:
        if not job_ids:
            raise IngestionBlocked("at least one ingestion receipt required")
        receipts = [self.require_ready(job_id) for job_id in job_ids]
        return benchmark(receipts)
