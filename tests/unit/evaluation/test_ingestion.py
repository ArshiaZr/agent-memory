"""Behavioral tests for native writes, durable progress and benchmark isolation."""
from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event
from types import SimpleNamespace

import pytest

from evaluation.ingestion import (
    IngestionBlocked,
    IngestionService,
    MemoryAPIWriter,
    SourceRecord,
    pipeline_fingerprint,
)
from evaluation.ingestion.__main__ import main
from evaluation.ingestion.importers import jsonl_records, longmemeval_records
from jiuwen_memory.api import Scope

pytestmark = pytest.mark.unit
SPEC = {"revision": "test-version", "storage_id": "test-store", "config": {"model": "fake"}}


def record(index=0, *, user="alice", day=1):
    return SourceRecord(f"{user}-{index}", f"user: Theme preference {index}.\nassistant: Saved.",
                        Scope(org="test", user=user, agent="jiuwen"),
                        f"2024-01-{day:02d}T12:00:00+00:00")


class RecordingWriter:
    fingerprint = pipeline_fingerprint(SPEC)

    def __init__(self):
        self.records = []
        self.fail = False

    def write(self, source):
        self.records.append(source)
        if self.fail:
            raise TimeoutError("provider credentials MUST NOT appear in journal")
        return [] if source.content == "user: No lasting facts." else [f"unit-{source.record_id}"]


@pytest.fixture
def setup_job(tmp_path):
    writer = RecordingWriter()
    service = IngestionService(tmp_path / "journal.db", writer)
    job = service.create("complete-histories")
    return service, writer, job


def test_all_histories_before_any_benchmark_and_restart_skips_saved_writes(setup_job):
    service, writer, job = setup_job
    service.append(job, [record(0), record(1, day=2), record(2, user="bob")], offset=0)
    with pytest.raises(IngestionBlocked):
        service.run(job)
    service.seal(job)
    callbacks = []
    service.run(job, max_records=1)
    with pytest.raises(IngestionBlocked):
        service.run_benchmark([job], lambda receipts: callbacks.append(receipts))
    assert callbacks == []
    restarted = IngestionService(service.path, writer)
    restarted.run(job)
    restarted.run(job)
    restarted.run_benchmark([job], lambda receipts: callbacks.append(receipts))
    assert [source.record_id for source in writer.records] == ["alice-0", "alice-1", "bob-2"]
    assert callbacks[0][0]["counts"] == {"succeeded": 3}
    assert callbacks[0][0]["input_digest"]
    assert restarted.results(job, offset=1, limit=1)[0]["unit_ids"] == ["unit-alice-1"]


def test_one_incomplete_job_blocks_multi_job_benchmark(setup_job):
    service, _, first = setup_job
    service.append(first, [record()], offset=0)
    service.seal(first)
    service.run(first)
    second = service.create("second")
    service.append(second, [record(user="bob")], offset=0)
    service.seal(second)
    called = []
    with pytest.raises(IngestionBlocked):
        service.run_benchmark([first, second], lambda receipts: called.append(receipts))
    assert called == []
    with pytest.raises(IngestionBlocked):
        service.run_benchmark([], lambda receipts: None)


def test_page_replay_is_idempotent_conflicts_and_gaps_roll_back(setup_job):
    service, _, job = setup_job
    service.append(job, [record()], offset=0)
    service.append(job, [record()], offset=0)
    changed = record().payload()
    changed["content"] = "different history"
    for records, offset in (([changed], 0), ([record(2)], 3)):
        with pytest.raises(ValueError):
            service.append(job, records, offset=offset)
    with pytest.raises(sqlite3.IntegrityError):
        service.append(job, [record(1), record()], offset=1)
    assert service.status(job)["source_records"] == 1


def test_chronology_is_enforced_per_scope_and_entire_page_is_atomic(setup_job):
    service, _, job = setup_job
    service.append(job, [record(day=3)], offset=0)
    with pytest.raises(ValueError, match="chronological"):
        service.append(job, [record(1, user="bob"), record(2, day=2)], offset=1)
    assert service.status(job)["source_records"] == 1
    service.append(job, [record(1, user="bob")], offset=1)


def test_sealing_freezes_payload_and_empty_jobs_cannot_complete(setup_job):
    service, _, job = setup_job
    with pytest.raises(ValueError, match="empty"):
        service.seal(job)
    service.append(job, [record()], offset=0)
    first = service.seal(job)
    assert service.seal(job)["input_digest"] == first["input_digest"]
    with pytest.raises(IngestionBlocked):
        service.append(job, [record(1)], offset=1)


def test_failed_write_blocks_resume_and_does_not_log_provider_error(setup_job):
    service, writer, job = setup_job
    service.append(job, [record(), record(1)], offset=0)
    service.seal(job)
    writer.fail = True
    with pytest.raises(TimeoutError):
        service.run(job)
    status = service.status(job)
    assert status["counts"] == {"uncertain": 1, "pending": 1}
    assert status["problem"]["error_type"] == "TimeoutError"
    assert b"credentials MUST NOT" not in service.path.read_bytes()
    for action in (service.run, service.require_ready, service.retry_uncertain):
        with pytest.raises(IngestionBlocked):
            action(job)
    assert len(writer.records) == 1
    # Explicit operator acknowledgement permits retry; not an exactly-once promise.
    writer.fail = False
    service.retry_uncertain(job, acknowledge_partial_write=True)
    service.run(job)
    assert len(writer.records) == 3


def test_crashed_worker_requires_confirmed_recovery_and_review(setup_job):
    service, writer, job = setup_job
    service.append(job, [record()], offset=0)
    service.seal(job)
    with sqlite3.connect(service.path) as db:
        db.execute("UPDATE ingestion_jobs SET state='running',token='dead' WHERE id=?", (job,))
        db.execute("UPDATE ingestion_records SET state='running' WHERE job=?", (job,))
    for action in (service.run, service.recover):
        with pytest.raises(IngestionBlocked):
            action(job)
    recovered = service.recover(job, worker_stopped=True)
    assert recovered["counts"] == {"uncertain": 1}
    with pytest.raises(IngestionBlocked):
        service.run(job)
    assert writer.records == []


def test_concurrent_worker_cannot_issue_a_second_native_write(setup_job):
    service, writer, job = setup_job
    service.append(job, [record()], offset=0)
    service.seal(job)
    entered, release = Event(), Event()
    original = writer.write

    def blocked(source):
        entered.set()
        assert release.wait(5)
        return original(source)

    writer.write = blocked
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(service.run, job)
        try:
            assert entered.wait(5)
            with pytest.raises(IngestionBlocked):
                IngestionService(service.path, writer).run(job)
            with pytest.raises(IngestionBlocked):
                service.require_ready(job)
        finally:
            release.set()
        assert future.result()["state"] == "complete"
    assert len(writer.records) == 1


def test_pipeline_or_storage_change_cannot_reuse_completion(setup_job):
    service, writer, job = setup_job
    service.append(job, [record()], offset=0)
    service.seal(job)
    service.run(job)
    writer.fingerprint = pipeline_fingerprint({**SPEC, "storage_id": "different-store"})
    for action in (service.run, service.require_ready):
        with pytest.raises(IngestionBlocked, match="fingerprint"):
            action(job)


def test_zero_extraction_is_visible_and_never_replaced_by_gold(setup_job):
    service, writer, job = setup_job
    source = record().payload()
    source["content"] = "user: No lasting facts."
    service.append(job, [source], offset=0)
    service.seal(job)
    service.run(job)
    receipt = service.require_ready(job)
    assert receipt["zero_extractions"] == 1
    assert receipt["returned_units"] == 0
    assert len(writer.records) == 1


@pytest.mark.parametrize("change", [
    {"answer": "gold"}, {"question": "probe"}, {"has_answer": True},
    {"observed_at": "2024-01-01T00:00:00"}, {"scope": {"tenant_id": "x"}},
    {"content": ""}, {"project": {}}, {"content": "x" * (2 * 1024 * 1024)},
])
def test_invalid_or_labeled_source_records_are_rejected(change):
    with pytest.raises(ValueError):
        SourceRecord.parse({**record().payload(), **change})


def test_pages_are_bounded_and_invalid_tail_cannot_write_prefix(setup_job):
    service, _, job = setup_job
    with pytest.raises(ValueError, match="100 records"):
        service.append(job, (record(i) for i in range(101)), offset=0)
    assert service.status(job)["source_records"] == 0


def test_native_api_arguments_include_roles_dates_project_and_trusted_context():
    calls = []
    trusted = object()
    api = SimpleNamespace(add=lambda *args, **kwargs:
                          calls.append((args, kwargs)) or [SimpleNamespace(id="derived")])
    writer = MemoryAPIWriter(api, lambda: trusted, SPEC)
    source = record().payload()
    source["project"] = "editor"
    assert writer.write(SourceRecord.parse(source)) == ["derived"]
    args, kwargs = calls[0]
    assert args[0] == source["content"]
    assert args[1] == Scope(org="test", user="alice", agent="jiuwen")
    assert kwargs["security"] is trusted
    assert kwargs["system_metadata"] == {
        "infer": True, "observation_date": "2024-01-01", "project": "editor"}
    assert kwargs["occurred_at"] == datetime(2024, 1, 1, 12, tzinfo=UTC)
    assert kwargs["user_metadata"]["ingestion_record_id"] == "alice-0"


def test_full_longmemeval_import_keeps_distractors_unpaired_messages_and_updates(tmp_path):
    sample = {
        "question_id": "case-1", "question": "DO NOT WRITE QUESTION",
        "answer": "DO NOT WRITE GOLD", "answer_session_ids": ["answer-session"],
        "haystack_dates": ["2024/01/03 (Wed) 12:00", "2024/01/01 (Mon) 12:00"],
        "haystack_session_ids": ["answer-session", "distractor"],
        "haystack_sessions": [
            [{"role": "user", "content": "Actually use light theme.", "has_answer": True},
             {"role": "assistant", "content": "Updated."}],
            [{"role": "system", "content": "Conversation start."},
             {"role": "user", "content": "I use dark theme.", "has_answer": False},
             {"role": "assistant", "content": "Recorded."},
             {"role": "user", "content": "My coffee is espresso.", "has_answer": False}],
        ],
    }
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps([sample, {**sample, "question_id": "case-2"}]))
    sources = list(longmemeval_records(path, run_id="experiment"))
    assert len(sources) == 8
    assert sources[0].content == "system: Conversation start."
    assert sources[1].content == "user: I use dark theme.\nassistant: Recorded."
    assert sources[2].content == "user: My coffee is espresso."
    assert sources[3].content == "user: Actually use light theme.\nassistant: Updated."
    assert sources[0].scope == sources[3].scope
    assert sources[0].scope.user != sources[4].scope.user
    serialized = json.dumps([source.payload() for source in sources])
    assert "DO NOT WRITE" not in serialized
    assert "has_answer" not in serialized and "answer-session" not in serialized


def test_jsonl_stream_validates_each_line_and_never_silently_skips(tmp_path):
    path = tmp_path / "records.jsonl"
    path.write_text(json.dumps(record().payload()) + "\n\n")
    iterator = jsonl_records(path)
    assert next(iterator).record_id == "alice-0"
    with pytest.raises(ValueError, match="blank"):
        next(iterator)


def test_cli_blocks_callback_import_until_ready(setup_job, monkeypatch):
    service, writer, job = setup_job
    service.append(job, [record()], offset=0)
    service.seal(job)
    imported = []

    def load(reference):
        imported.append(reference)
        return lambda: writer

    monkeypatch.setattr("evaluation.ingestion.__main__._callable", load)
    with pytest.raises(IngestionBlocked):
        main(["--journal", str(service.path), "--factory", "trusted:writer",
              "benchmark", "--job", job, "--callback", "answer:queries"])
    assert imported == ["trusted:writer"]


def test_public_agent_profile_returns_fresh_unmodified_native_choices():
    from jiuwen_memory_adapter.jiuwenswarm.agent_memory_provider import agent_default_config

    first, second = agent_default_config(), agent_default_config()
    assert first["engine"]["default"]["target"] == "agent"
    assert first["extractor"]["default"]["target"] == "agent_llm"
    assert first["dedup"]["default"]["target"] == "shadow"
    first["globals"]["chunk_size"] = 1
    assert second["globals"]["chunk_size"] == 120


def test_configured_factory_pins_native_profile_and_ignores_only_credential_rotation(
    tmp_path, monkeypatch,
):
    from evaluation.ingestion.runtime import configured_writer

    configs = []
    closed = []

    def assemble(config):
        configs.append(config)
        return SimpleNamespace(api=object(), close=lambda: closed.append(True))

    monkeypatch.setenv("MEMORY_INGEST_ROOT", str(tmp_path))
    monkeypatch.setenv("JIUWEN_MEMORY_MODEL", "test-model")
    monkeypatch.setenv("JIUWEN_MEMORY_API_KEY", "fixture-first-credential")
    monkeypatch.delenv("MEMORY_INGEST_LLM_KEY_FILE", raising=False)
    monkeypatch.delenv("MEMORY_INGEST_EMBED_KEY_FILE", raising=False)
    monkeypatch.setattr("evaluation.ingestion.runtime.assemble_runtime", assemble)
    from evaluation.ingestion import runtime as ingestion_runtime

    original_profile = ingestion_runtime.agent_default_config

    def profile_with_optional_store():
        config = original_profile()
        config["kv_store"]["optional_source"] = "memory"
        return config

    monkeypatch.setattr(ingestion_runtime, "agent_default_config", profile_with_optional_store)
    first = configured_writer()
    monkeypatch.setenv("JIUWEN_MEMORY_API_KEY", "fixture-second-credential")
    rotated = configured_writer()
    assert first.fingerprint == rotated.fingerprint
    monkeypatch.setenv("JIUWEN_MEMORY_MODEL", "different-model")
    changed = configured_writer()
    assert changed.fingerprint != first.fingerprint
    assert configs[0]["engine"]["default"]["target"] == "agent"
    assert configs[0]["extractor"]["default"]["target"] == "agent_llm"
    assert configs[0]["kv_store"]["default"]["target"] == "sqlite"
    assert configs[0]["kv_store"]["optional_source"] == "memory"
    assert configs[0]["globals"]["watch_document"] is False
    assert first.security_provider().auth.actor == Scope(org="local", user="developer")
    for writer in (first, rotated, changed):
        writer.close()
    assert len(closed) == 3


def test_source_digest_detects_uncommitted_code_and_storage_marker_detects_new_store(tmp_path):
    from evaluation.ingestion.runtime import _source_digest, _storage_identity

    source = tmp_path / "evaluation/ingestion"
    source.mkdir(parents=True)
    code = source / "source.py"
    code.write_text("version = 1")
    before = _source_digest(tmp_path)
    code.write_text("version = 2")
    assert _source_digest(tmp_path) != before
    identity = _storage_identity(tmp_path)
    assert _storage_identity(tmp_path) == identity
    (tmp_path / ".ingestion-store-id").unlink()
    assert _storage_identity(tmp_path) != identity


def test_cli_prepare_run_pause_complete_then_callback(tmp_path, monkeypatch, capsys):
    writer = RecordingWriter()
    callbacks = []

    def load(reference):
        if reference == "trusted:writer":
            return lambda: writer
        return lambda receipts: callbacks.append(receipts) or {"queries_finished": True}

    monkeypatch.setattr("evaluation.ingestion.__main__._callable", load)
    path = tmp_path / "all-history.jsonl"
    path.write_text("\n".join(json.dumps(record(i).payload()) for i in range(3)) + "\n")
    common = ["--journal", str(tmp_path / "journal.db"), "--factory", "trusted:writer"]
    assert main([*common, "prepare", "--input", str(path), "--format", "jsonl",
                 "--dataset-id", "full-history"]) == 0
    prepared = json.loads(capsys.readouterr().out.splitlines()[-1])
    job = prepared["id"]
    assert prepared["state"] == "sealed" and writer.records == []
    assert main([*common, "run", "--job", job, "--max-records", "1"]) == 0
    capsys.readouterr()
    with pytest.raises(IngestionBlocked):
        main([*common, "ready", "--job", job])
    assert main([*common, "run", "--job", job]) == 0
    capsys.readouterr()
    assert main([*common, "benchmark", "--job", job, "--callback", "answer:queries"]) == 0
    assert len(callbacks) == 1 and callbacks[0][0]["counts"] == {"succeeded": 3}
    assert len(writer.records) == 3
