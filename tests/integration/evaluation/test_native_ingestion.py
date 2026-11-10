"""Real agent engine/extractor/dedup/document indexes; only model output is fixed."""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime

import pytest

from evaluation.ingestion import IngestionService, MemoryAPIWriter, SourceRecord
from jiuwen_memory.api import (
    Context,
    Credentials,
    Scope,
    Surface,
    assemble_runtime,
    build_dev_authenticator,
    new_request_context,
)
from jiuwen_memory.common.llm.llm_impl.echo_llm import EchoLLM
from jiuwen_memory_adapter.jiuwenswarm.agent_memory_provider import agent_default_config

pytestmark = pytest.mark.integration


def test_native_extract_persist_restart_then_gated_retrieval(tmp_path, monkeypatch):
    model_prompts = []

    def model_output(self, messages, **options):
        text = messages[-1].content
        model_prompts.append(text)
        source_id = re.search(r"\[ID: ([^\]]+)\]", text)
        assert source_id is not None
        return json.dumps([{
            "source_id": source_id[1], "content": "User prefers espresso coffee.",
            "target": "fact", "tier": "semantic", "confidence": 1.0,
            "memory_class": "long_term_memory",
        }])

    monkeypatch.setattr(EchoLLM, "chat", model_output)
    config = agent_default_config()
    config["globals"].update(watch_document=False, embedder_dim=16, graph_enabled=False)
    config["llm"] = {"default": "echo"}
    config["embedder"] = {"default": "hashing"}
    config["tokenizer"] = {"default": "whitespace"}
    config["markdown_store"]["default"]["params"]["root"] = str(tmp_path / "docs")
    config["shadow_index"]["default"]["params"]["db_path"] = str(tmp_path / "shadow.db")
    config["kv_store"] = {"default": {"target": "sqlite", "params": {
        "db_path": str(tmp_path / "source.db"),
    }}}
    auth = build_dev_authenticator(identities={"test-root": {
        "actor": {"org": "local", "user": "developer"}, "role": "root",
    }})

    def security():
        return new_request_context(auth.authenticate(Credentials(api_key="test-root")),
                                   surface=Surface.INTERNAL)

    spec = {"revision": "fixture", "storage_id": str(tmp_path), "config": config}
    scope = Scope(org="test", user="alice", agent="jiuwen")
    observed = datetime(2024, 1, 1, 12, tzinfo=UTC)
    runtime = assemble_runtime(config=config)
    try:
        writer = MemoryAPIWriter(runtime.api, security, spec)
        service = IngestionService(tmp_path / "ingestion.db", writer)
        job = service.create("all-source-history")
        service.append(job, [SourceRecord("turn-1", "user: I prefer espresso coffee.\n"
                                          "assistant: Recorded.", scope, observed.isoformat())],
                       offset=0)
        service.seal(job)
        receipt = service.run(job)
        assert receipt["returned_units"] >= 1
        assert model_prompts and "observation_date: 2024-01-01" in model_prompts[0]
        units = runtime.api.list(scope, security=security()).items
        assert any("espresso" in unit.content for unit in units)
        assert all(unit.source_ref and unit.provenance for unit in units)
        assert all(unit.temporal.t_message == observed for unit in units)
        assert list((tmp_path / "docs").rglob("*.md"))
    finally:
        runtime.close()
    # New runtime, same persistent source/document/index state and job fingerprint.
    runtime = assemble_runtime(config=config)
    try:
        service = IngestionService(tmp_path / "ingestion.db",
                                   MemoryAPIWriter(runtime.api, security, spec))
        result = service.run_benchmark([job], lambda receipts: runtime.api.search(
            "espresso", Context(scope), security=security(),
        ))
        assert any("espresso" in item.content for item in result.items)
        assert len(model_prompts) == 1, "completed ingestion must not run the model again"
    finally:
        runtime.close()
