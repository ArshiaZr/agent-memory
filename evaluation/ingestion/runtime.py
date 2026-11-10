"""Local experiment composition root using the current native agent profile.

This explicit developer CLI identity is independent of dataset target scopes.
Deployments should supply their own authenticated writer factory.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from uuid import uuid4

from jiuwen_memory.api import (
    SECRET_PARAM_KEYS,
    Credentials,
    Surface,
    assemble_runtime,
    build_dev_authenticator,
    new_request_context,
)
from jiuwen_memory_adapter.jiuwenswarm.agent_memory_provider import agent_default_config

from .writer import MemoryAPIWriter


def _without_credentials(value):
    if isinstance(value, dict):
        return {key: _without_credentials(item) for key, item in value.items()
                if key not in SECRET_PARAM_KEYS and "api_key" not in key.lower()}
    if isinstance(value, list):
        return [_without_credentials(item) for item in value]
    return value


def _key_file(name: str, current: str) -> str:
    filename = os.environ.get(name)
    return Path(filename).read_text().strip() if filename else current


def _source_digest(repository: Path) -> str:
    checksum = hashlib.sha256()
    for folder in ("jiuwen_memory", "jiuwen_memory_adapter", "evaluation/ingestion"):
        for path in sorted((repository / folder).rglob("*")):
            if path.suffix not in {".py", ".yaml", ".yml"} or not path.is_file():
                continue
            checksum.update(str(path.relative_to(repository)).encode())
            checksum.update(b"\0")
            checksum.update(path.read_bytes())
    return checksum.hexdigest()


def _storage_identity(root: Path) -> str:
    marker = root / ".ingestion-store-id"
    try:
        with marker.open("x") as output:
            output.write(uuid4().hex)
            output.flush()
            os.fsync(output.fileno())
    except FileExistsError:
        pass
    identity = marker.read_text().strip()
    if len(identity) != 32 or any(ch not in "0123456789abcdef" for ch in identity):
        raise ValueError("invalid experiment storage identity marker")
    return identity


def configured_writer() -> MemoryAPIWriter:
    root_value = os.environ.get("MEMORY_INGEST_ROOT", "")
    if not root_value or not Path(root_value).is_absolute():
        raise ValueError("MEMORY_INGEST_ROOT must identify an absolute persistent experiment root")
    root = Path(root_value).resolve()
    root.mkdir(parents=True, exist_ok=True)
    identity = _storage_identity(root)
    config = agent_default_config()
    config["globals"]["watch_document"] = False
    config["globals"]["embedder_dim"] = int(os.environ.get("MEMORY_INGEST_EMBED_DIM", "1024"))
    for field, variable in (("llm_api_key", "MEMORY_INGEST_LLM_KEY_FILE"),
                            ("embedder_api_key", "MEMORY_INGEST_EMBED_KEY_FILE")):
        config["globals"][field] = _key_file(variable, config["globals"].get(field, ""))
    config["markdown_store"]["default"]["params"]["root"] = str(root / "documents")
    config["shadow_index"]["default"]["params"]["db_path"] = str(root / "shadow.db")
    config["kv_store"]["default"] = {"target": "sqlite", "params": {
        "db_path": str(root / "sources.db"),
    }}
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True,
    ).strip()
    runtime = assemble_runtime(config=config)
    try:
        authenticator = build_dev_authenticator(identities={"ingestion-evaluation": {
            "actor": {"org": "local", "user": "developer"}, "role": "root",
        }})

        def security():
            auth = authenticator.authenticate(Credentials(api_key="ingestion-evaluation"))
            return new_request_context(auth, surface=Surface.INTERNAL)

        return MemoryAPIWriter(runtime.api, security, {
            "revision": revision, "storage_id": f"{root}:{identity}",
            "code_digest": _source_digest(Path(__file__).resolve().parents[2]),
            "config": _without_credentials(config),
        }, close=runtime.close)
    except BaseException:
        runtime.close()
        raise
