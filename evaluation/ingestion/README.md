# Full-history ingestion before benchmarks

This experimental **Python API and CLI** feed source conversations through the normal
public `MemoryAPI.add(..., system_metadata={"infer": True})` path. They do not add new
HTTP server routes. Native extraction, recent-conversation context, deduplication,
updates and indexing run synchronously. Questions and answer labels stay outside ingestion.

## What runs

1. `prepare` streams JSONL records or the **entire supplied LongMemEval JSON array**
   into a local SQLite journal and seals its ordered source digest. This makes no
   generation/embedding calls; the default factory assembles the native runtime only.
2. `run` writes records in source chronology, checkpoints each completed API call,
   and stops at the first error. `--max-records 1` bounds new writes for a first pilot.
3. `status` reports source counts, completed/uncertain/pending records, IDs returned
   by native writes and zero-extraction calls. Returned IDs are **not a unique final
   memory count**: native deduplication/update can reuse IDs.
4. `ready` verifies the completion state and pipeline/storage fingerprint.
5. `benchmark` checks **every** supplied job before importing/calling the benchmark
   callback. Resume skips completed records. Paginated `service.results(job)` supplies
   the original record-to-derived-ID map for scoring/source audits.

The old `evaluation.longmemeval.run` remains a separate no-source diagnostic runner.
**It does not enforce this new gate.** Full-history experiments must use
`IngestionService.run_benchmark` or the new `benchmark` command; do not call the old
runner and describe it as this native, full-history protocol.

## CLI

Run from this fork with the environment used for the experiments. Install the small
streaming parser with `pip install -e '.[evaluation]'` if necessary.

```bash
export MEMORY_INGEST_ROOT=/absolute/path/to/fresh-experiment-store
# Model/provider parameters use the existing agent adapter environment variables:
# JIUWEN_MEMORY_BASE_URL, JIUWEN_MEMORY_MODEL, JIUWEN_MEMORY_API_KEY,
# EMBED_API_BASE, EMBED_MODEL, EMBED_API_KEY; MEMORY_INGEST_EMBED_DIM defaults to 1024.
# Optional credential files (values are never printed by this layer):
# MEMORY_INGEST_LLM_KEY_FILE, MEMORY_INGEST_EMBED_KEY_FILE

python -m evaluation.ingestion --journal /absolute/path/to/ingestion.db prepare \
  --format longmemeval --input /path/to/longmemeval_s_cleaned.json \
  --dataset-id longmemeval-s-full --run-id unique-experiment-001
# Save the job ID printed immediately by prepare.
python -m evaluation.ingestion --journal /absolute/path/to/ingestion.db run \
  --job JOB_ID --max-records 1
python -m evaluation.ingestion --journal /absolute/path/to/ingestion.db status --job JOB_ID
python -m evaluation.ingestion --journal /absolute/path/to/ingestion.db run --job JOB_ID
python -m evaluation.ingestion --journal /absolute/path/to/ingestion.db ready --job JOB_ID
python -m evaluation.ingestion --journal /absolute/path/to/ingestion.db benchmark \
  --job JOB_ID --callback my_benchmark:run_queries
```

The callback receives verified receipts; it must query the same persistent store
and sample scopes (`org="memory-eval"`, `user="unique-experiment-001:QUESTION_ID"`,
`agent="jiuwen"`, empty session). It owns the retrieval/answer/judge configuration
and must save those settings separately. A callback must not re-ingest the data.
Pass `--job` multiple times to require a global barrier across jobs.
For an already assembled runtime, inject `MemoryAPIWriter` directly or provide
`--factory my_module:make_writer`; the factory returns a writer with `fingerprint`
and `write(SourceRecord) -> list[str]`, plus optional `close()`.

## Python API

```python
from evaluation.ingestion import IngestionService, MemoryAPIWriter

writer = MemoryAPIWriter(api, trusted_security_provider, {
    "revision": "exact-code-version",
    "storage_id": "isolated-persistent-store-identity",
    "config": effective_model_extractor_tokenizer_chunker_and_index_settings,
})
service = IngestionService("/absolute/path/to/ingestion.db", writer)
job = service.create("full-source-dataset-version")
service.append(job, [{
    "record_id": "session-1-turn-0",
    "content": "user: I prefer dark theme.\nassistant: Saved.",
    "scope": {"org": "experiment", "user": "alice", "agent": "jiuwen", "session": ""},
    "observed_at": "2024-01-01T12:00:00+00:00",
}], offset=0)
# Append all further source turns (including later corrections), then:
service.seal(job)
service.run(job)
results = service.run_benchmark([job], lambda receipts: run_queries(receipts))
```

Only source fields are accepted: `record_id`, `content`, native `scope`,
`observed_at`, optional `project` and opaque `source_session`. No answer/question
fields or arbitrary system flags can override extraction. Each append is an
atomic page of at most 100 records / 8 MiB; each source record is at most 2 MiB.
A larger source must be split explicitly at conversation boundaries, not silently
truncated. Native chunk/prompt limits still apply and are part of the evaluated system.
Page retries with identical offset/content are safe **during upload**. Failed
preparation stays unsealed: use `append` to resume or create a fresh job.

## Exact source handling and profile differences

LongMemEval includes **all** supplied sessions/messages. The importer does not inspect
`answer_session_ids`, `has_answer`, question text or expected answers. Adjacent
user+assistant messages share a write; unpaired/system/tool messages are retained.
Sessions are sorted by their original dates. Different samples have isolated users;
sessions inside one sample share memory. Source session IDs are hashed, since the
benchmark itself sometimes prefixes relevant IDs with `answer_`.

LongMemEval timestamps have no timezone: `--source-timezone UTC` is the explicit
default assumption. No artificial per-turn time is invented. `occurred_at` preserves
the source timestamp; `observation_date` supplies the source day to native extraction.
Native `t_ingest`/`t_valid` remain the real ingestion clock. A historical question's
source date must **not** be used as `as_of` against newly created validity intervals;
filter source/message time as required by the benchmark protocol instead.

The bundled factory calls the same public `agent_default_config()` used by the adapter.
It keeps `AgentEngine`, `agent_llm`, shadow deduplication and document construction.
For restartable offline batches, it changes the raw-message KV to persistent SQLite,
uses explicit document/shadow paths and disables the document watcher. Model parameters
come from the usual environment; it does not silently substitute hashing or a new model.
Its fingerprint includes effective configuration excluding credentials, Git revision,
a digest of current source/config files and a persistent random storage identity.
Custom factories must identify their effective code, models and storage just as carefully.

## Interruptions and limits

A journal checkpoint and native storage changes cannot share one transaction. On any
write exception, or after a crash during a write, that record is **uncertain**. Normal
resume refuses to replay it. After confirming an old worker has stopped, `recover
--worker-stopped` fences its journal ownership; inspect the store and preferably rebuild
in a fresh scope/store. `retry --acknowledge-partial-write` is an explicit operator
option and may repeat partial side effects. There is no exactly-once guarantee.

A successful empty extraction is counted and visible; the API cannot distinguish a
legitimate empty result from native extraction that silently returned no facts. Review
zero extractions before interpreting accuracy. The receipt proves call completion and
configuration/source alignment, not memory quality or external store integrity. Do not
mutate, delete or share the benchmark store with live traffic after ingestion. Native
recent-source eviction and all native update/extraction limitations remain in effect;
the durable journal retains the complete submitted source input separately.

No new paid benchmark was run to validate this infrastructure. Tests cover deterministic
failures/concurrency plus the real agent engine/extractor/dedup/document/restart path
with fixed model output and a hashing embedder. Those tests are not a model-quality result.
