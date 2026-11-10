"""CLI for source preparation, bounded ingestion, status and gated benchmarks."""
from __future__ import annotations

import argparse
import importlib
import json
from zoneinfo import ZoneInfo

from .importers import jsonl_records, longmemeval_records
from .records import canonical
from .service import IngestionService


def _callable(reference: str):
    module, name = reference.split(":", 1)
    result = getattr(importlib.import_module(module), name)
    if not callable(result):
        raise ValueError("factory/callback must be callable")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--factory", default="evaluation.ingestion.runtime:configured_writer",
                        help="trusted local module:function returning a native writer")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="stream source records and seal, no LLM writes")
    prepare.add_argument("--input", required=True)
    prepare.add_argument("--format", choices=("jsonl", "longmemeval"), required=True)
    prepare.add_argument("--dataset-id", required=True)
    prepare.add_argument("--run-id", help="new isolated scope prefix for LongMemEval")
    prepare.add_argument("--source-timezone", default="UTC",
                         help="explicit assumption for timezone-less LongMemEval dates")
    run = commands.add_parser("run")
    run.add_argument("--job", required=True)
    run.add_argument("--max-records", type=int, help="pause after this many new native writes")
    for name in ("status", "ready", "recover", "retry"):
        command = commands.add_parser(name)
        command.add_argument("--job", required=True)
        if name == "recover":
            command.add_argument("--worker-stopped", action="store_true")
        if name == "retry":
            command.add_argument("--acknowledge-partial-write", action="store_true")
    benchmark = commands.add_parser("benchmark")
    benchmark.add_argument("--job", action="append", required=True)
    benchmark.add_argument("--callback", required=True,
                           help="trusted local module:function accepting verified receipts")
    args = parser.parse_args(argv)
    writer = None
    try:
        if args.command not in ("status", "recover"):
            writer = _callable(args.factory)()
        service = IngestionService(args.journal, writer)
        if args.command == "prepare":
            if args.format == "longmemeval" and not args.run_id:
                parser.error("--run-id is required for isolated LongMemEval ingestion")
            job_id = service.create(args.dataset_id)
            # Emit immediately: a malformed source remains an unsealed, inspectable job.
            print(json.dumps({"job": job_id, "state": "uploading"}), flush=True)
            records = (jsonl_records(args.input) if args.format == "jsonl" else
                       longmemeval_records(args.input, run_id=args.run_id,
                                           source_timezone=ZoneInfo(args.source_timezone)))
            page, size, offset = [], 0, 0
            for record in records:
                record_size = len(canonical(record.payload()).encode())
                if page and (len(page) == 100 or size + record_size > 8 * 1024 * 1024):
                    service.append(job_id, page, offset=offset)
                    offset += len(page)
                    page, size = [], 0
                page.append(record)
                size += record_size
            if page:
                service.append(job_id, page, offset=offset)
            result = service.seal(job_id)
        elif args.command == "run":
            result = service.run(args.job, max_records=args.max_records)
        elif args.command == "status":
            result = service.status(args.job)
        elif args.command == "ready":
            result = service.require_ready(args.job)
        elif args.command == "recover":
            result = service.recover(args.job, worker_stopped=args.worker_stopped)
        elif args.command == "retry":
            service.retry_uncertain(args.job,
                                    acknowledge_partial_write=args.acknowledge_partial_write)
            result = service.status(args.job)
        else:
            # Import the callback only AFTER the barrier: imports can have side effects.
            result = service.run_benchmark(args.job,
                         lambda receipts: _callable(args.callback)(receipts))
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        if writer is not None:
            close = getattr(writer, "close", None)
            if callable(close):
                close()


if __name__ == "__main__":
    raise SystemExit(main())
