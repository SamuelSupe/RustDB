from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import threading
import time
from typing import Any

import duckdb

from checksum import MODE, MultisetChecksum
from checksum_arrow import update_batch
from native_setup import NativeSetup
from rss import RssSampler
from start_gate import QueryStartGate
from worker_resources import collect as collect_worker_resources


EXPECTED_VERSION = "1.5.4"
CHECKSUM_BACKEND = "python-arrow-buffer-sha256-v2"
START_BARRIER_TIMEOUT_SECONDS = 30


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Long-lived DuckDB v0.7 benchmark worker")
    parser.add_argument("--threads", type=positive, default=4)
    parser.add_argument("--memory-limit", type=positive, default=2_147_483_648)
    parser.add_argument("--concurrency", type=positive, default=1)
    parser.add_argument("--batch-size", type=positive, default=8192)
    parser.add_argument("--temp-directory", required=True)
    parser.add_argument("--build-id", type=sha256_value, required=True)
    parser.add_argument("--database")
    return parser.parse_args()


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def sha256_value(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise argparse.ArgumentTypeError("must be a lowercase SHA-256")
    return value


class Worker:
    def __init__(self, args: argparse.Namespace) -> None:
        if duckdb.__version__ != EXPECTED_VERSION:
            raise RuntimeError(f"expected DuckDB {EXPECTED_VERSION}, got {duckdb.__version__}")
        os.makedirs(args.temp_directory, exist_ok=True)
        self.args = args
        self.worker_resources = collect_worker_resources()
        database = args.database or os.path.join(args.temp_directory, "benchmark.duckdb")
        if not database or database.startswith(":") or "://" in database:
            raise ValueError("the benchmark worker requires a shared file database")
        self.database = os.path.abspath(database)
        if os.path.isdir(self.database):
            raise ValueError("the benchmark database path must be a file")
        os.makedirs(os.path.dirname(self.database), exist_ok=True)
        self.connection = duckdb.connect(self.database)
        self.connection.execute(f"SET GLOBAL threads = {args.threads}")
        self.connection.execute(f"SET GLOBAL memory_limit = '{args.memory_limit}B'")
        self.connection.execute("SET GLOBAL enable_external_file_cache = false")
        self.connection.execute("SET GLOBAL parquet_metadata_cache = false")
        escaped_temp = args.temp_directory.replace("'", "''")
        self.connection.execute(f"SET GLOBAL temp_directory = '{escaped_temp}'")
        self._verify_connection(self.connection)
        self.version = self.connection.execute("SELECT version()").fetchone()[0]
        self.cache_state = {
            "os_page_cache": "warm-uncontrolled",
            "metadata_cache": "disabled",
            "external_file_cache": "disabled",
        }
        self.native_setup = NativeSetup(
            self.connection,
            self.database,
            args.temp_directory,
        )
        self._worker_local = threading.local()
        self._query_connections: list[duckdb.DuckDBPyConnection] = []
        self._connections_lock = threading.Lock()
        self.executor: concurrent.futures.ThreadPoolExecutor | None = None

    def _open_query_threads(self) -> None:
        if self.executor is not None:
            return
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.args.concurrency,
            thread_name_prefix="duckdb-query",
            initializer=self._initialize_query_thread,
        )
        try:
            self._start_query_threads()
        except Exception:
            self.executor.shutdown(wait=True, cancel_futures=True)
            for connection in self._query_connections:
                connection.close()
            self._query_connections.clear()
            self.executor = None
            raise

    def _verify_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        settings = connection.execute(
            "SELECT current_setting('threads'), current_setting('memory_limit'), "
            "current_setting('enable_external_file_cache'), "
            "current_setting('parquet_metadata_cache')"
        ).fetchone()
        if settings[0] != self.args.threads:
            raise RuntimeError(
                f"DuckDB applied {settings[0]} threads, expected {self.args.threads}"
            )
        if memory_bytes(settings[1]) != self.args.memory_limit:
            raise RuntimeError(
                f"DuckDB applied memory_limit={settings[1]!r}, "
                f"expected {self.args.memory_limit} bytes"
            )
        if settings[2] is not False or settings[3] is not False:
            raise RuntimeError("DuckDB external caches were not disabled")

    def _initialize_query_thread(self) -> None:
        connection = duckdb.connect(self.database)
        self._verify_connection(connection)
        self._worker_local.connection = connection
        with self._connections_lock:
            self._query_connections.append(connection)

    def _start_query_threads(self) -> None:
        if self.executor is None:
            raise RuntimeError("query executor is not open")
        barrier = threading.Barrier(
            self.args.concurrency + 1,
            timeout=START_BARRIER_TIMEOUT_SECONDS,
        )
        futures = [
            self.executor.submit(barrier.wait)
            for _ in range(self.args.concurrency)
        ]
        barrier.wait()
        for future in futures:
            future.result()

    def _close_query_threads(self) -> None:
        executor = self.executor
        if executor is None:
            return
        self.executor = None
        executor.shutdown(wait=True, cancel_futures=True)
        for connection in self._query_connections:
            connection.close()
        self._query_connections.clear()

    def close(self) -> None:
        self._close_query_threads()
        self.connection.close()

    def hello(self) -> dict[str, Any]:
        return {
            "kind": "hello",
            "engine": "duckdb",
            "version": self.version.removeprefix("v"),
            "build_id": self.args.build_id,
            "threads": self.args.threads,
            "memory_limit_bytes": self.args.memory_limit,
            "concurrency": self.args.concurrency,
            "batch_size": self.args.batch_size,
            "cache_state": self.cache_state,
            "worker_resources": self.worker_resources,
        }

    def run(self, command: dict[str, Any]) -> dict[str, Any]:
        self._open_query_threads()
        if self.executor is None:
            raise RuntimeError("query executor failed to open")
        setup_id = None
        if command.get("storage_track") == "native":
            setup_id = self.native_setup.require_ready(command.get("setup_id"))
        gate = QueryStartGate(self.args.concurrency, START_BARRIER_TIMEOUT_SECONDS)
        sampler = None
        try:
            futures = [
                self.executor.submit(
                    self._query,
                    command["sql"],
                    gate,
                    query_slot,
                    f"{command['run_id']}:query-{query_slot}",
                )
                for query_slot in range(self.args.concurrency)
            ]
            gate.wait_until_ready()
            sampler = RssSampler()
            sampler.start()
            group_started = gate.release()
            queries = [future.result() for future in futures]
            group_elapsed_ms = (time.perf_counter() - group_started) * 1000
        finally:
            if sampler is not None:
                baseline, peak = sampler.stop()
        checksums = {query["checksum"] for query in queries}
        if len(checksums) != 1:
            raise RuntimeError("concurrent executions returned different checksums")
        worker_resources = collect_worker_resources()
        if worker_resources != self.worker_resources:
            raise RuntimeError("worker resource constraints changed after hello")
        response = {
            "kind": "run",
            "run_id": command["run_id"],
            "engine": "duckdb",
            "version": self.version.removeprefix("v"),
            "build_id": self.args.build_id,
            "threads": self.args.threads,
            "memory_limit_bytes": self.args.memory_limit,
            "concurrency": self.args.concurrency,
            "batch_size": self.args.batch_size,
            "cache_state": self.cache_state,
            "worker_resources": worker_resources,
            "storage_track": command["storage_track"],
            "engine_order": command["engine_order"],
            "group_elapsed_ms": group_elapsed_ms,
            "rss_baseline_bytes": baseline,
            "peak_rss_bytes": peak,
            "start_skew_ms": start_skew_ms(queries),
            "throughput_queries_per_second": self.args.concurrency * 1000 / group_elapsed_ms,
            "queries": queries,
        }
        if setup_id is not None:
            response["setup_id"] = setup_id
        return response

    def setup(self, command: dict[str, Any]) -> dict[str, Any]:
        self._close_query_threads()
        return self.native_setup.execute(command)

    def _query(
        self,
        sql: str,
        gate: QueryStartGate,
        query_slot: int,
        harness_query_id: str,
    ) -> dict[str, Any]:
        connection = self._worker_local.connection
        group_started, started = gate.wait_for_start()
        start_offset_ms = (started - group_started) * 1000
        checksum = MultisetChecksum()
        rows = 0
        batches = 0
        ttfb_ms: float | None = None
        checksum_compute_ms = 0.0
        reader = None
        try:
            reader = connection.execute(sql).to_arrow_reader(self.args.batch_size)
            for batch in reader:
                if batch.num_rows == 0:
                    continue
                if ttfb_ms is None:
                    ttfb_ms = (time.perf_counter() - started) * 1000
                batches += 1
                rows += batch.num_rows
                checksum_started = time.perf_counter()
                update_batch(checksum, batch)
                checksum_compute_ms += (time.perf_counter() - checksum_started) * 1000
        finally:
            if reader is not None:
                reader.close()
        checksum_started = time.perf_counter()
        checksum_value = checksum.finish()
        checksum_compute_ms += (time.perf_counter() - checksum_started) * 1000
        finished = time.perf_counter()
        elapsed_ms = (finished - started) * 1000
        return {
            "query_slot": query_slot,
            "harness_query_id": harness_query_id,
            "start_offset_ms": start_offset_ms,
            "finish_offset_ms": (finished - group_started) * 1000,
            "elapsed_ms": elapsed_ms,
            "ttfb_ms": elapsed_ms if ttfb_ms is None else ttfb_ms,
            "rows": rows,
            "batches": batches,
            "checksum": checksum_value,
            "checksum_mode": MODE,
            "checksum_backend": CHECKSUM_BACKEND,
            "checksum_compute_ms": checksum_compute_ms,
            "complete": True,
        }


def start_skew_ms(queries: list[dict[str, Any]]) -> float:
    starts = [query["start_offset_ms"] for query in queries]
    return max(starts) - min(starts)


def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":")), flush=True)


def memory_bytes(value: str) -> int:
    number, unit = value.split()
    multipliers = {
        "bytes": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }
    return round(float(number) * multipliers[unit])


def main() -> int:
    worker: Worker | None = None
    try:
        worker = Worker(arguments())
        emit(worker.hello())
        for line in sys.stdin:
            if not line.strip():
                continue
            command = json.loads(line)
            command_name = command.get("command")
            if command_name == "shutdown":
                return 0
            if command_name == "setup":
                emit(worker.setup(command))
            elif command_name == "run":
                emit(worker.run(command))
            else:
                raise ValueError("command must be 'setup', 'run' or 'shutdown'")
        return 0
    except Exception as error:
        emit({"kind": "error", "message": str(error)})
        return 1
    finally:
        if worker is not None:
            worker.close()


if __name__ == "__main__":
    raise SystemExit(main())
