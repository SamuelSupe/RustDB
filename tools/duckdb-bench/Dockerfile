FROM debian@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

COPY tools/duckdb-bench/requirements.txt /opt/duckdb-bench/requirements.txt
RUN python3 -m venv /opt/duckdb-bench/venv \
    && /opt/duckdb-bench/venv/bin/pip install \
      --no-cache-dir --only-binary=:all: --require-hashes \
      --requirement /opt/duckdb-bench/requirements.txt

COPY tools/duckdb-bench/checksum.py tools/duckdb-bench/checksum_arrow.py \
     tools/duckdb-bench/disk.py tools/duckdb-bench/marker.py \
     tools/duckdb-bench/native_setup.py tools/duckdb-bench/rss.py \
     tools/duckdb-bench/source.py tools/duckdb-bench/start_gate.py \
     tools/duckdb-bench/worker_resources.py \
     tools/duckdb-bench/runner.py /opt/duckdb-bench/

ENV PATH=/opt/duckdb-bench/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/opt/duckdb-bench/runner.py"]
