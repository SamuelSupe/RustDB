FROM debian@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df

ARG TARGETARCH
ARG DUCKDB_VERSION=1.4.3

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl gzip \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETARCH}" in \
      amd64) archive=duckdb_cli-linux-amd64.gz; checksum=c479794045d094058d3092e404e696508d6310b5d234a8c1945b745678f09d8d ;; \
      arm64) archive=duckdb_cli-linux-arm64.gz; checksum=c709eb3efc74a609af4b92bc885c509a1bd21ddfa71ea1e717420d4dd9fc121b ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --retry 5 \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/${archive}" \
      --output /tmp/duckdb.gz \
    && echo "${checksum}  /tmp/duckdb.gz" | sha256sum --check --strict \
    && gzip --decompress /tmp/duckdb.gz \
    && install --mode 0755 /tmp/duckdb /usr/local/bin/duckdb \
    && rm /tmp/duckdb

# Keep the versioned TPC-H extension in the image, so data generation does not
# download executable code at runtime.
RUN duckdb :memory: -batch -no-stdin -c "INSTALL tpch; LOAD tpch;" \
    && duckdb --version | grep --fixed-strings "v${DUCKDB_VERSION}"

ENTRYPOINT ["duckdb"]
