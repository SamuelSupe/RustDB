FROM rust@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS builder

WORKDIR /workspace
ENV RUSTUP_TOOLCHAIN=1.97.0 \
    CARGO_INCREMENTAL=0

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --locked --release --bin rustdb

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818 AS runtime

LABEL org.opencontainers.image.title="RustDB" \
      org.opencontainers.image.description="Single-node Rust OLAP engine" \
      org.opencontainers.image.source="https://github.com/SamuelSupe/RustDB" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder --chown=root:root /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN groupadd --gid 10001 rustdb \
    && useradd --uid 10001 --gid rustdb --no-create-home --home-dir /nonexistent rustdb \
    && install -d -o rustdb -g rustdb -m 0700 /var/lib/rustdb /var/lib/rustdb/state /var/lib/rustdb/results

COPY --from=builder --chown=root:root /workspace/target/release/rustdb /usr/local/bin/rustdb

USER 10001:10001
WORKDIR /var/lib/rustdb
VOLUME ["/var/lib/rustdb"]
EXPOSE 7400
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/rustdb"]
CMD ["--help"]
