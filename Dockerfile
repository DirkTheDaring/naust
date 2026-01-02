# syntax=docker/dockerfile:1

FROM rust:1.76-bookworm AS build
WORKDIR /app

# Cache deps first.
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN adduser --system --uid 10001 --group registry \
  && apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY --from=build /app/target/release/registry-rust /usr/local/bin/registry-rust

ENV LISTEN_ADDR=0.0.0.0:5000 \
    STORAGE_BACKEND=fs \
    STORAGE_FS_ROOT=/data \
    ALLOW_TAG_OVERWRITE=1 \
    MAX_UPLOAD_BYTES=5368709120 \
    MAX_REQUEST_BODY_BYTES=33554432 \
    REQUEST_TIMEOUT_SECS=300

VOLUME ["/data"]
EXPOSE 5000

USER registry
ENTRYPOINT ["/usr/local/bin/registry-rust"]
