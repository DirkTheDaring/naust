# syntax=docker/dockerfile:1

# NOTE: This repo targets Rust 2024 edition.
# We use Alpine images here because this environment's Podman setup cannot run glibc-based images.
FROM rust:1.88-alpine AS build
WORKDIR /app

RUN apk add --no-cache build-base musl-dev

# Cache deps first.
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM alpine:3.20

RUN addgroup -S -g 10001 registry \
  && adduser -S -u 10001 -G registry registry \
  && apk add --no-cache ca-certificates

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
CMD ["server"]
