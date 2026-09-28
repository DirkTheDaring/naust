# syntax=docker/dockerfile:1

# NOTE: This repo targets Rust 2024 edition.
# We use Alpine images here because this environment's Podman setup cannot run glibc-based images.
FROM rust:alpine AS build
WORKDIR /app

RUN apk add --no-cache build-base musl-dev

# Copy source code and vendor dependencies
COPY . .

# Stage vendored path dependencies where the manifests expect them
# (../acmecert and ../naust-* relative to /app). Refresh vendor/
# with `make vendor-sync` before building (KI-10).
RUN if [ -d "vendor/acmecert" ]; then \
      mkdir -p /acmecert/crates \
      && cp -r vendor/acmecert/crates/acmecert-core /acmecert/crates/ \
      && cp vendor/acmecert/Cargo.toml /acmecert/Cargo.toml; \
    fi
RUN if [ -d "vendor/naust-storage-core" ]; then \
      mkdir -p /naust-storage-core && cp -r vendor/naust-storage-core/* /naust-storage-core/; \
    fi
RUN if [ -d "vendor/naust-storage-fs" ]; then \
      mkdir -p /naust-storage-fs && cp -r vendor/naust-storage-fs/* /naust-storage-fs/; \
    fi
RUN if [ -d "vendor/naust-storage-s3" ]; then \
      mkdir -p /naust-storage-s3 && cp -r vendor/naust-storage-s3/* /naust-storage-s3/; \
    fi
RUN if [ -d "vendor/naust-auth" ]; then \
      mkdir -p /naust-auth && cp -r vendor/naust-auth/* /naust-auth/; \
    fi
RUN if [ -d "vendor/naust-types" ]; then \
      mkdir -p /naust-types && cp -r vendor/naust-types/* /naust-types/; \
    fi
RUN if [ -d "vendor/naust-core" ]; then \
      mkdir -p /naust-core && cp -r vendor/naust-core/* /naust-core/; \
    fi

RUN cargo build --release

FROM alpine:3.20

RUN addgroup -S -g 10001 registry \
  && adduser -S -u 10001 -G registry registry \
  && apk add --no-cache ca-certificates

WORKDIR /srv
COPY --from=build /app/target/release/naust /usr/local/bin/naust

ENV LISTEN_ADDR=0.0.0.0:5000 \
    STORAGE_BACKEND=fs \
    STORAGE_FS_ROOT=/data \
    ALLOW_TAG_OVERWRITE=1 \
    MAX_REQUEST_BODY_BYTES=33554432 \
    REQUEST_TIMEOUT_SECS=300

VOLUME ["/data"]
EXPOSE 5000

# TOKEN_SIGNING_KEY is not baked into the image. `serve` refuses to start
# until the operator sets one (compose sets a loopback-only dev key).
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:5000/healthz || exit 1

USER registry
ENTRYPOINT ["/usr/local/bin/naust"]
CMD ["server"]
