#!/usr/bin/env bash
set -euo pipefail

IMAGE="docker.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
CONTAINER_NAME="minio-live-test"
PORT="${MINIO_PORT:-9000}"

ENGINE=""
if command -v podman >/dev/null 2>&1; then
    ENGINE="podman"
elif command -v docker >/dev/null 2>&1; then
    ENGINE="docker"
else
    echo "Neither podman nor docker found on PATH" >&2
    exit 1
fi

echo "Starting MinIO test container using $ENGINE ($IMAGE on port $PORT)..."
$ENGINE rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
$ENGINE run -d \
    --name "$CONTAINER_NAME" \
    -p "127.0.0.1:${PORT}:9000" \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    "$IMAGE" server /data >/dev/null

echo "Waiting for MinIO health endpoint on http://127.0.0.1:${PORT}/minio/health/live..."
for i in $(seq 1 30); do
    if curl -s -f "http://127.0.0.1:${PORT}/minio/health/live" >/dev/null 2>&1; then
        echo "MinIO is ready on http://127.0.0.1:${PORT}"
        exit 0
    fi
    sleep 0.5
done

echo "MinIO failed to start within 15 seconds" >&2
$ENGINE logs "$CONTAINER_NAME" || true
exit 1
