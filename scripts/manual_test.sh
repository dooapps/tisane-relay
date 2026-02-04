#!/usr/bin/env bash
set -euo pipefail

# Manual test: starts server and runs simple curl tests
# Requires: docker, docker-compose, curl, jq

export DATABASE_URL="postgres://test:test@127.0.0.1:5432/tisane_relay_test"
export PORT=${PORT:-8080}

echo "Bringing up Postgres..."
docker compose up -d postgres

# Run server in background
echo "Starting server (cargo run)..."
cargo run --quiet &
SERVER_PID=$!

cleanup() {
  echo "Stopping server..."
  kill ${SERVER_PID} || true
  echo "Stopping Postgres..."
  docker compose down -v || true
}
trap cleanup EXIT

# Wait for server to be ready
for i in {1..30}; do
  if curl -sSf -o /dev/null "http://127.0.0.1:${PORT}/health"; then
    echo "Server ready"
    break
  fi
  echo "Waiting for server... ($i)"
  sleep 1
done

# 1) GET /health
echo "GET /health"
curl -sS "http://127.0.0.1:${PORT}/health" | jq .

# 2) POST /relay/push
echo "POST /relay/push"
EVENT_ID=$(python3 - <<'PY'
import uuid, json
print(str(uuid.uuid4()))
PY
)
cat <<JSON | curl -sS -X POST "http://127.0.0.1:${PORT}/relay/push" -H "Content-Type: application/json" -d @- | jq .
[
  {
    "event_id": "${EVENT_ID}",
    "device_id": "dev-local",
    "author_id": "author-local",
    "content_id": "content-local",
    "event_type": "type-local",
    "payload_json": {"msg":"hello"},
    "occurred_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "lamport": 1
  }
]
JSON

# 3) GET /relay/pull?since=0
echo "GET /relay/pull?since=0"
curl -sS "http://127.0.0.1:${PORT}/relay/pull?since=0" | jq .

echo "Manual tests complete."
