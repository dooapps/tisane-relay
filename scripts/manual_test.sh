#!/usr/bin/env bash
set -euo pipefail

# Manual test: starts relay and runs simple curl tests.
# If Docker is available, starts the full relay dependencies.
# If Docker is unavailable, falls back to distillery-only mode.
# Requires: cargo, curl, jq

export DATABASE_URL="postgres://test:test@127.0.0.1:5432/tisane_relay_test"
export PORT=${PORT:-8080}
MODE="distillery-only"

if docker info >/dev/null 2>&1; then
  MODE="full-relay"
  echo "Bringing up Postgres..."
  docker compose up -d postgres

  echo "Waiting for Postgres..."
  export PGPASSWORD=test
  ./scripts/wait_for_postgres.sh 127.0.0.1 5432 test 60
else
  echo "Docker daemon indisponivel. Usando serve-distillery para smoke test local."
fi

# Run server in background
if [[ "${MODE}" == "full-relay" ]]; then
  echo "Starting server (cargo run -- serve)..."
  cargo run --quiet -- serve --port "${PORT}" --database-url "${DATABASE_URL}" &
else
  echo "Starting server (cargo run -- serve-distillery)..."
  cargo run --quiet -- serve-distillery --port "${PORT}" &
fi
SERVER_PID=$!

cleanup() {
  echo "Stopping server..."
  kill ${SERVER_PID} || true
  if [[ "${MODE}" == "full-relay" ]]; then
    echo "Stopping Postgres..."
    docker compose down -v || true
  fi
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

# 2) POST /distillery/rank
echo "POST /distillery/rank"
cat <<JSON | curl -sS -X POST "http://127.0.0.1:${PORT}/distillery/rank" -H "Content-Type: application/json" -d @- | jq .
{
  "surface": "discover",
  "account_id": "acct-local",
  "candidates": [
    {
      "candidate_id": "quiet",
      "author_id": "author-a",
      "channel": "essays",
      "read_completed": 3,
      "citation_created": 0,
      "derivative_created": 0,
      "value_snapshot": 0.0
    },
    {
      "candidate_id": "strong",
      "author_id": "author-b",
      "channel": "briefs",
      "read_completed": 1,
      "citation_created": 1,
      "derivative_created": 1,
      "value_snapshot": 2.0
    }
  ]
}
JSON

# 3) POST /distillery/distribute
echo "POST /distillery/distribute"
cat <<JSON | curl -sS -X POST "http://127.0.0.1:${PORT}/distillery/distribute" -H "Content-Type: application/json" -d @- | jq .
{
  "surface": "home",
  "account_id": "acct-local",
  "slot_count": 3,
  "max_per_author": 1,
  "max_per_channel": 2,
  "candidates": [
    {
      "candidate_id": "author-a-strong",
      "author_id": "author-a",
      "channel": "essays",
      "read_completed": 1,
      "citation_created": 1,
      "derivative_created": 1,
      "value_snapshot": 1.0
    },
    {
      "candidate_id": "author-a-second",
      "author_id": "author-a",
      "channel": "essays",
      "read_completed": 2,
      "citation_created": 1,
      "derivative_created": 0,
      "value_snapshot": 0.0
    },
    {
      "candidate_id": "author-b-alt",
      "author_id": "author-b",
      "channel": "briefs",
      "read_completed": 1,
      "citation_created": 0,
      "derivative_created": 0,
      "value_snapshot": 0.0
    }
  ]
}
JSON

if [[ "${MODE}" == "full-relay" ]]; then
  echo "Relay completo iniciado com Postgres."
else
  echo "Smoke test executado em modo distillery-only."
fi

echo "Manual tests complete."
