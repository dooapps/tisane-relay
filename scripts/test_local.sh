#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  echo "Stopping local Postgres..."
  docker compose down -v || true
}
trap cleanup EXIT

# Start Postgres for local tests
echo "Starting local Postgres via docker-compose..."
docker compose up -d postgres

# Wait for Postgres to accept connections
export DATABASE_URL="postgres://test:test@127.0.0.1:5432/tisane_relay_test"
export PORT=8080
export PGPASSWORD=test

echo "Waiting for Postgres to be ready..."
if ! ./scripts/wait_for_postgres.sh 127.0.0.1 5432 test 60; then
  echo "Postgres did not become ready in time" >&2
  docker compose logs postgres || true
  docker compose down -v || true
  exit 1
fi

# Run cargo test for distillery and relay
echo "Running distillery tests..."
cargo test --manifest-path ../distillery/Cargo.toml

echo "Running tisane-relay tests..."
cargo test --tests

echo "Done." 
