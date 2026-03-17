#!/usr/bin/env bash
set -euo pipefail

# Start Postgres for local tests
echo "Starting local Postgres via docker-compose..."
docker compose up -d postgres

# Wait for Postgres to accept connections
export DATABASE_URL="postgres://test:test@127.0.0.1:5432/tisane_relay_test"
export PORT=8080

echo "Waiting for Postgres to be ready..."
for i in {1..60}; do
  if command -v pg_isready >/dev/null 2>&1; then
    pg_isready -h 127.0.0.1 -p 5432 -U test >/dev/null 2>&1 && { echo "Postgres ready"; break; }
  elif command -v psql >/dev/null 2>&1; then
    PGPASSWORD=test psql -h 127.0.0.1 -U test -c '\q' >/dev/null 2>&1 && { echo "Postgres ready"; break; }
  else
    # Fallback: try TCP connect
    (echo > /dev/tcp/127.0.0.1/5432) >/dev/null 2>&1 && { echo "Postgres ready"; break; } || true
  fi
  echo "Waiting... ($i)"
  sleep 1
done

# check final
if ! (command -v pg_isready >/dev/null 2>&1 && pg_isready -h 127.0.0.1 -p 5432 -U test >/dev/null 2>&1) && ! (command -v psql >/dev/null 2>&1 && PGPASSWORD=test psql -h 127.0.0.1 -U test -c '\q' >/dev/null 2>&1); then
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

# Tear down
echo "Stopping local Postgres..."
docker compose down -v

echo "Done." 
