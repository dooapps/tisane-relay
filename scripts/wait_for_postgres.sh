#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-5432}"
USER_NAME="${3:-test}"
MAX_ATTEMPTS="${4:-60}"

is_ready() {
  if command -v pg_isready >/dev/null 2>&1; then
    pg_isready -h "${HOST}" -p "${PORT}" -U "${USER_NAME}" >/dev/null 2>&1
    return $?
  fi

  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="${PGPASSWORD:-test}" psql -h "${HOST}" -p "${PORT}" -U "${USER_NAME}" -c '\q' >/dev/null 2>&1
    return $?
  fi

  if command -v docker >/dev/null 2>&1 && docker compose ps postgres >/dev/null 2>&1; then
    docker compose exec -T postgres pg_isready -U "${USER_NAME}" >/dev/null 2>&1
    return $?
  fi

  (echo > "/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1
}

for i in $(seq 1 "${MAX_ATTEMPTS}"); do
  if is_ready; then
    echo "Postgres ready"
    exit 0
  fi

  echo "Waiting for Postgres... (${i}/${MAX_ATTEMPTS})"
  sleep 1
done

echo "Postgres did not become ready in time" >&2
exit 1
