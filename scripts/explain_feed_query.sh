#!/usr/bin/env bash
set -euo pipefail

echo "Starting local Postgres via docker-compose..."
docker compose up -d postgres

cleanup() {
  echo "Stopping local Postgres..."
  docker compose down -v || true
}
trap cleanup EXIT

export PGPASSWORD=test
./scripts/wait_for_postgres.sh 127.0.0.1 5432 test 60

echo "Preparing schema..."
DATABASE_URL="postgres://test:test@127.0.0.1:5432/tisane_relay_test" cargo test test_aggregate_candidate_signals --test integration_tests -- --nocapture >/tmp/tisane_explain_seed.log

echo "Seeding synthetic feed load..."
docker compose exec -T postgres psql -U test -d tisane_relay_test <<'SQL'
TRUNCATE TABLE events;

INSERT INTO events (
    event_id,
    author_pubkey,
    signature,
    payload_hash,
    device_id,
    author_id,
    content_id,
    event_type,
    payload_json,
    occurred_at,
    lamport
)
SELECT
    (
        substr(md5('event-' || series_id::text), 1, 8) || '-' ||
        substr(md5('event-' || series_id::text), 9, 4) || '-' ||
        substr(md5('event-' || series_id::text), 13, 4) || '-' ||
        substr(md5('event-' || series_id::text), 17, 4) || '-' ||
        substr(md5('event-' || series_id::text), 21, 12)
    )::uuid,
    repeat('a', 64),
    repeat('b', 128),
    repeat('c', 64),
    'device-local',
    'author-' || ((series_id - 1) % 40),
    'content-' || ((series_id - 1) % 400),
    CASE series_id % 4
        WHEN 0 THEN 'read.completed'
        WHEN 1 THEN 'citation.created'
        WHEN 2 THEN 'derivative.created'
        ELSE 'value.snapshot'
    END,
    jsonb_build_object(
        'content_id', 'content-' || ((series_id - 1) % 400),
        'channel', CASE series_id % 3
            WHEN 0 THEN 'essays'
            WHEN 1 THEN 'briefs'
            ELSE 'video'
        END,
        'surface', CASE series_id % 2
            WHEN 0 THEN 'home'
            ELSE 'desk'
        END,
        'account_id', CASE series_id % 2
            WHEN 0 THEN 'acct-1'
            ELSE 'acct-2'
        END,
        'score', ((series_id % 100)::double precision / 10.0)
    ),
    NOW() - make_interval(secs => series_id),
    series_id
FROM generate_series(1, 12000) AS seed(series_id);
SQL

echo "EXPLAIN ANALYZE for canonical feed aggregation:"
docker compose exec -T postgres psql -U test -d tisane_relay_test <<'SQL'
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    candidate_id_resolved AS candidate_id,
    MAX(author_id) AS author_id,
    MAX(channel_scope) AS channel,
    COALESCE(SUM(CASE WHEN event_type = 'read.completed' THEN 1 ELSE 0 END), 0) AS read_completed,
    COALESCE(SUM(CASE WHEN event_type = 'citation.created' THEN 1 ELSE 0 END), 0) AS citation_created,
    COALESCE(SUM(CASE WHEN event_type = 'derivative.created' THEN 1 ELSE 0 END), 0) AS derivative_created,
    COALESCE(MAX(snapshot_score), 0.0) AS value_snapshot
FROM events
WHERE candidate_id_resolved IS NOT NULL
  AND event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot')
  AND surface_scope = 'home'
  AND account_scope = 'acct-1'
GROUP BY candidate_id_resolved
ORDER BY MAX(occurred_at) DESC, candidate_id ASC
LIMIT 50;
SQL
