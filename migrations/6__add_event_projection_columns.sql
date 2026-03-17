-- Migration: add stored projections for relay feed/discovery queries

-- These generated columns keep the event ledger canonical while making the
-- distillery hot path cheaper to filter and aggregate inside Postgres.
ALTER TABLE events
ADD COLUMN IF NOT EXISTS candidate_id_resolved TEXT
GENERATED ALWAYS AS (COALESCE(content_id, payload_json->>'content_id')) STORED;

ALTER TABLE events
ADD COLUMN IF NOT EXISTS surface_scope TEXT
GENERATED ALWAYS AS (payload_json->>'surface') STORED;

ALTER TABLE events
ADD COLUMN IF NOT EXISTS account_scope TEXT
GENERATED ALWAYS AS (payload_json->>'account_id') STORED;

ALTER TABLE events
ADD COLUMN IF NOT EXISTS channel_scope TEXT
GENERATED ALWAYS AS (payload_json->>'channel') STORED;

ALTER TABLE events
ADD COLUMN IF NOT EXISTS snapshot_score DOUBLE PRECISION
GENERATED ALWAYS AS (
    CASE
        WHEN event_type = 'value.snapshot'
        THEN NULLIF(payload_json->>'score', '')::double precision
        ELSE NULL
    END
) STORED;

CREATE INDEX IF NOT EXISTS events_distillery_scope_projection_idx
ON events (surface_scope, account_scope, channel_scope, occurred_at DESC)
WHERE event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot');

CREATE INDEX IF NOT EXISTS events_distillery_candidate_projection_idx
ON events (candidate_id_resolved, occurred_at DESC)
WHERE event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot');
