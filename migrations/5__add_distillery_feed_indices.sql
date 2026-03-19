-- Migration: Add focused indices for Distillery aggregation queries

-- This partial index narrows the hot path for feed/discovery queries that
-- filter by surface/account/channel and recent time windows over Value Protocol
-- events only. Keeping it partial reduces write amplification on the relay.
CREATE INDEX IF NOT EXISTS events_distillery_scope_time_idx
ON events (
    (payload_json->>'surface'),
    (payload_json->>'account_id'),
    (payload_json->>'channel'),
    occurred_at DESC
)
WHERE event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot');

-- Distillery groups by the stable candidate id fallback and then orders by the
-- freshest signal seen for each candidate. This index supports that path while
-- remaining restricted to the same Value Protocol subset.
CREATE INDEX IF NOT EXISTS events_distillery_candidate_time_idx
ON events (
    (COALESCE(content_id, payload_json->>'content_id')),
    occurred_at DESC
)
WHERE event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot');
