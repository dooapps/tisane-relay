-- Migration: add focused indices for author discovery and ranking queries

CREATE INDEX IF NOT EXISTS events_distillery_author_projection_idx
ON events (surface_scope, account_scope, channel_scope, author_id, occurred_at DESC)
WHERE event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot')
  AND author_id IS NOT NULL;
