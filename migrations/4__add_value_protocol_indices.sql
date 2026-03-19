-- Migration: Add indices for Value Protocol

-- 1. Index for querying events by type and time (e.g. "give me all reads since yesterday")
CREATE INDEX IF NOT EXISTS events_type_time_idx ON events (event_type, occurred_at);

-- 2. Index for querying events by content_id inside JSONB (e.g. "give me stats for this article")
-- We use a B-tree expression index for specific key extraction, or GIN. 
-- For simple equality checks on a known key, a B-tree expression index is often smaller/faster than full GIN.
CREATE INDEX IF NOT EXISTS events_content_id_idx ON events ((payload_json->>'content_id'));
