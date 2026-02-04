-- Migration: create peers table and prepare events cursor

-- 1. Create peers table
CREATE TABLE IF NOT EXISTS peers (
    peer_id UUID PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    shared_secret TEXT NOT NULL,
    last_cursor_time TIMESTAMPTZ DEFAULT '-infinity',
    last_cursor_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    health TEXT DEFAULT 'unknown',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Backfill occurred_at if null (defaulting to transaction timestamp)
UPDATE events SET occurred_at = NOW() WHERE occurred_at IS NULL;

-- 3. Make occurred_at NOT NULL
ALTER TABLE events ALTER COLUMN occurred_at SET NOT NULL;

-- 4. Create index for cursor paging performance
CREATE INDEX IF NOT EXISTS events_replication_cursor_idx ON events (occurred_at, event_id);
