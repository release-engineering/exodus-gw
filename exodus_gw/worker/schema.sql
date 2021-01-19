-- This schema comes from dramatiq-pg.
-- It was bundled here due to issue https://gitlab.com/dalibo/dramatiq-pg/-/issues/13
-- which prevents access to the intended public API for getting the schema.

CREATE SCHEMA dramatiq;

CREATE TYPE dramatiq."state" AS ENUM (
  'queued',
  'consumed',
  'rejected',
  'done'
);

CREATE TABLE dramatiq.queue(
  message_id uuid PRIMARY KEY,
  queue_name TEXT NOT NULL DEFAULT 'default',
  "state" dramatiq."state",
  mtime TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
  -- message as encoded by dramatiq.
  message JSONB,
  "result" JSONB,
  result_ttl  TIMESTAMP WITH TIME ZONE
) WITHOUT OIDS;

-- Index state and mtime together to speed up deletion. This can also speed up
-- statistics when VACUUM ANALYZE is recent enough.
CREATE INDEX ON dramatiq.queue("state", mtime);
