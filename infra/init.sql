-- Runs automatically the first time the postgres container starts
-- (docker-entrypoint-initdb.d convention — only fires on a fresh volume).

CREATE TABLE IF NOT EXISTS delivery_status (
  id SERIAL PRIMARY KEY,
  event_id UUID NOT NULL,
  channel TEXT NOT NULL,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL,             -- attempting | delivered | failed | rate_limited
  attempt INT DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_id, channel)        -- the real dedup correctness gate
);

CREATE INDEX IF NOT EXISTS idx_delivery_user_time
  ON delivery_status (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_delivery_status_lookup
  ON delivery_status (event_id);

-- quick sanity marker so you can confirm this script actually ran
INSERT INTO delivery_status (event_id, channel, user_id, status)
VALUES ('00000000-0000-0000-0000-000000000000', 'email', 'seed-check', 'delivered')
ON CONFLICT (event_id, channel) DO NOTHING;