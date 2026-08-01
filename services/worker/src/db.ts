import pg from "pg";

const { Pool } = pg;

// Local dev default matches the credentials from infra/docker-compose.yml.
// Note port 5432 (Postgres's normal port — nothing special like Kafka's
// dual-listener setup, since Postgres doesn't have an internal-vs-host split
// the way Kafka does here).
const connectionString =
  process.env.DATABASE_URL ??
  "postgres://notifuser:devpass@localhost:5432/notifications";

export const pool = new Pool({ connectionString });

/**
 * The real correctness gate (from our design discussion).
 * Returns true if THIS call is the one that gets to actually do the work,
 * false if some earlier attempt (even a crashed one) already claimed it —
 * because the UNIQUE(event_id, channel) constraint + ON CONFLICT DO NOTHING
 * makes "claim" and "durably record" the same atomic operation.
 */
export async function claimAttempt(
  eventId: string,
  channel: string,
  userId: string
): Promise<boolean> {
  const result = await pool.query(
    `INSERT INTO delivery_status (event_id, channel, user_id, status)
     VALUES ($1, $2, $3, 'attempting')
     ON CONFLICT (event_id, channel) DO NOTHING`,
    [eventId, channel, userId]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function markDelivered(eventId: string, channel: string) {
  await pool.query(
    `UPDATE delivery_status
     SET status = 'delivered', updated_at = now()
     WHERE event_id = $1 AND channel = $2`,
    [eventId, channel]
  );
}

export async function markFailed(eventId: string, channel: string) {
  await pool.query(
    `UPDATE delivery_status
     SET status = 'failed', updated_at = now()
     WHERE event_id = $1 AND channel = $2`,
    [eventId, channel]
  );
}

export async function insertRateLimited(
  eventId: string,
  channel: string,
  userId: string
): Promise<boolean> {
  const result = await pool.query(
    `INSERT INTO delivery_status (event_id, channel, user_id, status)
     VALUES ($1, $2, $3, 'rate_limited')
     ON CONFLICT (event_id, channel) DO NOTHING`,
    [eventId, channel, userId]
  );
  return (result.rowCount ?? 0) > 0;
}