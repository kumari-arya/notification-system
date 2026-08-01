import { redis } from "./redisClient.js";

/**
 * FAST-PATH dedup check. This is an OPTIMIZATION, not the correctness
 * guarantee — that's Postgres's UNIQUE(event_id, channel) constraint in
 * db.ts. This just lets us skip an obviously-already-seen event cheaply,
 * before paying for a rate-limit check + a Postgres round trip.
 *
 * Returns true if this looks new (go ahead and do the real work),
 * false if we've already seen this event_id very recently.
 *
 * Short TTL (5 min) is deliberate: this key's only job is to catch
 * duplicates that show up within a normal retry/rebalance window, not to
 * be a permanent record — permanence is Postgres's job.
 */
export async function fastPathIsNew(eventId: string): Promise<boolean> {
  const result = await redis.set(`dedup:${eventId}`, "1", "EX", 300, "NX");
  return result === "OK";
}