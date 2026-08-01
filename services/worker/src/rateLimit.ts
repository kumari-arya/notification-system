import { redis } from "./redisClient.js";

// Atomic fixed-window rate limiter. INCR + EXPIRE done as one Lua script
// so there's no gap between "read the count" and "increment it" that two
// worker replicas could race through — same atomicity principle as the
// dedup NX check in dedup.ts, applied to counting instead of existence.
const RATE_LIMIT_SCRIPT = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCR", key)
if current == 1 then
  redis.call("EXPIRE", key, window)
end
if current > limit then
  return 0
else
  return 1
end
`;

const DEFAULT_LIMIT = 10; // max notifications per user+channel per window
const DEFAULT_WINDOW_SECONDS = 60;

/**
 * Returns true if this event is allowed to proceed, false if the user has
 * hit the rate limit for this channel in the current window.
 */
export async function checkRateLimit(
  userId: string,
  channel: string,
  limit: number = DEFAULT_LIMIT,
  windowSeconds: number = DEFAULT_WINDOW_SECONDS
): Promise<boolean> {
  const key = `rl:${userId}:${channel}`;
  const allowed = await redis.eval(
    RATE_LIMIT_SCRIPT,
    1,
    key,
    limit,
    windowSeconds
  );
  return allowed === 1;
}