import client from "prom-client";

export const register = new client.Registry();
client.collectDefaultMetrics({ register }); // CPU, memory, event loop lag, etc.

// --- The metrics that actually tell the story of this system ---

export const eventsProcessed = new client.Counter({
  name: "worker_events_processed_total",
  help: "Total events processed, broken down by outcome",
  labelNames: ["outcome"], // delivered | duplicate | rate_limited | dlq
});

// Full pipeline time: dedup check + rate limit check + (if applicable)
// Postgres claim + delivery. Useful for "how fast can a worker chew through
// its queue" — but NOT a fair measure of delivery speed alone, since most
// duplicates/rate-limits exit in ~1ms and would drag any delivery-focused
// average down misleadingly.
export const totalProcessingDuration = new client.Histogram({
  name: "worker_processing_duration_seconds",
  help: "Time from receiving a message to any final outcome (includes early exits)",
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
});

// ONLY the time spent in attemptDelivery() — i.e. only counted for events
// that actually reached a real delivery attempt. This is the honest
// "delivery latency" number, uncontaminated by fast-path skips.
export const deliveryAttemptDuration = new client.Histogram({
  name: "worker_delivery_attempt_duration_seconds",
  help: "Time spent specifically in the simulated delivery call (retries included), only recorded for events that reached delivery",
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5],
});

export const dlqTotal = new client.Counter({
  name: "worker_dlq_total",
  help: "Total events sent to the dead-letter queue after exhausting retries",
});

register.registerMetric(eventsProcessed);
register.registerMetric(totalProcessingDuration);
register.registerMetric(deliveryAttemptDuration);
register.registerMetric(dlqTotal);