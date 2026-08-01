import express from "express";
import { randomUUID } from "crypto";
import client from "prom-client";
import { IncomingEvent, NotificationEvent } from "./schema.js";
import { connectProducer, publishEvent } from "./kafka.js";

const TOPIC = "notification.events";
const PORT = Number(process.env.PORT ?? 3000);

const app = express();
app.use(express.json());

// --- Prometheus metrics setup ---
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const eventsAccepted = new client.Counter({
  name: "ingest_events_accepted_total",
  help: "Total events accepted and published to Kafka",
  labelNames: ["channel"],
});
const eventsRejected = new client.Counter({
  name: "ingest_events_rejected_total",
  help: "Total events rejected due to validation failure",
});
const requestDuration = new client.Histogram({
  name: "ingest_request_duration_seconds",
  help: "POST /events request duration",
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
});
register.registerMetric(eventsAccepted);
register.registerMetric(eventsRejected);
register.registerMetric(requestDuration);

// --- Routes ---
app.post("/events", async (req, res) => {
  const end = requestDuration.startTimer();

  const parsed = IncomingEvent.safeParse(req.body);
  if (!parsed.success) {
    eventsRejected.inc();
    end();
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const event: NotificationEvent = {
    ...parsed.data,
    event_id: randomUUID(),
    created_at: new Date().toISOString(),
  };

  try {
    await publishEvent(TOPIC, event);
    eventsAccepted.inc({ channel: event.channel });
    end();
    // 202 Accepted, not 200/201 — signals "received, not yet completed,"
    // which is the correct status code for an async hand-off like this.
    return res.status(202).json({ event_id: event.event_id });
  } catch (err) {
    end();
    console.error(JSON.stringify({ msg: "publish failed", error: String(err) }));
    return res.status(503).json({ error: "could not publish event, try again" });
  }
});

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", register.contentType);
  res.send(await register.metrics());
});

async function main() {
  await connectProducer();
  app.listen(PORT, () => {
    console.log(JSON.stringify({ msg: "ingest-api listening", port: PORT }));
  });
}

main().catch((err) => {
  console.error(JSON.stringify({ msg: "fatal startup error", error: String(err) }));
  process.exit(1);
});