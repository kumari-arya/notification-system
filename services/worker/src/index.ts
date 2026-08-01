import { Kafka, logLevel } from "kafkajs";
import { claimAttempt, markDelivered, markFailed, insertRateLimited } from "./db.js";
import { fastPathIsNew } from "./dedup.js";
import { checkRateLimit } from "./rateLimit.js";
import { attemptDelivery, publishToDlq } from "./deliver.js";
import { startHttpServer } from "./httpServer.js";
import {
  eventsProcessed,
  totalProcessingDuration,
  deliveryAttemptDuration,
  dlqTotal,
} from "./metrics.js";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:29092").split(",");
const TOPIC = "notification.events";
const GROUP_ID = "notification-workers";

const kafka = new Kafka({
  clientId: "worker",
  brokers,
  logLevel: logLevel.WARN,
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

async function commit(topic: string, partition: number, offset: string) {
  await consumer.commitOffsets([
    { topic, partition, offset: (Number(offset) + 1).toString() },
  ]);
}

async function run() {
  startHttpServer();

  await consumer.connect();
  console.log(JSON.stringify({ msg: "worker connected", brokers, group: GROUP_ID }));

  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  console.log(JSON.stringify({ msg: "subscribed", topic: TOPIC }));

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;
      const event = JSON.parse(raw);
      const { event_id, channel, user_id } = event;

      // Measures the WHOLE pipeline, every exit path — an honest
      // "total processing time" metric, not a "delivery speed" one.
      const endTotalTimer = totalProcessingDuration.startTimer();

      // --- Layer 1: Redis fast-path dedup ---
      const isNew = await fastPathIsNew(event_id);
      if (!isNew) {
        eventsProcessed.inc({ outcome: "duplicate" });
        endTotalTimer();
        console.log(JSON.stringify({ msg: "fast-path duplicate, skipping", event_id }));
        await commit(topic, partition, message.offset);
        return;
      }

      // --- Layer 2: Redis rate limit ---
      const allowed = await checkRateLimit(user_id, channel);
      if (!allowed) {
        await insertRateLimited(event_id, channel, user_id);
        eventsProcessed.inc({ outcome: "rate_limited" });
        endTotalTimer();
        console.log(JSON.stringify({ msg: "rate limited", event_id, user_id, channel }));
        await commit(topic, partition, message.offset);
        return;
      }

      // --- Layer 3: Postgres claim (the real correctness gate) ---
      const claimed = await claimAttempt(event_id, channel, user_id);
      if (!claimed) {
        eventsProcessed.inc({ outcome: "duplicate" });
        endTotalTimer();
        console.log(JSON.stringify({ msg: "duplicate (db-level), skipping", event_id }));
        await commit(topic, partition, message.offset);
        return;
      }

      // --- Layer 4: simulated delivery with retry/backoff ---
      // ONLY this stage feeds the delivery-specific histogram — the honest
      // "how slow is a real delivery attempt" number.
      const endDeliveryTimer = deliveryAttemptDuration.startTimer();
      const result = await attemptDelivery(event_id);
      endDeliveryTimer();
      endTotalTimer();

      if (result.success) {
        await markDelivered(event_id, channel);
        eventsProcessed.inc({ outcome: "delivered" });
        console.log(JSON.stringify({ msg: "delivered", event_id, channel }));
      } else {
        await markFailed(event_id, channel);
        await publishToDlq(event);
        eventsProcessed.inc({ outcome: "dlq" });
        dlqTotal.inc();
        console.log(
          JSON.stringify({ msg: "sent to DLQ", event_id, channel, attempts: result.attempts })
        );
      }

      await commit(topic, partition, message.offset);
    },
  });
}

run().catch((err) => {
  console.error(JSON.stringify({ msg: "worker fatal error", error: String(err) }));
  process.exit(1);
});