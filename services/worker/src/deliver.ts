import { Kafka, logLevel, Producer } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:29092").split(",");
const DLQ_TOPIC = "notification.events.dlq";
const MAX_ATTEMPTS = 3;
const BASE_BACKOFF_MS = 200; // doubles each retry: 200ms, 400ms, 800ms

const kafka = new Kafka({ clientId: "worker-dlq-producer", brokers, logLevel: logLevel.WARN });
let dlqProducer: Producer | null = null;

async function getDlqProducer(): Promise<Producer> {
  if (!dlqProducer) {
    dlqProducer = kafka.producer();
    await dlqProducer.connect();
  }
  return dlqProducer;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Simulates calling a real delivery provider (email/SMS/push).
 * ~85% success rate, deliberately random, to give retry logic something
 * real to react to. In production this is where you'd call SendGrid,
 * Twilio, FCM, etc.
 */
async function simulateProviderCall(): Promise<void> {
  await sleep(20 + Math.random() * 80); // pretend network latency
  if (Math.random() < 0.15) {
    throw new Error("simulated provider failure");
  }
}

export type DeliveryResult = { success: true } | { success: false; attempts: number };

/**
 * Attempts delivery up to MAX_ATTEMPTS times with exponential backoff.
 * Runs IN-PROCESS, meaning this blocks processing of the next message on
 * this partition until it resolves — a deliberate simplicity trade-off for
 * this project's scale. At real production scale, a persistently-failing
 * event should instead be handed off to a separate retry topic/scheduler so
 * one bad event can't stall an entire partition's throughput.
 */
export async function attemptDelivery(eventId: string): Promise<DeliveryResult> {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await simulateProviderCall();
      return { success: true };
    } catch (err) {
      const isLastAttempt = attempt === MAX_ATTEMPTS;
      console.log(
        JSON.stringify({
          msg: isLastAttempt ? "delivery failed, exhausted retries" : "delivery attempt failed, retrying",
          event_id: eventId,
          attempt,
        })
      );
      if (!isLastAttempt) {
        const backoff = BASE_BACKOFF_MS * 2 ** (attempt - 1);
        await sleep(backoff);
      }
    }
  }
  return { success: false, attempts: MAX_ATTEMPTS };
}

/** Publishes an exhausted event to the DLQ topic for later inspection. */
export async function publishToDlq(event: unknown) {
  const producer = await getDlqProducer();
  await producer.send({
    topic: DLQ_TOPIC,
    messages: [{ value: JSON.stringify(event) }],
  });
}