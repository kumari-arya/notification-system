import { Kafka, logLevel } from "kafkajs";
import type { NotificationEvent } from "./schema.js";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:29092").split(",");

export const kafka = new Kafka({
  clientId: "ingest-api",
  brokers,
  logLevel: logLevel.WARN, // keep kafkajs's own logs quiet; we log ourselves
});

export const producer = kafka.producer();

export async function connectProducer() {
  await producer.connect();
  console.log(JSON.stringify({ msg: "kafka producer connected", brokers }));
}

export async function publishEvent(topic: string, event: NotificationEvent) {
  // Keying by user_id is what guarantees all of this user's events land in
  // the same partition, in order — this is the thing that makes the
  // "no concurrent rate-limit race under normal operation" property true.
  await producer.send({
    topic,
    messages: [
      {
        key: event.user_id,
        value: JSON.stringify(event),
      },
    ],
  });
}