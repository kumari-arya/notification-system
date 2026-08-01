import { z } from "zod";

// What a caller (frontend or another service) sends us.
// Notice event_id and created_at are NOT provided by the caller —
// the server owns those, so a caller can never spoof/collide an ID.
export const IncomingEvent = z.object({
  user_id: z.string().min(1),
  channel: z.enum(["email", "sms", "push"]),
  type: z.string().min(1),          // e.g. "order.placed"
  payload: z.record(z.any()).default({}),
});
export type IncomingEvent = z.infer<typeof IncomingEvent>;

// What actually goes onto the Kafka topic — server-generated fields added.
export const NotificationEvent = IncomingEvent.extend({
  event_id: z.string().uuid(),
  created_at: z.string().datetime(),
});
export type NotificationEvent = z.infer<typeof NotificationEvent>;