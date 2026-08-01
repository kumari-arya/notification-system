import express from "express";
import { register } from "./metrics.js";

const PORT = Number(process.env.PORT ?? 3001);

export function startHttpServer() {
  const app = express();

  app.get("/healthz", (_req, res) => res.status(200).send("ok"));

  app.get("/metrics", async (_req, res) => {
    res.set("Content-Type", register.contentType);
    res.send(await register.metrics());
  });

  app.listen(PORT, () => {
    console.log(JSON.stringify({ msg: "worker http server listening", port: PORT }));
  });
}