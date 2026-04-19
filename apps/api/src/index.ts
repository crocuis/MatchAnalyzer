import { Hono } from "hono";
import { cors } from "hono/cors";

import type { AppBindings } from "./env";
import matches from "./routes/matches";
import predictions from "./routes/predictions";
import reviews from "./routes/reviews";

const app = new Hono<AppBindings>();

app.use(
  "*",
  cors({
    origin: (origin) => origin ?? "*",
    allowMethods: ["GET", "OPTIONS"],
  }),
);

app.get("/health", (c) => c.json({ ok: true }));
app.route("/matches", matches);
app.route("/predictions", predictions);
app.route("/reviews", reviews);

export default app;
