import { Hono } from "hono";
import { cors } from "hono/cors";

import type { AppBindings } from "./env";
import dailyPicks from "./routes/daily-picks";
import matches from "./routes/matches";
import predictions from "./routes/predictions";
import reviews from "./routes/reviews";
import rollouts from "./routes/rollouts";

const app = new Hono<AppBindings>();

app.use(
  "*",
  cors({
    origin: (origin) => origin ?? "*",
    allowMethods: ["GET", "OPTIONS"],
  }),
);

app.get("/health", (c) => c.json({ ok: true }));
app.route("/daily-picks", dailyPicks);
app.route("/matches", matches);
app.route("/predictions", predictions);
app.route("/reviews", reviews);
app.route("/rollouts", rollouts);

export default app;
