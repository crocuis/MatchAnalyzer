import { Hono } from "hono";

import type { AppBindings } from "./env";
import matches from "./routes/matches";
import predictions from "./routes/predictions";
import reviews from "./routes/reviews";

const app = new Hono<AppBindings>();

app.get("/health", (c) => c.json({ ok: true }));
app.route("/matches", matches);
app.route("/predictions", predictions);
app.route("/reviews", reviews);

export default app;
