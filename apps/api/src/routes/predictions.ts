import { Hono } from "hono";

import type { AppBindings } from "../env";

const predictions = new Hono<AppBindings>();

predictions.get("/:matchId", (c) =>
  c.json({
    matchId: c.req.param("matchId"),
    checkpoints: [],
  }),
);

export default predictions;
