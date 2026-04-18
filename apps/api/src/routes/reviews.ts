import { Hono } from "hono";

import type { AppBindings } from "../env";

const reviews = new Hono<AppBindings>();

reviews.get("/:matchId", (c) =>
  c.json({
    matchId: c.req.param("matchId"),
    review: null,
  }),
);

export default reviews;
