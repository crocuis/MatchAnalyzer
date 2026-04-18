import { Hono } from "hono";

import type { AppBindings } from "../env";

const matches = new Hono<AppBindings>();

matches.get("/", (c) => c.json({ items: [] }));

export default matches;
