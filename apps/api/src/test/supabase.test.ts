import { describe, expect, it } from "vitest";

import { getSupabaseClient } from "../lib/supabase";

describe("supabase boundary", () => {
  it("throws until the client is implemented", () => {
    expect(() => getSupabaseClient({})).toThrowError(/not implemented/i);
  });
});
