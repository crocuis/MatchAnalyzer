import { describe, expect, it } from "vitest";

import { getSupabaseClient } from "../lib/supabase";

describe("supabase boundary", () => {
  it("returns null without required bindings", () => {
    expect(getSupabaseClient({})).toBeNull();
  });

  it("creates a client when bindings are present", () => {
    const client = getSupabaseClient({
      SUPABASE_URL: "https://example.supabase.co",
      SUPABASE_SERVICE_ROLE_KEY: "service-role-key",
    });

    expect(client).not.toBeNull();
  });
});
