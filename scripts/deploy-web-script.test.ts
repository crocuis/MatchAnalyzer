import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

describe("deploy:web script", () => {
  it("passes ASCII commit metadata to Cloudflare Pages", () => {
    const packageJson = JSON.parse(
      readFileSync(new URL("../package.json", import.meta.url), "utf8"),
    ) as { scripts: Record<string, string> };

    const deployWeb = packageJson.scripts["deploy:web"];

    expect(deployWeb).toContain('COMMIT_HASH="${DEPLOY_SHA:-$(git rev-parse HEAD)}"');
    expect(deployWeb).toContain('--commit-hash "$COMMIT_HASH"');
    expect(deployWeb).toContain('--commit-message "Deploy $COMMIT_HASH"');
  });
});
