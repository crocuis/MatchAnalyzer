import { execFileSync } from "node:child_process";
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

describe("render-api-wrangler-config script", () => {
  it("renders a deployable config without a fresh Hyperdrive id", () => {
    execFileSync(
      "node",
      ["scripts/render-api-wrangler-config.mjs"],
      {
        cwd: new URL("..", import.meta.url),
        env: {
          ...process.env,
          CLOUDFLARE_HYPERDRIVE_ID: "cached-hyperdrive-id",
          CLOUDFLARE_HYPERDRIVE_FRESH_ID: "",
          HYPERDRIVE_FRESH_ID: "",
        },
        stdio: "pipe",
      },
    );

    const rendered = readFileSync(
      new URL("../apps/api/wrangler.hyperdrive.toml", import.meta.url),
      "utf8",
    );

    expect(rendered).toContain('binding = "HYPERDRIVE"');
    expect(rendered).not.toContain('binding = "HYPERDRIVE_FRESH"');
  });
});
