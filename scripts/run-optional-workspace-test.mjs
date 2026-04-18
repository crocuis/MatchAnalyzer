import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";

const workspace = process.argv[2];

if (!workspace) {
  console.error("Missing workspace path.");
  process.exit(1);
}

const packageJsonPath = new URL(`../${workspace}/package.json`, import.meta.url);

if (!existsSync(packageJsonPath)) {
  console.log(`N/A: ${workspace} workspace is absent, skipping tests.`);
  process.exit(0);
}

const result = spawnSync("npm", ["--workspace", workspace, "run", "test"], {
  stdio: "inherit",
});

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}

process.exit(result.status ?? 1);
