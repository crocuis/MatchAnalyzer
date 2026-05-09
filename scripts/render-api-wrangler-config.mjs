import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import process from "node:process";

const repoRoot = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
const apiDir = path.join(repoRoot, "apps/api");
const sourcePath = path.join(apiDir, "wrangler.toml");
const outputPath = path.join(apiDir, "wrangler.hyperdrive.toml");

const hyperdriveId =
  process.env.CLOUDFLARE_HYPERDRIVE_ID ?? process.env.HYPERDRIVE_ID;

if (!hyperdriveId) {
  throw new Error(
    "CLOUDFLARE_HYPERDRIVE_ID is required to deploy the API with Hyperdrive.",
  );
}

const localConnectionString =
  process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE;

const baseConfig = await readFile(sourcePath, "utf8");
const withoutExistingHyperdrive = baseConfig.replace(
  /\n\[\[hyperdrive\]\][\s\S]*?(?=\n\[[^\[]|\n$|$)/g,
  "",
);
const hyperdriveConfig = [
  "",
  "[[hyperdrive]]",
  'binding = "HYPERDRIVE"',
  `id = ${JSON.stringify(hyperdriveId)}`,
  ...(localConnectionString
    ? [`localConnectionString = ${JSON.stringify(localConnectionString)}`]
    : []),
  "",
].join("\n");

await writeFile(outputPath, `${withoutExistingHyperdrive.trimEnd()}${hyperdriveConfig}`);

console.log(`Rendered API Wrangler config with Hyperdrive binding: ${outputPath}`);
