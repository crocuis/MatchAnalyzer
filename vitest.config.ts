import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    exclude: [
      "**/.codex/**",
      "**/node_modules/**",
      "**/dist/**",
      "**/cypress/**",
      "**/.{idea,git,cache,output,temp}/**",
    ],
  },
});
