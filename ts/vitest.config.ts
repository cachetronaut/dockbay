import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@dockbay/core": fileURLToPath(new URL("packages/core/src/index.ts", import.meta.url)),
      "@dockbay/convex": fileURLToPath(new URL("packages/convex/src/index.ts", import.meta.url)),
      "@dockbay/memory": fileURLToPath(new URL("packages/memory/src/index.ts", import.meta.url)),
      "@dockbay/postgres": fileURLToPath(
        new URL("packages/postgres/src/index.ts", import.meta.url),
      ),
    },
  },
});
