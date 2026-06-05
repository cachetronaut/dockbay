import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "packages/core/src/index.ts",
    convex: "packages/convex/src/index.ts",
    memory: "packages/memory/src/index.ts",
    postgres: "packages/postgres/src/index.ts",
  },
  format: "esm",
  dts: true,
  splitting: true,
  clean: true,
  outDir: "dist",
  target: "es2022",
});
