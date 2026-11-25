import { defineConfig } from "drizzle-kit";
import { resolveDataDir, runtimeDbPath } from "./state/paths.js";

export default defineConfig({
  dialect: "sqlite",
  schema: "./state/schema.js",
  out: "./.migrations/runtime",
  dbCredentials: {
    url: runtimeDbPath(resolveDataDir()),
  },
});
