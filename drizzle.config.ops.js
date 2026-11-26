import { defineConfig } from "drizzle-kit";
import { resolveDataDir, opsDbPath } from "./state/paths.js";

export default defineConfig({
  dialect: "sqlite",
  schema: "./state/schema.ops.js",
  out: "./.migrations/ops",
  dbCredentials: {
    url: opsDbPath(resolveDataDir()),
  },
});
