import { defineConfig } from "drizzle-kit";
import path from "node:path";
import os from "node:os";

export default defineConfig({
  dialect: "sqlite",
  schema: "./state/schema.ops.js",
  out: "./.migrations/ops",
  dbCredentials: {
    url: path.join(os.homedir(), ".openclaw", "ops.sqlite"),
  },
});
