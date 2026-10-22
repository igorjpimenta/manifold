import { defineConfig } from "drizzle-kit";
import path from "node:path";
import os from "node:os";

export default defineConfig({
  dialect: "sqlite",
  schema: "./state/schema.js",
  out: "./.migrations/runtime",
  dbCredentials: {
    url: path.join(os.homedir(), ".openclaw", "runtime.sqlite"),
  },
});
