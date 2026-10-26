#!/usr/bin/env node
// bin/manifold-sidecar.js — run manifold as a standalone sidecar: own
// process, package-owned store, harness-agnostic HTTP surface. Harnesses
// (the openclaw companion, dashboards, CLIs) consume events/responses only
// and never open the database.
import { bootSidecar } from "../sidecar/server.js";

bootSidecar().catch((err) => {
  console.error(`manifold-sidecar: fatal: ${err?.message ?? err}`);
  process.exit(1);
});
