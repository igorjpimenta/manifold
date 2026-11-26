import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createManifoldToolHandlers, TOOL_DEFS } from "../../../mcp/tools.js";

const identity = { harness: "test-harness", agent: "a1", session: "sess-9" };

function handlers() {
  return createManifoldToolHandlers({ logger: { info() {}, warn() {}, error() {} } });
}
