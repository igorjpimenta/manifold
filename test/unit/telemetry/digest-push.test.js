/**
 * Unit tests for telemetry/digest-push.js
 *
 * Verifies the manifold.digest payload shape, the apns-runtime wiring
 * (unconfigured / no-tokens / terminal-drop paths), and that failures
 * never throw — callers only branch on the returned reason.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { sendDigestPush, configureApi } from "../../../telemetry/digest-push.js";

function fakeApns(overrides = {}) {
  const calls = { send: [], dropped: [] };
  return {
    calls,
    apns: {
      isConfigured: () => true,
      listTokens: () => [
        { device_id: "d1", apns_token: "tok-1", environment: "production" },
      ],
      send: async (args) => { calls.send.push(args); return { ok: true }; },
      dropToken: async ({ apnsToken }) => { calls.dropped.push(apnsToken); },
      ...overrides,
    },
  };
}

describe("telemetry/digest-push", () => {
  test("unconfigured apns short-circuits with a reason", async () => {
    configureApi({ runtime: {} });
    assert.deepEqual(await sendDigestPush({ title: "t", body: "b" }), { ok: false, reason: "not_configured" });
    configureApi({ runtime: { apns: { isConfigured: () => false } } });
    assert.deepEqual(await sendDigestPush({ title: "t", body: "b" }), { ok: false, reason: "not_configured" });
  });

  test("sends manifold.digest payloads per token with its environment", async () => {
    const { apns, calls } = fakeApns();
    configureApi({ runtime: { apns } });
    const out = await sendDigestPush({ title: "Weekly", body: "funnel" });
    assert.equal(out.ok, true);
    assert.equal(out.sent, 1);
    const [call] = calls.send;
    assert.equal(call.pushType, "alert");
    assert.equal(call.environment, "production");
    assert.equal(call.payload.kind, "manifold.digest");
    assert.equal(call.payload.aps.category, "manifold.digest");
    assert.equal(call.payload.aps.alert.title, "Weekly");
  });

  test("terminal tokens are dropped; throws never escape", async () => {
    const { apns, calls } = fakeApns({
      send: async (args) => {
        calls.send.push(args);
        if (args.deviceToken === "tok-1") return { ok: false, status: 400, reason: "BadDeviceToken" };
        throw new Error("down");
      },
    });
    configureApi({ runtime: { apns } });
    const out = await sendDigestPush({ title: "t", body: "b" });
    assert.equal(out.ok, false);
    assert.deepEqual(calls.dropped, ["tok-1"]);
  });
});
