// Parity guard between the server KINDS table and the contract both sides
// implement. This is the server half; the client half runs in oc-pipeclaw-ios
// against the same file. A kind present in one implementation and missing in
// the other delivers a banner whose tap is dropped at NotificationsBridge, with
// no signal anywhere.

import { describe, it } from "node:test";
import assert from "node:assert/strict";

import { NotificationKinds, buildPayload } from "../../../ios/notifications.js";
// The contract itself, not a copy of it. JSON rather than the package's TS
// module because this suite runs on plain node.
import contract from "@igorjpimenta/oc-contracts/pipeclaw-ios/notification-kinds.json" with { type: "json" };

/** Raw values of the client's enum, as the contract defines them. */
function clientKindRawValues() {
  return contract.kinds;
}

describe("server/client notification kind parity", () => {
  const serverKinds = Object.values(NotificationKinds);

  // Emitted outside this table: scripts/deploy-on-green.sh posts the deploy
  // category straight to the gateway's /apns/push route rather than going
  // through pushKind, so it has no KINDS entry by design.
  const OUT_OF_BAND = new Set(["openclaw.deploy"]);

  it("has no server kind the iOS client cannot route", () => {
    const client = new Set(clientKindRawValues());
    const unrouteable = serverKinds.filter((k) => !client.has(k));
    assert.deepEqual(
      unrouteable, [],
      "server emits kinds with no NotificationKind case — their banners will "
      + "deliver but every tap will be dropped. Add a case in NotificationKinds.swift.",
    );
  });

  it("has no client kind nothing ever sends", () => {
    const server = new Set([...serverKinds, ...OUT_OF_BAND]);
    const orphan = clientKindRawValues().filter((k) => !server.has(k));
    assert.deepEqual(
      orphan, [],
      "iOS registers categories for kinds nothing emits — dead surface. "
      + "Remove the case, or add it to the server KINDS table.",
    );
  });
});

describe("buildPayload", () => {
  it("throws on an unregistered kind rather than sending a bare banner", () => {
    assert.throws(() => buildPayload("openclaw.nonexistent", {}), /unknown kind/);
  });

  it("carries `kind` and `aps.category` in lockstep for every kind", () => {
    for (const kind of Object.values(NotificationKinds)) {
      const payload = buildPayload(kind, {
        title: "T", body: "B", flowId: "f", itemId: "i", failureId: "x",
      });
      assert.equal(payload.aps.category, kind, `${kind}: aps.category`);
      assert.equal(payload.kind, kind, `${kind}: top-level kind mirror`);
    }
  });

  it("groups by kind so a flow's notifications stack rather than fan out", () => {
    const a = buildPayload(NotificationKinds.APPROVAL, { flowId: "abc", title: "T", body: "B" });
    const b = buildPayload(NotificationKinds.APPROVAL, { flowId: "abc", title: "T2", body: "B2" });
    const c = buildPayload(NotificationKinds.APPROVAL, { flowId: "xyz", title: "T", body: "B" });
    assert.equal(a.aps["thread-id"], b.aps["thread-id"]);
    assert.notEqual(a.aps["thread-id"], c.aps["thread-id"]);
  });

  it("titles a failure by its classified error_type, falling back when unknown", () => {
    const known = buildPayload(NotificationKinds.FAILURE, { errorType: "infra_unavailable" });
    assert.match(known.aps.alert.title, /Infrastructure unavailable/);
    const unknown = buildPayload(NotificationKinds.FAILURE, { errorType: "something_new" });
    assert.match(unknown.aps.alert.title, /Failure/);
  });
});
