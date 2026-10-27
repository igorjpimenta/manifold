// Unit tests for the pull-claim ticket core (PULL-1):
// claimNext CAS, lease + sweep, renew, report — all against a real
// sqlite-backed store on an isolated tmp file, never the shared singleton.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import {
  createNodeExecutionsStore,
  PULL_CLAIM_LEASE_MS,
} from "../../../state/node-executions.js";

function tmpDb() {
  return path.join(os.tmpdir(), `pull-claim-${randomUUID()}.sqlite`);
}

const TRIPLE = { harness: "harness-1", agent: "agent-1", session: "session-1" };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function mintReady(store, nodeId = "WORK") {
  const flowId = `flow-${randomUUID()}`;
  const { invocationId } = await store.enter({ flowId, nodeId, kind: "fresh" });
  const parked = await store.markPullReady(invocationId);
  assert.equal(parked.ok, true);
  return { flowId, nodeId, ticket: invocationId };
}

describe("pull-claim — claimant triple is required", () => {
  test("claimNext/renewClaim/reportClaim/markReported throw on a missing field", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    await assert.rejects(() => store.claimNext({}), /harness/);
    await assert.rejects(() => store.claimNext({ harness: "h", agent: "a" }), /session/);
    await assert.rejects(() => store.renewClaim({ ticket: "t", harness: "h", agent: "a" }), /session/);
    await assert.rejects(() => store.reportClaim({ ticket: "t", harness: "h", session: "s", result: {} }), /agent/);
    await assert.rejects(() => store.markReported({ ticket: "t", agent: "a", session: "s" }), /harness/);
    store.close();
  });

  test("claimNext rejects a non-positive leaseMs", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    await assert.rejects(() => store.claimNext({ ...TRIPLE, leaseMs: 0 }), /leaseMs/);
    store.close();
  });
});

describe("pull-claim — claimNext CAS", () => {
  test("assigns the oldest ready ticket first, in mint order", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const first = await mintReady(store);
    const second = await mintReady(store);
    const c1 = await store.claimNext({ ...TRIPLE });
    const c2 = await store.claimNext({ ...TRIPLE, session: "session-2" });
    assert.equal(c1.ticket, first.ticket);
    assert.equal(c2.ticket, second.ticket);
    assert.equal(c1.flowId, first.flowId);
    assert.equal(c1.nodeId, "WORK");
    assert.ok(c1.leaseExpiresAt > Date.now());
    store.close();
  });

  test("parallel claims on one ticket produce exactly one winner, never a double-assign", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) => store.claimNext({ harness: "h", agent: "a", session: `s-${i}` })),
    );
    const winners = results.filter(Boolean);
    assert.equal(winners.length, 1);
    assert.equal(winners[0].ticket, ticket);
    const claim = await store.getClaim(ticket);
    assert.equal(claim.claimState, "claimed");
    assert.equal(claim.session, winners[0].session ?? claim.session);
    store.close();
  });

  test("parallel claims across tickets assign distinct tickets", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const minted = [await mintReady(store), await mintReady(store), await mintReady(store)];
    const results = await Promise.all(
      minted.map((_, i) => store.claimNext({ harness: "h", agent: "a", session: `s-${i}` })),
    );
    assert.equal(new Set(results.map((r) => r.ticket)).size, 3);
    store.close();
  });

  test("returns null when no ready ticket exists", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    assert.equal(await store.claimNext({ ...TRIPLE }), null);
    store.close();
  });
});

describe("pull-claim — lease + sweep", () => {
  test("sweepExpiredClaims requeues an expired claim to ready and clears the binding", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE, leaseMs: 40 });
    await sleep(90);
    const swept = await store.sweepExpiredClaims();
    assert.equal(swept.requeued, 1);
    assert.deepEqual(swept.tickets, [ticket]);
    const claim = await store.getClaim(ticket);
    assert.equal(claim.claimState, "ready");
    assert.equal(claim.harness, null);
    assert.equal(claim.leaseExpiresAt, null);
    const reclaimed = await store.claimNext({ ...TRIPLE });
    assert.equal(reclaimed.ticket, ticket);
    store.close();
  });

  test("sweep leaves unexpired claims alone and is empty when nothing lapsed", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE, leaseMs: PULL_CLAIM_LEASE_MS });
    assert.deepEqual(await store.sweepExpiredClaims(), { requeued: 0, tickets: [] });
    assert.equal((await store.getClaim(ticket)).claimState, "claimed");
    store.close();
  });
});

describe("pull-claim — renew", () => {
  test("renewClaim extends the lease on binding match", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    const claimed = await store.claimNext({ ...TRIPLE, leaseMs: 60000 });
    await sleep(10);
    const renewed = await store.renewClaim({ ticket, ...TRIPLE, leaseMs: 60000 });
    assert.equal(renewed.ok, true);
    assert.ok(renewed.leaseExpiresAt > claimed.leaseExpiresAt);
    store.close();
  });

  test("renewClaim refuses mismatched binding, unknown tickets, and unclaimed states", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE });
    await assert.rejects(() => store.renewClaim({ ticket, ...TRIPLE, agent: "impostor" }), /mismatch/);
    await assert.rejects(() => store.renewClaim({ ticket: "nope", ...TRIPLE }), /no such ticket/);
    const { ticket: ready } = await mintReady(store);
    await assert.rejects(() => store.renewClaim({ ticket: ready, ...TRIPLE }), /unclaimed/);
    store.close();
  });
});

describe("pull-claim — report", () => {
  test("reportClaim stores the result and flips claimed to reported", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { flowId, ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE });
    const reported = await store.reportClaim({ ticket, ...TRIPLE, result: { WORK: { done: true } } });
    assert.equal(reported.ok, true);
    assert.equal(reported.flowId, flowId);
    assert.equal((await store.getClaim(ticket)).claimState, "reported");
    const recorded = await store.getResultForNode(flowId, "WORK");
    assert.deepEqual(JSON.parse(recorded.resultJson), { WORK: { done: true } });
    store.close();
  });

  test("reportClaim refuses impostor bindings and unclaimed tickets", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE });
    await assert.rejects(
      () => store.reportClaim({ ticket, ...TRIPLE, session: "impostor", result: {} }),
      /mismatch/,
    );
    const { ticket: ready } = await mintReady(store);
    await assert.rejects(() => store.reportClaim({ ticket: ready, ...TRIPLE, result: {} }), /unclaimed/);
    store.close();
  });

  test("a same-binding repeat report is idempotent, a cross-binding one is refused", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE });
    await store.reportClaim({ ticket, ...TRIPLE, result: { WORK: { done: true } } });
    const again = await store.reportClaim({ ticket, ...TRIPLE, result: { WORK: { done: true } } });
    assert.equal(again.alreadyReported, true);
    await assert.rejects(
      () => store.reportClaim({ ticket, ...TRIPLE, agent: "impostor", result: {} }),
      /already reported/,
    );
    store.close();
  });
});

describe("pull-claim — park and settle bookkeeping", () => {
  test("markPullReady is idempotent; unknown and missing rows fail loud", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    assert.equal((await store.markPullReady(ticket)).ok, true);
    assert.deepEqual(await store.markPullReady("nope"), { ok: false, reason: "no-such-invocation" });
    assert.deepEqual(await store.markPullReady(null), { ok: false, reason: "no-invocation-id" });
    store.close();
  });

  test("markApplied flips reported to applied exactly once", async () => {
    const store = await createNodeExecutionsStore(tmpDb());
    const { ticket } = await mintReady(store);
    await store.claimNext({ ...TRIPLE });
    assert.deepEqual(await store.markApplied(ticket), { ok: false, reason: "unexpected-state-claimed" });
    await store.reportClaim({ ticket, ...TRIPLE, result: { WORK: true } });
    assert.deepEqual(await store.markApplied(ticket), { ok: true });
    assert.deepEqual(await store.markApplied(ticket), { ok: true, alreadyApplied: true });
    store.close();
  });
});
