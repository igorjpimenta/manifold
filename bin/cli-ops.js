// cli-ops.js — resume-admitted, release-leaked-admission, dashboard, paused subcommands.
// Deps keys used: store, executor, logger, runStore, nodeExecutionsStore, admissionQueueStore, api, dashboardUrl.
import { spawn } from "node:child_process";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";
import { listPaused } from "../gates/paused-flows.js";
import { flipToRunningAndResume } from "../engine/child-resume-trigger.js";
import { wakePromotedRelease } from "../engine/admission-promotion-resume.js";
import { waitForFlowSettlement } from "../engine/wait-for-flow-settlement.js";
import { withExit, browserOpenCommand } from "./cli.js";

export function registerOpsCommands(root, deps) {
  const { store, executor, logger, runStore, nodeExecutionsStore, admissionQueueStore, api, dashboardUrl } = deps;
  // manifold resume-admitted <flowId> — recovery for an "orphaned
  // promotion": a flow stuck status='queued' whose flow-start/child-dispatch
  // admission row is already 'admitted' (it holds a live pool slot) because
  // some past release() promoted it and the promotion's resume was dropped
  // (see engine/admission-promotion-resume.js's header). Deliberately does
  // NOT call admissionQueueStore.admit() — the flow already holds a valid
  // slot, and every "resume a queued flow" path that admits unconditionally
  // (admitAndSetRunning, used by retry/approve/gate-resolve) would create a
  // SECOND admission row here, permanently shrinking the shared pool by one
  // more slot exactly like the bug this command recovers from. Refuses to
  // run unless it can confirm the flow genuinely already holds that slot —
  // this is a recovery tool for the specific stuck state, not a way to cut
  // the queue.
  root
    .command("resume-admitted <flowId>")
    .description("Force-resume a flow stuck 'queued' that already holds a live flow-start/child-dispatch admission slot (an orphaned promotion). Refuses if the flow doesn't already hold that slot.")
    .action(withExit(async (flowId) => {
      const flow = await store.get(flowId);
      if (!flow) {
        console.error(`manifold: flow not found: ${flowId}`);
        process.exit(1);
      }
      if (flow.status !== "queued") {
        console.error(`manifold: flow is not queued (status=${flow.status}); nothing to force-resume`);
        process.exit(1);
      }
      const kind = flow.waitJson?.kind;
      if (kind !== "flow-start" && kind !== "child-dispatch") {
        console.error(`manifold: wait kind "${kind ?? "?"}" isn't resumable this way — only flow-start/child-dispatch hold a durable admission with no in-process waiter of their own`);
        process.exit(1);
      }
      if (!admissionQueueStore) {
        console.error("manifold: no admissionQueueStore wired in");
        process.exit(1);
      }
      const admitted = await admissionQueueStore.listByStatus("admitted");
      const held = admitted.find((r) => r.flowId === flowId && r.kind === kind);
      if (!held) {
        console.error(`manifold: flow does not hold a live '${kind}' admission — it's genuinely still queued, not orphaned. Wait for the normal release/promotion cycle instead of forcing this.`);
        process.exit(1);
      }
      const result = await flipToRunningAndResume({ parentFlowId: flowId, runStore, resume: executor.resume, logger, api });
      if (!result.resumed) {
        console.error(`manifold: force-resume did not apply: ${result.reason ?? "unknown"}`);
        process.exit(1);
      }
      await waitForFlowSettlement(flowId, { store, runStore, nodeExecutionsStore, logger });
      // By now the flow is guaranteed terminal or gate-blocked (see
      // waitForFlowSettlement's own contract) — either way its admission
      // release went out via state-store.js's setWaiting/terminal hooks,
      // fire-and-forget. This CLI invocation is short-lived: withExit's
      // process.exit() right after this action returns gives that
      // background release no guarantee of finishing its DB round-trip
      // first, so release explicitly here before exiting. releaseByFlow is
      // a safe no-op for whichever kind doesn't apply, and for the one the
      // internal call already handled.
      if (admissionQueueStore) {
        const results = await Promise.all([
          admissionQueueStore.releaseByFlow({ flowId, kind: "flow-start" }).catch(() => ({ released: false, promoted: null })),
          admissionQueueStore.releaseByFlow({ flowId, kind: "child-dispatch" }).catch(() => ({ released: false, promoted: null })),
        ]);
        await wakePromotedRelease(results, {
          runStore, resume: executor.resume, logger, api, context: "manifold resume-admitted: cleanup-release",
        });
      }
      const final = await store.get(flowId);
      console.log(`manifold: force-resumed ${flowId.slice(0, 8)} → ${final?.status ?? "?"}`);
    }));

  // manifold release-leaked-admission <flowId> — recovery for the OTHER
  // admission-pool failure mode (see engine/admission-promotion-resume.js's
  // header for the orphaned-promotion one resume-admitted above handles): a
  // flow_admission_queue row is only ever supposed to exist while its owning
  // flow is genuinely non-terminal. If a terminal transition's own release
  // (state-store.js's triggerAdmissionRelease) doesn't fire or doesn't land
  // — a bug in the releasing code path, or a process exiting before that
  // fire-and-forget write commits — the row is a leak: a dead flow
  // permanently holding a live pool slot. boot/flow-revival.js sweeps for
  // exactly this at every boot; this is the same check and release, scoped
  // to one caller-specified flowId instead of a full-table scan, for when
  // restarting the gateway isn't the right lever.
  root
    .command("release-leaked-admission <flowId>")
    .description("Release a terminal flow's leaked flow_admission_queue row (status=admitted/queued for a flow that already finished) and resume whatever gets promoted into the freed slot.")
    .action(withExit(async (flowId) => {
      const flow = await store.get(flowId);
      if (!flow) {
        console.error(`manifold: flow not found: ${flowId}`);
        process.exit(1);
      }
      if (!TERMINAL_STATUS_SET.has(flow.status)) {
        console.error(`manifold: flow is not terminal (status=${flow.status}) — this command only cleans up a leak on an already-finished flow; use resume-admitted or cancel for a live one`);
        process.exit(1);
      }
      if (!admissionQueueStore) {
        console.error("manifold: no admissionQueueStore wired in");
        process.exit(1);
      }
      let releasedCount = 0;
      for (const kind of ["flow-start", "child-dispatch"]) {
        const result = await admissionQueueStore.releaseByFlow({ flowId, kind });
        if (!result.released) continue;
        releasedCount++;
        console.log(`manifold: released leaked ${kind} admission for ${flowId.slice(0, 8)}`);
        if (result.promoted) {
          const r = await wakePromotedRelease(result, {
            runStore, resume: executor.resume, logger, api, context: `manifold release-leaked-admission [${flowId.slice(0, 8)}]`,
          });
          console.log(`  promoted ${result.promoted.flowId.slice(0, 8)} (${result.promoted.kind}) into the freed slot — resumed=${r.resumed}${r.resumed ? "" : ` reason=${r.reason}`}`);
        }
      }
      if (releasedCount === 0) {
        console.log(`manifold: flow ${flowId.slice(0, 8)} held no live admission — nothing to release`);
      }
    }));

  // manifold dashboard — print URL and open the browser to the read-only UI.
  // The dashboard service runs in the gateway process; the CLI just opens a
  // window pointing at it. Doesn't check liveness — the user will see a
  // connection error in their browser if the gateway isn't running.
  root
    .command("dashboard")
    .description("Open the read-only manifold dashboard in your browser.")
    .option("--no-open", "Just print the URL; don't launch a browser.")
    .option("--url <url>", "Override the URL to open (defaults to the configured dashboard host/port).")
    .action(withExit((options) => {
      const url = options.url || dashboardUrl || "http://127.0.0.1:7430";
      console.log(`manifold dashboard: ${url}`);
      if (options.open === false) return;
      const { cmd, args } = browserOpenCommand(url);
      if (!cmd) {
        console.log(`manifold dashboard: no platform-appropriate opener; open the URL manually.`);
        return;
      }
      try {
        const child = spawn(cmd, args, { stdio: "ignore", detached: true });
        child.on("error", (err) => console.log(`manifold dashboard: opener "${cmd}" failed (${err.message}); open the URL manually.`));
        child.unref();
      } catch (err) {
        console.log(`manifold dashboard: failed to launch browser (${err.message}); open the URL manually.`);
      }
    }));

  // manifold paused — list paused flows
  root
    .command("paused")
    .description("List flows currently paused (operator side-channel active).")
    .action(withExit(async () => {
      const paused = [...listPaused()];
      if (paused.length === 0) {
        console.log("manifold: no flows are paused.");
        return;
      }
      for (const flowId of paused) {
        const f = await store.get(flowId);
        const pipelineId = f?.stateJson?.pipelineId ?? "(unknown)";
        console.log(`  ${flowId.slice(0, 8)}  ${pipelineId}  status=${f?.status ?? "(missing)"}`);
      }
    }));
}
