// /manifold/telemetry/budget/* routes — cap read/write + alert test.
import {
  currentCapUSD, setCapAndPersist,
  currentLockThreshold, setLockThresholdAndPersist,
  testFireAlert, getCapHistory, getAlertHistory,
} from "./budget-alert.js";

export function registerBudgetRoutes({ api, getStore, logger, json, bad, getBroadcast }) {
  // ─── /budget/cap — read / update the operator-set daily cap ───────────────
  api.registerHttpRoute({
    path: "/manifold/telemetry/budget/cap", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method === "GET") {
        return json(res, { capUSD: currentCapUSD() });
      }
      if (req.method === "PUT") {
        try {
          let raw = "";
          for await (const chunk of req) raw += chunk;
          const body = raw ? JSON.parse(raw) : {};
          const capUSD = Number(body.capUSD);
          if (!capUSD || capUSD <= 0 || !Number.isFinite(capUSD)) {
            return json(res, { error: "capUSD must be a positive number" }, 400);
          }
          await setCapAndPersist(capUSD);
          // iOS's Budget screen fetches cap/threshold via two independent
          // requests (home tile + hourly chart) — without this broadcast the
          // chart's cap line goes stale until the next unrelated reload.
          getBroadcast?.()?.("plugin.budget.cap.changed", { capUSD });
          return json(res, { ok: true, capUSD });
        } catch (err) {
          return json(res, { error: String(err?.message ?? err) }, 400);
        }
      }
      res.statusCode = 405; res.end("method not allowed");
    },
  });

  // ─── /budget/gate-threshold — read / update the auto-lock threshold ───────
  api.registerHttpRoute({
    path: "/manifold/telemetry/budget/gate-threshold", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      if (req.method === "GET") {
        return json(res, { lockThreshold: currentLockThreshold() });
      }
      if (req.method === "PUT") {
        try {
          let raw = "";
          for await (const chunk of req) raw += chunk;
          const body = raw ? JSON.parse(raw) : {};
          const lockThreshold = Number(body.lockThreshold);
          if (!lockThreshold || lockThreshold <= 0 || lockThreshold > 1 || !Number.isFinite(lockThreshold)) {
            return json(res, { error: "lockThreshold must be a number between 0 and 1" }, 400);
          }
          await setLockThresholdAndPersist(lockThreshold);
          getBroadcast?.()?.("plugin.budget.cap.changed", { lockThreshold });
          return json(res, { ok: true, lockThreshold });
        } catch (err) {
          return json(res, { error: String(err?.message ?? err) }, 400);
        }
      }
      res.statusCode = 405; res.end("method not allowed");
    },
  });

  // ─── /budget/cap-history — SCD2 cap/threshold versions (oldest first) ──────
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/budget/cap-history", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      try {
        const rows = await getCapHistory();
        const versions = (rows ?? []).map((r) => ({
          capUSD: Number(r.capUSD),
          lockThreshold: Number(r.lockThreshold),
          registeredAt: Number(r.registeredAt),
          retiredAt: r.retiredAt == null ? null : Number(r.retiredAt),
        }));
        return json(res, { versions });
      } catch (err) {
        logger?.warn?.(`lineage[http /budget/cap-history]: ${err?.message ?? err}`);
        return json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /budget/alert-history — threshold-crossing events, newest first ──────
  // Powers the cap-hit chart's tap-to-detail (which threshold, what time) —
  // distinct from the debounce file, which only suppresses same-day re-fires;
  // every threshold fire is durably logged here with its timestamp.
  api.registerHttpRoute({
    method: "GET", path: "/manifold/telemetry/budget/alert-history", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      try {
        const url = new URL(req.url, "http://x");
        const limit = Math.min(Number(url.searchParams.get("limit")) || 60, 200);
        const rows = await getAlertHistory({ limit });
        const events = (rows ?? []).map((r) => ({
          threshold: Number(r.threshold),
          totalUSD: Number(r.totalUSD),
          capUSD: Number(r.capUSD),
          firedAt: Number(r.firedAt),
        }));
        return json(res, { events });
      } catch (err) {
        logger?.warn?.(`lineage[http /budget/alert-history]: ${err?.message ?? err}`);
        return json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });

  // ─── /budget/test-alert — fire a test push without mutating persisted state
  api.registerHttpRoute({
    method: "POST", path: "/manifold/telemetry/budget/test-alert", auth: "plugin", match: "exact",
    handler: async (req, res) => {
      try {
        const store = getStore();
        if (!store) return bad(res, "lineage store not ready");
        const url = new URL(req.url, "http://x");
        const threshold = Number(url.searchParams.get("threshold") || 0.5);
        await testFireAlert({ store, logger, threshold });
        json(res, { ok: true, threshold });
      } catch (err) {
        logger?.warn?.(`lineage[http /budget/test-alert]: ${err?.message ?? err}`);
        json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });
}
