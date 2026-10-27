// /manifold/telemetry/sessions/parts — session transcript from lcm.db.
// Route shape stays core; the lcm.db read lives in the harness-side
// adapter, injected as `sources`. Without it the file is unreachable,
// so the route answers the same 503 as a missing lcm.db.
export function registerSessionRoutes({ api, logger, json, bad, sources = null, lcmDbPath } = {}) {
  api.registerHttpRoute({
    method: "GET",
    path: "/manifold/telemetry/sessions/parts",
    auth: "plugin",
    handler: async (req, res) => {
      const u = new URL(req.url, "http://localhost");
      const sessionKey = u.searchParams.get("session_key");
      const limit = Math.min(parseInt(u.searchParams.get("limit") ?? "80", 10), 2000);
      if (!sessionKey) return bad(res, "session_key required");
      if (!sources) {
        logger?.warn?.(`lineage[sessions/parts] no gateway sources injected`);
        return json(res, { error: "lcm.db not available" }, 503);
      }
      // Cursor params:
      //   before_seq — newest `limit` rows with seq < before_seq (backward)
      //   after_seq  — oldest `limit` rows with seq > after_seq  (forward)
      // Default (no cursor): newest `limit` rows in the session.
      const beforeSeq = u.searchParams.has("before_seq")
        ? parseInt(u.searchParams.get("before_seq"), 10)
        : null;
      const afterSeq = u.searchParams.has("after_seq")
        ? parseInt(u.searchParams.get("after_seq"), 10)
        : null;
      try {
        const { parts, hasMore, hasMoreNewer, firstSeq, lastSeq } =
          await sources.querySessionParts({ lcmDbPath, sessionKey, limit, beforeSeq, afterSeq });
        return json(res, { sessionKey, parts, hasMore, hasMoreNewer, firstSeq, lastSeq });
      } catch (err) {
        if (err?.code === "LCM_DB_MISSING") {
          logger?.warn?.(`lineage[sessions/parts] lcm.db open failed: ${err?.message}`);
          return json(res, { error: "lcm.db not available" }, 503);
        }
        logger?.warn?.(`lineage[sessions/parts]: ${err?.message ?? err}`);
        return json(res, { error: String(err?.message ?? err) }, 500);
      }
    },
  });
}
