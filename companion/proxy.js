// companion/proxy.js — forward one gateway request to the sidecar and
// stream the response back. Bodies and SSE streams pass through untouched;
// hop headers and the caller's bearer are stripped (the sidecar sees only
// the service token). Returns false for paths this harness must not serve
// (sidecar internals, sibling plugin namespaces) so gateway matching can
// continue to their owners.
import http from "node:http";
import https from "node:https";

const HOP_HEADERS = new Set([
  "host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade", "content-length",
]);

// Sibling namespaces owned by other plugins — never proxied.
const PASSTHROUGH_PREFIXES = ["/manifold/ios/"];
// Sidecar internals — never exposed through the harness.
const DENY_PREFIXES = ["/manifold/internal/"];

export function proxyDecision(pathname) {
  if (DENY_PREFIXES.some((p) => pathname === p || pathname.startsWith(p))) return "deny";
  if (PASSTHROUGH_PREFIXES.some((p) => pathname.startsWith(p))) return "passthrough";
  return "proxy";
}

export function forwardToSidecar({ sidecarUrl, serviceToken, logger = null }) {
  const base = new URL(sidecarUrl);
  const transport = base.protocol === "https:" ? https : http;
  return (req, res) => new Promise((resolve) => {
    const outHeaders = {};
    for (const [k, v] of Object.entries(req.headers ?? {})) {
      if (!HOP_HEADERS.has(k.toLowerCase())) outHeaders[k] = v;
    }
    outHeaders.authorization = `Bearer ${serviceToken}`;
    delete outHeaders["content-length"];
    const upstream = transport.request(
      {
        hostname: base.hostname,
        port: base.port,
        path: req.url,
        method: req.method,
        headers: outHeaders,
      },
      (upstreamRes) => {
        res.statusCode = upstreamRes.statusCode ?? 502;
        for (const [k, v] of Object.entries(upstreamRes.headers ?? {})) {
          if (HOP_HEADERS.has(k.toLowerCase())) continue;
          try {
            res.setHeader(k, v);
          } catch {}
        }
        upstreamRes.on("error", () => {
          try {
            res.destroy();
          } catch {}
          resolve(true);
        });
        upstreamRes.pipe(res);
        upstreamRes.on("end", () => resolve(true));
      },
    );
    upstream.on("error", (err) => {
      logger?.warn?.(`companion proxy: sidecar unreachable: ${err?.message ?? err}`);
      try {
        res.statusCode = 502;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: "sidecar unavailable" }));
      } catch {}
      resolve(true);
    });
    req.pipe(upstream);
    req.on("error", () => {
      try {
        upstream.destroy();
      } catch {}
    });
  });
}
