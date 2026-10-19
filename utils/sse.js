import http from "node:http";

// Factory — pass a getter so the broadcast ref captured from the gateway
// method ping handler is available without a module-level mutable variable.
export function createEmitSseEvent(getGatewayBroadcast) {
  return async function emitSseEvent(event) {
    const broadcast = getGatewayBroadcast();
    if (broadcast && typeof event?.type === "string" && event.type.startsWith("discuss.")) {
      try { broadcast(`plugin.${event.type}`, event); } catch {}
    }
    const token = process.env.OPENCLAW_GATEWAY_TOKEN;
    if (!token) return { ok: false, error: "no_gateway_token" };
    const body = JSON.stringify(event);
    return new Promise((resolve) => {
      const req = http.request({
        hostname: "127.0.0.1",
        port: Number(process.env.OPENCLAW_GATEWAY_PORT) || 18789,
        method: "POST",
        path: "/pipeclaw/ios/push",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
          "Content-Length": Buffer.byteLength(body),
        },
      }, (res) => {
        res.resume();
        res.on("end", () => resolve({
          ok: res.statusCode >= 200 && res.statusCode < 300,
          status: res.statusCode ?? 0,
        }));
      });
      req.on("error", (err) => resolve({ ok: false, error: err?.message ?? String(err) }));
      req.setTimeout(10_000, () => req.destroy(new Error("emit timeout")));
      req.write(body);
      req.end();
    });
  };
}
