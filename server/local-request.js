// Refuse a request whose Host/Origin does not name this machine, checked
// before any bearer comparison. DNS rebinding: an attacker page resolves its
// own hostname to 127.0.0.1 and posts to this loopback listener — the browser
// sends the attacker's Host/Origin, so refusing non-local Host/Origin up
// front means the rebinding attempt never reaches the route table. IP literals
// and an explicit LAN bind stay allowed (`MANIFOLD_API_HOST=0.0.0.0`);
// `localhost`, `*.localhost` and `127.0.0.1`/`::1` always pass.
import net from "node:net";

function headerHostname(raw) {
  if (typeof raw !== "string" || !/^[A-Za-z0-9.\-:[\]]+$/.test(raw)) return null;
  try {
    return new URL(`http://${raw}`).hostname.replace(/^\[|\]$/g, "").toLowerCase().replace(/\.$/, "");
  } catch {
    return null;
  }
}

function localHostname(hostname, boundHost, { anyIp = true } = {}) {
  if (!hostname) return false;
  if (hostname === "localhost" || hostname.endsWith(".localhost")) return true;
  if (hostname === "127.0.0.1" || hostname === "::1") return true;
  if (anyIp && net.isIP(hostname)) return true;
  const bound = String(boundHost ?? "").toLowerCase().replace(/^\[|\]$/g, "").replace(/\.$/, "");
  return bound !== "" && bound !== "0.0.0.0" && bound !== "::" && hostname === bound;
}

/**
 * True when Host names this machine and, if a browser sent one, Origin does
 * too. A missing Host is refused: HTTP/1.1 clients always send one.
 */
export function isLocalRequest(req, { boundHost = null } = {}) {
  const host = headerHostname(req?.headers?.host);
  if (!localHostname(host, boundHost)) return false;
  const origin = req?.headers?.origin;
  if (origin === undefined || origin === "") return true;
  if (typeof origin !== "string" || origin === "null") return false;
  try {
    const url = new URL(origin);
    if (url.protocol !== "http:" && url.protocol !== "https:") return false;
    return localHostname(
      url.hostname.replace(/^\[|\]$/g, "").toLowerCase().replace(/\.$/, ""),
      boundHost,
      { anyIp: false },
    );
  } catch {
    return false;
  }
}
