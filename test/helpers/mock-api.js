// Shared test helpers for pipeclaw route unit tests.

/**
 * Returns a mock `api` that records every registerHttpRoute / registerGatewayMethod /
 * api.on call so unit tests can assert on registration behaviour without a real gateway.
 */
export function createMockApi() {
  const routes = [];
  const gatewayMethods = [];
  const events = {};

  const api = {
    registerHttpRoute:    (opts) => routes.push(opts),
    registerGatewayMethod:(name, handler, opts) => gatewayMethods.push({ name, handler, opts }),
    on:                   (event, handler) => { events[event] = handler; },
    logger: { info: () => {}, warn: () => {}, debug: () => {}, error: () => {} },
  };

  return { api, routes, gatewayMethods, events };
}

/**
 * Minimal writable response object. Tracks statusCode, headers, and body chunks.
 */
export function createMockRes() {
  const chunks = [];
  const headers = {};
  let _statusCode = 200;
  let _ended = false;

  return {
    get statusCode()  { return _statusCode; },
    set statusCode(v) { _statusCode = v; },
    setHeader: (k, v) => { headers[k] = v; },
    write:     (chunk) => { chunks.push(chunk); },
    end:       (chunk) => { if (chunk != null) chunks.push(chunk); _ended = true; },
    get body()    { return chunks.join(""); },
    get ended()   { return _ended; },
    get headers() { return headers; },
  };
}

/**
 * Minimal readable request object.
 */
export function createMockReq(method = "GET", path = "/", extraHeaders = {}) {
  return {
    method,
    url: path,
    headers: { host: "localhost", ...extraHeaders },
  };
}
