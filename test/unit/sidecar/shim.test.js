// shim.test.js — gateway api replay: registries + longest-match routing.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createSidecarApi } from "../../../sidecar/shim.js";

describe("createSidecarApi", () => {
  it("collects services, routes, cli, tools, methods, hooks", () => {
    const s = createSidecarApi({ logger: null, pluginConfig: { a: 1 } });
    s.api.registerService({ id: "x" });
    s.api.registerHttpRoute({ path: "/a", match: "exact", handler: () => true });
    s.api.registerCli(() => {}, {});
    s.api.registerTool(() => ({}));
    s.api.registerGatewayMethod("m.ping", () => {}, {});
    const unsub = s.api.on("after_tool_call", () => {});
    assert.equal(s.services.length, 1);
    assert.equal(s.routes.length, 1);
    assert.equal(s.cliRegistrations.length, 1);
    assert.equal(s.tools.length, 1);
    assert.ok(s.gatewayMethods.has("m.ping"));
    assert.equal(s.eventHandlers.get("after_tool_call")?.size, 1);
    unsub();
    assert.equal(s.eventHandlers.get("after_tool_call")?.size, 0);
    assert.equal(s.api.pluginConfig.a, 1);
  });

  it("findRoute: exact beats prefix, longest wins, null otherwise", () => {
    const s = createSidecarApi({});
    const exact = { path: "/manifold/failures", match: "exact" };
    const prefix = { path: "/manifold/failures/", match: "prefix" };
    const root = { path: "/manifold/", match: "prefix" };
    s.api.registerHttpRoute(root);
    s.api.registerHttpRoute(prefix);
    s.api.registerHttpRoute(exact);
    assert.equal(s.findRoute("/manifold/failures"), exact);
    assert.equal(s.findRoute("/manifold/failures/abc/retry"), prefix);
    assert.equal(s.findRoute("/manifold/graphql"), root);
    assert.equal(s.findRoute("/other"), null);
  });
});
