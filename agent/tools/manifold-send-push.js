// manifold-send-push.js — registers the manifold_send_push agent tool.

import { toolErr } from "./shared.js";
import { sendDigestPush } from "../../telemetry/digest-push.js";

export function registerManifoldSendPushTool({ api }) {
  api.registerTool({
    name: "manifold_send_push",
    label: "Manifold: send an operator push notification",
    description:
      "Send a one-way APNs push notification (title + body) to every device the operator has registered. Use this when interviewing the operator during a Vulcan-driven priming session (one question per turn) or for any agent-to-operator alert that doesn't need inline-keyboard buttons. This is a push banner, not a chat thread — the operator must open the app/dashboard to respond; there is no inline reply.",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["text"],
      properties: {
        title: {
          type: "string",
          description: "Notification title. Defaults to a generic label if omitted.",
        },
        text: {
          type: "string",
          description: "Notification body. Plain text.",
        },
      },
    },
    execute: async (_toolCallId, params, _signal) => {
      const { title, text } = params ?? {};
      if (typeof text !== "string" || !text.length) return toolErr("text is required and non-empty");
      const sent = await sendDigestPush({ title: title || "Manifold", body: text, logger: api.logger });
      if (!sent.ok) return toolErr(`push failed: ${sent.reason ?? "unknown"}`);
      return { content: [{ type: "text", text: `Pushed to ${sent.sent}/${sent.total} device(s).` }] };
    },
  });
}
