import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { ContextLifecycleCoordinatorV1, type ManagedCompactionAdapter } from "./coordinator.js";
import { publishContextLifecycleV1 } from "./registry.js";

function textToolResult(text: string) {
  return { content: [{ type: "text" as const, text }], details: { disposition: text } };
}

export default function contextLifecycleExtension(pi: ExtensionAPI): void {
  const coordinator = new ContextLifecycleCoordinatorV1();
  const publication = publishContextLifecycleV1(coordinator.ownerInstanceId, coordinator, {});
  coordinator.attachPublication(publication);
  let generationId: string | undefined;
  let currentContext: ExtensionContext | undefined;

  const adapter: ManagedCompactionAdapter = {
    compact(options) {
      if (!currentContext) throw new Error("No active Pi extension context");
      currentContext.compact(options);
    },
    sendResume(message) {
      pi.sendUserMessage(message, { deliverAs: "steer" });
    },
  };

  pi.on("session_start", (_event, ctx) => {
    currentContext = ctx;
    generationId = coordinator.bindSession(ctx.sessionManager.getSessionId(), adapter);
  });

  pi.on("agent_settled", (_event, ctx) => {
    currentContext = ctx;
    if (generationId) coordinator.onAgentSettled(generationId);
  });

  pi.on("session_compact", (event, ctx) => {
    currentContext = ctx;
    if (generationId) coordinator.onSessionCompact(generationId, event.reason);
  });

  pi.on("message_start", (event, ctx) => {
    currentContext = ctx;
    if (generationId) coordinator.onMessageStart(generationId, event.message);
  });

  pi.on("session_shutdown", () => {
    generationId = undefined;
    currentContext = undefined;
    coordinator.dispose();
  });

  pi.registerTool({
    name: "self_compact",
    label: "Self Compact",
    description: "Request managed context compaction after this agent run settles, then queue one resume turn.",
    promptSnippet: "Use self_compact when the operator asks you to compact before continuing. The lifecycle coordinator waits for this tool result and the settled run before compacting.",
    parameters: Type.Object({
      instructions: Type.Optional(Type.String({ description: "Optional focus for what the compaction summary and resumed turn should preserve and reload." })),
    }),
    execute(toolCallId, params) {
      const disposition = coordinator.requestSelfCompaction(params.instructions ?? "", toolCallId);
      if (disposition.disposition === "rejected") return Promise.resolve(textToolResult(`Self compact rejected (${disposition.code}); no compaction was started.`));
      if (disposition.disposition === "joined") return Promise.resolve(textToolResult(`Self compact request joined managed operation ${disposition.operationId}; compaction will start after this run settles.`));
      return Promise.resolve(textToolResult(`Self compact accepted as managed operation ${disposition.operationId}; compaction will start after this tool result and agent run settle.`));
    },
  });
}
