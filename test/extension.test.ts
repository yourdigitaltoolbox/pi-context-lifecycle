import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import contextLifecycleExtension from "../src/extension.js";
import { CONTEXT_LIFECYCLE_REGISTRY_SYMBOL } from "../src/types.js";

interface RegisteredToolLike {
  execute(toolCallId: string, params: { instructions?: string }): Promise<{ content: Array<{ type: string; text: string }> }>;
}
type Handler = (event: Record<string, unknown>, context: ExtensionContext) => unknown;

function harness() {
  const handlers = new Map<string, Handler>();
  let tool: RegisteredToolLike | undefined;
  const sendUserMessage = vi.fn();
  const api = {
    on(event: string, handler: Handler) { handlers.set(event, handler); },
    registerTool(value: RegisteredToolLike) { tool = value; },
    sendUserMessage,
  } as unknown as ExtensionAPI;
  const compact = vi.fn();
  const context = {
    sessionManager: { getSessionId: () => "session" },
    compact,
  } as unknown as ExtensionContext;
  const emit = (name: string, event: Record<string, unknown> = {}) => handlers.get(name)?.(event, context);
  return { api, compact, context, emit, sendUserMessage, getTool: () => tool };
}

describe("Pi extension tracer", () => {
  beforeEach(() => { Reflect.deleteProperty(globalThis, CONTEXT_LIFECYCLE_REGISTRY_SYMBOL); });
  afterEach(() => { Reflect.deleteProperty(globalThis, CONTEXT_LIFECYCLE_REGISTRY_SYMBOL); });

  it("returns the tool result before compacting from agent_settled", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    const tool = test.getTool();
    expect(tool).toBeDefined();
    const result = await tool?.execute("tool-call", { instructions: "reload durable state" });
    expect(result?.content[0]?.text).toContain("accepted");
    expect(test.compact).not.toHaveBeenCalled();

    test.emit("agent_settled", { type: "agent_settled" });
    expect(test.compact).toHaveBeenCalledTimes(1);
    test.emit("agent_settled", { type: "agent_settled" });
    expect(test.compact).toHaveBeenCalledTimes(1);

    test.emit("session_compact", { type: "session_compact", reason: "manual", fromExtension: false });
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
    const resume = test.sendUserMessage.mock.calls[0]?.[0] as string;
    test.emit("input", { type: "input", source: "extension", text: resume });
    test.emit("agent_start", { type: "agent_start" });
    test.emit("agent_settled", { type: "agent_settled" });
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
  });
});
