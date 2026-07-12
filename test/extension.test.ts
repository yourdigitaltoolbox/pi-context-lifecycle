import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ManagedCompactionAdapter } from "../src/coordinator.js";
import contextLifecycleExtension from "../src/extension.js";
import { getContextLifecycleSnapshotV1 } from "../src/registry.js";
import { CONTEXT_LIFECYCLE_REGISTRY_SYMBOL } from "../src/types.js";

interface RegisteredToolLike {
  name: string;
  execute(toolCallId: string, params: { instructions?: string; handoffPath?: string; nextStep?: string }): Promise<{ content: Array<{ type: string; text: string }> }>;
}
interface RegisteredCommandLike {
  handler(args: string, context: ExtensionCommandContext): Promise<void>;
}
type Handler = (event: Record<string, unknown>, context: ExtensionContext) => unknown;

function harness() {
  const handlers = new Map<string, Handler>();
  const tools = new Map<string, RegisteredToolLike>();
  const commands = new Map<string, RegisteredCommandLike>();
  const sendUserMessage = vi.fn();
  const api = {
    on(event: string, handler: Handler) { handlers.set(event, handler); },
    registerTool(value: RegisteredToolLike) { tools.set(value.name, value); },
    registerCommand(name: string, value: RegisteredCommandLike) { commands.set(name, value); },
    sendUserMessage,
  } as unknown as ExtensionAPI;
  const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
  const context = {
    sessionManager: { getSessionId: () => "session" },
    compact,
  } as unknown as ExtensionContext;
  const emit = (name: string, event: Record<string, unknown> = {}) => handlers.get(name)?.(event, context);
  return { api, compact, context, emit, sendUserMessage, getTool: (name = "self_compact") => tools.get(name), getCommand: (name: string) => commands.get(name) };
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
    expect(test.sendUserMessage).not.toHaveBeenCalled();
    test.compact.mock.calls[0]?.[0].onComplete();
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
    const resume = test.sendUserMessage.mock.calls[0]?.[0] as string;
    expect(resume).toMatch(/pi-context-lifecycle:v1 resume operationId=.+ generationId=.+/);

    // Handled input/unrelated starts have no matching message_start and cannot release.
    test.emit("input", { type: "input", source: "extension", text: resume });
    test.emit("agent_start", { type: "agent_start" });
    test.emit("agent_settled", { type: "agent_settled" });
    expect(getContextLifecycleSnapshotV1().phase).toBe("resuming");
    test.emit("message_start", { type: "message_start", message: { role: "user", content: `${resume} transformed` } });
    test.emit("agent_settled", { type: "agent_settled" });
    expect(getContextLifecycleSnapshotV1().phase).toBe("resuming");
    test.emit("message_start", { type: "message_start", message: { role: "user", content: "unrelated" } });
    test.emit("agent_settled", { type: "agent_settled" });
    expect(getContextLifecycleSnapshotV1().phase).toBe("resuming");

    test.emit("message_start", { type: "message_start", message: { role: "user", content: resume } });
    test.emit("agent_settled", { type: "agent_settled" });
    expect(getContextLifecycleSnapshotV1().phase).toBe("idle");
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("routes both self-compact command aliases through one pending managed operation", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });

    const commandContext = test.context as unknown as ExtensionCommandContext;
    await test.getCommand("self_compact")?.handler("first focus", commandContext);
    await test.getCommand("self-compact")?.handler("second focus", commandContext);
    expect(getContextLifecycleSnapshotV1().phase).toBe("compacting");
    expect(test.compact).toHaveBeenCalledTimes(1);

    test.emit("agent_settled", { type: "agent_settled" });
    expect(test.compact).toHaveBeenCalledTimes(1);
  });

  it("observes automatic threshold preflight and releases after durable success", () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });

    test.emit("session_before_compact", { type: "session_before_compact", reason: "threshold", willRetry: false });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "observed-preflight", reason: "threshold" });

    test.emit("session_compact", { type: "session_compact", reason: "threshold", fromExtension: false, willRetry: false });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.sendUserMessage).not.toHaveBeenCalled();
  });

  it("keeps handoff tool guidance honest and replaces the session only from command context", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);

    const tool = test.getTool("handoff_new_session");
    expect(tool).toBeDefined();
    const result = await tool?.execute("handoff-tool", { handoffPath: "HANDOFF.md", nextStep: "Run the next test." });
    expect(test.sendUserMessage).not.toHaveBeenCalled();
    expect(result?.content[0]?.text).toContain("/handoff-new-session");
    expect(result?.content[0]?.text).toContain("HANDOFF.md");

    const kickoff = vi.fn<(message: string) => Promise<void>>(() => Promise.resolve());
    const newSession = vi.fn(async (options?: { parentSession?: string; withSession?: (ctx: { sendUserMessage: typeof kickoff }) => Promise<void> }) => {
      await options?.withSession?.({ sendUserMessage: kickoff });
      return { cancelled: false };
    });
    const commandContext = {
      sessionManager: { getSessionFile: () => "/tmp/old-session.jsonl" },
      newSession,
      hasUI: false,
    } as unknown as ExtensionCommandContext;
    const command = test.getCommand("handoff-new-session");
    expect(command).toBeDefined();
    await command?.handler(JSON.stringify({ handoffPath: "HANDOFF.md", nextStep: "Run the next test." }), commandContext);

    expect(newSession).toHaveBeenCalledTimes(1);
    expect(newSession.mock.calls[0]?.[0]).toMatchObject({ parentSession: "/tmp/old-session.jsonl" });
    expect(kickoff).toHaveBeenCalledTimes(1);
    expect(kickoff.mock.calls[0]?.[0]).toContain("HANDOFF.md");
    expect(kickoff.mock.calls[0]?.[0]).toContain("Run the next test.");
  });

  it("applies an exact context-lifecycle repair from out-of-band command context", async () => {
    vi.useFakeTimers();
    try {
      const test = harness();
      contextLifecycleExtension(test.api);
      test.emit("session_start", { type: "session_start", reason: "startup" });
      await test.getTool()?.execute("tool-call", {});
      test.emit("agent_settled", { type: "agent_settled" });
      test.emit("session_compact", { type: "session_compact", reason: "manual", fromExtension: false });
      test.compact.mock.calls[0]?.[0].onComplete();
      await vi.advanceTimersByTimeAsync(60_000);
      const blocked = getContextLifecycleSnapshotV1();
      expect(blocked.phase).toBe("blocked-unknown");

      const notify = vi.fn();
      const commandContext = { hasUI: true, ui: { notify } } as unknown as ExtensionCommandContext;
      const command = test.getCommand("context-lifecycle");
      expect(command).toBeDefined();
      await command?.handler(`repair ${JSON.stringify({
        action: "abandon-ambiguous-resume",
        operationId: blocked.operationId,
        sessionId: blocked.sessionId,
        generationId: blocked.generationId,
        expectedPhase: "blocked-unknown",
        expectedSequence: blocked.sequence,
        evidenceClass: "current-process-quiescent",
        actor: "operator",
        channel: "command",
      })}`, commandContext);

      await Promise.resolve();
      expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
      expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
      expect(notify).toHaveBeenCalledWith(expect.stringContaining("applied"), "info");
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not attribute a threshold event to the managed compact call", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    await test.getTool()?.execute("tool-call", {});
    test.emit("agent_settled", { type: "agent_settled" });
    test.emit("session_compact", { type: "session_compact", reason: "threshold", fromExtension: false });
    test.compact.mock.calls[0]?.[0].onComplete();
    expect(getContextLifecycleSnapshotV1().phase).toBe("blocked-unknown");
    expect(test.sendUserMessage).not.toHaveBeenCalled();
  });
});
