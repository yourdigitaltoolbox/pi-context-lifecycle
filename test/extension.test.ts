import type { ExtensionAPI, ExtensionCommandContext, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ManagedCompactionAdapter } from "../src/coordinator.js";
import contextLifecycleExtension from "../src/extension.js";
import { getContextLifecycleSnapshotV1, requestCompaction } from "../src/registry.js";
import { CONTEXT_LIFECYCLE_REGISTRY_SYMBOL } from "../src/types.js";

interface RegisteredToolLike {
  name: string;
  execute(toolCallId: string, params: { instructions?: string; handoffPath?: string; nextStep?: string }, signal: AbortSignal | undefined, onUpdate: undefined, context: ExtensionContext): Promise<{ content: Array<{ type: string; text: string }> }>;
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
  const appendEntry = vi.fn();
  const api = {
    on(event: string, handler: Handler) { handlers.set(event, handler); },
    registerTool(value: RegisteredToolLike) { tools.set(value.name, value); },
    registerCommand(name: string, value: RegisteredCommandLike) { commands.set(name, value); },
    sendUserMessage,
    appendEntry,
  } as unknown as ExtensionAPI;
  const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
  let contextUsage: { tokens: number; contextWindow: number; percent: number } | undefined;
  let sessionEntries: unknown[] = [];
  let idle = true;
  let pendingMessages = false;
  const context = {
    sessionManager: { getSessionId: () => "session", getEntries: () => sessionEntries },
    compact,
    isIdle: () => idle,
    hasPendingMessages: () => pendingMessages,
    getContextUsage: () => contextUsage,
  } as unknown as ExtensionContext;
  const emit = (name: string, event: Record<string, unknown> = {}, eventContext: ExtensionContext = context) => handlers.get(name)?.(event, eventContext);
  return {
    api,
    compact,
    context,
    emit,
    sendUserMessage,
    appendEntry,
    setContextUsage(tokens: number, contextWindow: number) { contextUsage = { tokens, contextWindow, percent: tokens / contextWindow }; },
    setIdle(value: boolean) { idle = value; },
    setPendingMessages(value: boolean) { pendingMessages = value; },
    setSessionEntries(entries: unknown[]) { sessionEntries = entries; },
    getTool: (name = "self_compact") => tools.get(name),
    getCommand: (name: string) => commands.get(name),
  };
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
    const result = await tool?.execute("tool-call", { instructions: "reload durable state" }, undefined, undefined, test.context);
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

  it("starts only an internally attested Remote request from session-start proof and invalidates at public activity seams", () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    const snapshot = getContextLifecycleSnapshotV1();
    const request = () => requestCompaction({
      requestId: "remote-idle",
      sessionId: snapshot.sessionId ?? "",
      generationId: snapshot.generationId ?? "",
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });

    expect(request()).toMatchObject({ disposition: "accepted" });
    expect(test.compact).toHaveBeenCalledTimes(1);
    expect(test.appendEntry.mock.calls.map(([, claim]) => (claim as { state: string }).state)).toEqual(["requested", "compacting"]);
    test.emit("session_shutdown", { type: "session_shutdown", reason: "reload" });

    const blocked = harness();
    contextLifecycleExtension(blocked.api);
    blocked.emit("session_start", { type: "session_start", reason: "startup" });
    const invalidatingEvents = ["input", "before_agent_start", "agent_start", "session_before_switch", "session_before_fork", "session_before_tree"];
    for (const event of invalidatingEvents) {
      blocked.emit(event, { type: event });
      const current = getContextLifecycleSnapshotV1();
      expect(requestCompaction({
        requestId: `remote-${event}`,
        sessionId: current.sessionId ?? "",
        generationId: current.generationId ?? "",
        reason: "remote",
        source: "remote-pi-action",
        actor: "operator",
        channel: "remote",
        settlementPolicy: "current-or-next-settled-boundary",
      })).toMatchObject({ disposition: "rejected", code: "settlement-proof-unavailable" });
      blocked.emit("agent_settled", { type: "agent_settled" });
    }
    const current = getContextLifecycleSnapshotV1();
    expect(requestCompaction({
      requestId: "wrong-attestation",
      sessionId: current.sessionId ?? "",
      generationId: current.generationId ?? "",
      reason: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    })).toMatchObject({ disposition: "rejected", code: "invalid-start-authority" });
  });

  it("arms, emits once, and rearms the high-context handoff watcher", () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });

    test.setContextUsage(95, 100);
    test.emit("turn_end", { type: "turn_end" });
    test.emit("turn_end", { type: "turn_end" });
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(test.sendUserMessage.mock.calls[0]?.[0]).toContain("durable handoff");
    expect(test.sendUserMessage.mock.calls[0]?.[0]).toContain("self_compact");

    test.setContextUsage(60, 100);
    test.emit("turn_end", { type: "turn_end" });
    test.setContextUsage(95, 100);
    test.emit("turn_end", { type: "turn_end" });
    expect(test.sendUserMessage).toHaveBeenCalledTimes(2);
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

    test.emit("session_before_compact", { type: "session_before_compact", reason: "threshold", willRetry: false, signal: new AbortController().signal });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "observed-preflight", reason: "threshold" });

    test.emit("session_compact", { type: "session_compact", reason: "threshold", fromExtension: false, willRetry: false });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.sendUserMessage).not.toHaveBeenCalled();
  });

  it("adopts automatic compaction that overtakes a pending self tool and resumes once", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    await test.getTool()?.execute("tool-call", {}, undefined, undefined, test.context);
    expect(getContextLifecycleSnapshotV1().phase).toBe("pending-settle");

    test.emit("session_before_compact", { type: "session_before_compact", reason: "threshold", willRetry: false, signal: new AbortController().signal });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "observed-preflight", reason: "self" });
    expect(test.compact).not.toHaveBeenCalled();
    test.emit("session_compact", { type: "session_compact", reason: "threshold", fromExtension: false, willRetry: false });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "pending-settle", lastOutcome: "completed" });
    expect(test.sendUserMessage).not.toHaveBeenCalled();
    test.emit("agent_settled", { type: "agent_settled" });
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "resuming", lastOutcome: "completed" });
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);

    const resume = test.sendUserMessage.mock.calls[0]?.[0] as string;
    test.emit("message_start", { type: "message_start", message: { role: "user", content: resume } });
    test.emit("agent_settled", { type: "agent_settled" });
    await vi.waitFor(() => expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "completed" }));
    expect(test.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("classifies an automatic compaction abort signal as cancelled", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    const cancellation = new AbortController();

    test.emit("session_before_compact", { type: "session_before_compact", reason: "overflow", willRetry: true, signal: cancellation.signal });
    expect(getContextLifecycleSnapshotV1().phase).toBe("observed-preflight");
    cancellation.abort();
    await Promise.resolve();

    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "cancelled" });
    expect(test.sendUserMessage).not.toHaveBeenCalled();
  });

  it("keeps handoff tool guidance honest and replaces the session only from command context", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);

    const tool = test.getTool("handoff_new_session");
    expect(tool).toBeDefined();
    const result = await tool?.execute("handoff-tool", { handoffPath: "HANDOFF.md", nextStep: "Run the next test." }, undefined, undefined, test.context);
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

  it("restores and repairs an old-owner ambiguous resume only after process replacement", async () => {
    const test = harness();
    test.setSessionEntries([{
      type: "custom",
      customType: "pi-context-lifecycle",
      data: {
        schemaVersion: 1,
        ownerInstanceId: "old-owner",
        originOwnerInstanceId: "old-owner",
        operationId: "old-operation",
        sessionId: "session",
        generationId: "old-generation",
        state: "resume-admitting",
        reason: "self",
        resumeIntent: true,
        timestamp: 1,
      },
    }]);
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "reload" });
    const blocked = getContextLifecycleSnapshotV1();
    expect(blocked).toMatchObject({ phase: "blocked-unknown", operationId: "old-operation" });

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
      evidenceClass: "owner-process-replaced",
      actor: "operator",
      channel: "command",
    })}`, commandContext);

    await Promise.resolve();
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(test.sendUserMessage).not.toHaveBeenCalled();
    expect(notify).toHaveBeenCalledWith(expect.stringContaining("applied"), "info");
  });

  it("fences old callbacks and contexts across a same-session-id reload", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    const oldGeneration = getContextLifecycleSnapshotV1().generationId;
    await test.getTool()?.execute("old-tool", {}, undefined, undefined, test.context);
    test.emit("agent_settled", { type: "agent_settled" });
    expect(test.compact).toHaveBeenCalledTimes(1);
    const oldComplete = () => test.compact.mock.calls[0]?.[0].onComplete();

    const newCompact = vi.fn<ManagedCompactionAdapter["compact"]>();
    const newContext = {
      sessionManager: { getSessionId: () => "session", getEntries: () => [] },
      compact: newCompact,
      getContextUsage: () => undefined,
    } as unknown as ExtensionContext;
    test.emit("session_start", { type: "session_start", reason: "reload" }, newContext);
    const replacement = getContextLifecycleSnapshotV1();
    expect(replacement).toMatchObject({ registryState: "ready", sessionId: "session", phase: "idle" });
    expect(replacement.generationId).not.toBe(oldGeneration);

    oldComplete();
    test.emit("agent_settled", { type: "agent_settled" }, test.context);
    expect(getContextLifecycleSnapshotV1()).toMatchObject({ sessionId: "session", generationId: replacement.generationId, phase: "idle" });
    expect(newCompact).not.toHaveBeenCalled();

    const staleTool = await test.getTool()?.execute("stale-tool", {}, undefined, undefined, test.context);
    expect(staleTool?.content[0]?.text).toContain("rejected (session-unavailable)");
    await test.getTool()?.execute("new-tool", {}, undefined, undefined, newContext);
    test.emit("agent_settled", { type: "agent_settled" }, newContext);
    expect(newCompact).toHaveBeenCalledTimes(1);
  });

  it("persists the bounded managed operation and resume claim sequence without content", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    await test.getTool()?.execute("tool-call", { instructions: "sensitive focus must not persist" }, undefined, undefined, test.context);
    test.emit("agent_settled", { type: "agent_settled" });
    test.emit("session_compact", { type: "session_compact", reason: "manual", fromExtension: false });
    test.compact.mock.calls[0]?.[0].onComplete();
    const resume = test.sendUserMessage.mock.calls[0]?.[0] as string;
    test.emit("message_start", { type: "message_start", message: { role: "user", content: resume } });
    test.emit("agent_settled", { type: "agent_settled" });
    await Promise.resolve();

    const claims = test.appendEntry.mock.calls
      .filter(([customType]) => customType === "pi-context-lifecycle")
      .map(([, claim]) => claim as { state: string });
    expect(claims.map((claim) => claim.state)).toEqual([
      "requested",
      "compacting",
      "compacted",
      "resume-pending",
      "resume-admitting",
      "resume-admitted",
      "resume-settled",
      "released",
    ]);
    expect(JSON.stringify(claims)).not.toContain("sensitive focus must not persist");
  });

  it("does not attribute a threshold event to the managed compact call", async () => {
    const test = harness();
    contextLifecycleExtension(test.api);
    test.emit("session_start", { type: "session_start", reason: "startup" });
    await test.getTool()?.execute("tool-call", {}, undefined, undefined, test.context);
    test.emit("agent_settled", { type: "agent_settled" });
    test.emit("session_compact", { type: "session_compact", reason: "threshold", fromExtension: false });
    test.compact.mock.calls[0]?.[0].onComplete();
    expect(getContextLifecycleSnapshotV1().phase).toBe("blocked-unknown");
    expect(test.sendUserMessage).not.toHaveBeenCalled();
  });
});
