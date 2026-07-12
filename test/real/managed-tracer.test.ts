import {
  AuthStorage,
  createAgentSession,
  DefaultResourceLoader,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  type AgentSessionEvent,
  type ExtensionAPI,
} from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it } from "vitest";
import contextLifecycleExtension from "../../src/extension.js";
import { createDeferredFakeProvider, createDisposableHarnessRoots, withDisposableHarnessEnvironment } from "../../src/testing/index.js";
import { getContextLifecycleSnapshotV1 } from "../../src/registry.js";
import { CONTEXT_LIFECYCLE_REGISTRY_SYMBOL } from "../../src/types.js";

interface TimelineEvent { index: number; type: string; role?: string; entryType?: string }

function timelineRecord(event: AgentSessionEvent): Omit<TimelineEvent, "index"> {
  if (event.type === "message_start" || event.type === "message_end") return { type: event.type, role: event.message.role };
  if (event.type === "entry_appended") return { type: event.type, entryType: event.entry.type };
  return { type: event.type };
}

describe("public SDK managed tracer", () => {
  afterEach(() => { Reflect.deleteProperty(globalThis, CONTEXT_LIFECYCLE_REGISTRY_SYMBOL); });

  it("proves tool-result -> settled -> one compaction -> one admitted/settled resume", async () => {
    const roots = await createDisposableHarnessRoots();
    try {
      await withDisposableHarnessEnvironment(roots, async () => {
    const provider = createDeferredFakeProvider();
    const first = provider.enqueue(fauxAssistantMessage(fauxToolCall("self_compact", { instructions: "reload HANDOFF.md" }, { id: "self-compact-call" }), { stopReason: "toolUse" }));
    const second = provider.enqueue(fauxAssistantMessage("Tool request recorded; ending the current run."));
    const historySummary = provider.enqueue(fauxAssistantMessage("Durable history summary."));
    const turnSummary = provider.enqueue(fauxAssistantMessage("Durable split-turn summary."));
    const resumed = provider.enqueue(fauxAssistantMessage("Resumed after reloading durable state."));
    const authStorage = AuthStorage.inMemory();
    const modelRegistry = ModelRegistry.inMemory(authStorage);
    const settingsManager = SettingsManager.inMemory({ compaction: { enabled: true, reserveTokens: 1_000, keepRecentTokens: 1_000 } });
    const timeline: TimelineEvent[] = [];
    const pushTimeline = (event: Omit<TimelineEvent, "index">): void => { timeline.push({ index: timeline.length, ...event }); };
    const providerExtension = (pi: ExtensionAPI) => {
      pi.on("agent_settled", () => { pushTimeline({ type: "extension_agent_settled" }); });
      pi.on("session_compact", () => { pushTimeline({ type: "extension_session_compact" }); });
      pi.registerProvider(provider.provider, {
        api: provider.api,
        apiKey: "disposable-test-key",
        baseUrl: "http://localhost.invalid",
        models: provider.models.map((model) => ({
          id: model.id,
          name: model.name,
          api: model.api,
          reasoning: model.reasoning,
          input: model.input,
          cost: model.cost,
          contextWindow: model.contextWindow,
          maxTokens: model.maxTokens,
        })),
        streamSimple: (model, context, options) => provider.streamSimple(model, context, options),
      });
    };
    const loader = new DefaultResourceLoader({
      cwd: roots.cwd,
      agentDir: roots.agentDir,
      settingsManager,
      extensionFactories: [providerExtension, contextLifecycleExtension],
    });
    await loader.reload();
    const sessionManager = SessionManager.create(roots.cwd, roots.sessions);
    for (let index = 0; index < 5; index += 1) {
      sessionManager.appendMessage({ role: "user", content: `historical-user-${index} ${"u".repeat(4_000)}`, timestamp: Date.now() });
      sessionManager.appendMessage(fauxAssistantMessage(`historical-assistant-${index} ${"a".repeat(4_000)}`));
    }
    const { session } = await createAgentSession({
      cwd: roots.cwd,
      agentDir: roots.agentDir,
      authStorage,
      modelRegistry,
      settingsManager,
      resourceLoader: loader,
      sessionManager,
      model: provider.getModel(),
      tools: ["self_compact"],
    });
    await session.bindExtensions({ mode: "print" });
    let activeRuns = 0;
    let maximumActiveRuns = 0;
    let settledCount = 0;
    let settleTwice!: () => void;
    const twiceSettled = new Promise<void>((resolve) => { settleTwice = resolve; });
    const unsubscribe = session.subscribe((event) => {
      pushTimeline(timelineRecord(event));
      if (event.type === "agent_start") { activeRuns += 1; maximumActiveRuns = Math.max(maximumActiveRuns, activeRuns); }
      if (event.type === "agent_settled") { activeRuns -= 1; settledCount += 1; if (settledCount === 2) settleTwice(); }
    });

    try {
      const prompt = session.prompt("Use self_compact now.");
      const awaitCall = async (call: Promise<unknown>, label: string): Promise<void> => {
        let timer: NodeJS.Timeout | undefined;
        try {
          await Promise.race([
            call,
            new Promise<never>((_resolve, reject) => {
              timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}; calls=${provider.callCount}; snapshot=${JSON.stringify(getContextLifecycleSnapshotV1())}; timeline=${JSON.stringify(timeline)}`)), 5_000);
            }),
          ]);
        } finally {
          if (timer !== undefined) clearTimeout(timer);
        }
      };
      await awaitCall(first.call, "initial provider call");
      first.release();
      await awaitCall(second.call, "post-tool provider call");
      second.release();
      await awaitCall(historySummary.call, "history compaction provider call");
      expect(timeline.filter((event) => event.type === "compaction_start")).toHaveLength(1);
      const settledBoundary = timeline.findIndex((event) => event.type === "extension_agent_settled");
      const compactStart = timeline.findIndex((event) => event.type === "compaction_start");
      const toolResultEnd = timeline.findIndex((event) => event.type === "message_end" && event.role === "toolResult");
      expect(toolResultEnd).toBeGreaterThanOrEqual(0);
      expect(toolResultEnd).toBeLessThan(settledBoundary);
      expect(settledBoundary).toBeLessThan(compactStart);
      historySummary.release();
      await awaitCall(turnSummary.call, "split-turn compaction provider call");
      turnSummary.release();
      await awaitCall(resumed.call, "resume provider call");
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(timeline.filter((event) => event.type === "extension_session_compact"), JSON.stringify(timeline)).toHaveLength(1);
      resumed.release();
      await prompt;
      await twiceSettled;
      expect(provider.callCount).toBe(5);
      expect(settledCount).toBe(2);
      expect(maximumActiveRuns).toBe(1);
      expect(activeRuns).toBe(0);
      expect(timeline.filter((event) => event.type === "compaction_start")).toHaveLength(1);
      expect(timeline.filter((event) => event.type === "extension_session_compact")).toHaveLength(1);
      expect(timeline.filter((event) => event.type === "tool_execution_start")).toHaveLength(1);
      expect(timeline.filter((event) => event.type === "tool_execution_end")).toHaveLength(1);
      expect(timeline.filter((event) => event.type === "message_start" && event.role === "toolResult")).toHaveLength(1);
      expect(timeline.filter((event) => event.type === "message_end" && event.role === "toolResult")).toHaveLength(1);
      expect(timeline.filter((event) => event.type === "message_start" && event.role === "user")).toHaveLength(2);
      expect(sessionManager.getEntries().filter((entry) => entry.type === "compaction")).toHaveLength(1);
      expect(process.env.HOME).toBe(roots.home);
      expect(process.env.PI_CODING_AGENT_DIR).toBe(roots.agentDir);
    } finally {
      unsubscribe();
      session.dispose();
    }
      });
    } finally {
      await roots.cleanup();
    }
  });
});
