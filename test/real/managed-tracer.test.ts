import {
  AuthStorage,
  createAgentSession,
  DefaultResourceLoader,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  type AgentSessionEvent,
  type ExtensionAPI,
  type ExtensionCommandContextActions,
} from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import contextLifecycleExtension from "../../src/extension.js";
import { createDeferredFakeProvider, createDisposableHarnessRoots, withDisposableHarnessEnvironment } from "../../src/testing/index.js";
import { getContextLifecycleDiagnosticsV1, getContextLifecycleSnapshotV1 } from "../../src/registry.js";
import { CONTEXT_LIFECYCLE_REGISTRY_SYMBOL } from "../../src/types.js";

interface TimelineEvent { index: number; type: string; role?: string; entryType?: string }
type NewSessionOptions = NonNullable<Parameters<ExtensionCommandContextActions["newSession"]>[0]>;
type ReplacementContext = Parameters<NonNullable<NewSessionOptions["withSession"]>>[0];

function timelineRecord(event: AgentSessionEvent): Omit<TimelineEvent, "index"> {
  if (event.type === "message_start" || event.type === "message_end") return { type: event.type, role: event.message.role };
  if (event.type === "entry_appended") return { type: event.type, entryType: event.entry.type };
  return { type: event.type };
}

function claimState(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const record = value as Record<string, unknown>;
  return typeof record.state === "string" ? record.state : undefined;
}

function textContent(content: unknown): string | undefined {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return undefined;
  const text: string[] = [];
  for (const block of content as unknown[]) {
    if (typeof block !== "object" || block === null) return undefined;
    const record = block as Record<string, unknown>;
    if (record.type !== "text" || typeof record.text !== "string") return undefined;
    text.push(record.text);
  }
  return text.join("");
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
      const resumedInvocation = await resumed.call;
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(timeline.filter((event) => event.type === "extension_session_compact"), JSON.stringify(timeline)).toHaveLength(1);
      const compactionEnd = timeline.findIndex((event) => event.type === "compaction_end");
      const resumeMessageStart = [...timeline].reverse().find((event) => event.type === "message_start" && event.role === "user")?.index ?? -1;
      expect(compactionEnd).toBeGreaterThanOrEqual(0);
      expect(compactionEnd).toBeLessThan(resumeMessageStart);
      const resumedUserMessage = [...resumedInvocation.context.messages].reverse().find((message) => message.role === "user");
      expect(textContent(resumedUserMessage?.content)).toMatch(/pi-context-lifecycle:v1 resume operationId=.+ generationId=.+/);
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

  it("releases the unchanged session without resume when the managed compaction provider fails", async () => {
    const roots = await createDisposableHarnessRoots();
    try {
      await withDisposableHarnessEnvironment(roots, async () => {
        const provider = createDeferredFakeProvider();
        const first = provider.enqueue(fauxAssistantMessage(fauxToolCall("self_compact", {}, { id: "self-compact-failure" }), { stopReason: "toolUse" }));
        const second = provider.enqueue(fauxAssistantMessage("Ending the requesting run before managed failure."));
        const failure = provider.enqueueFailure();
        const authStorage = AuthStorage.inMemory();
        const modelRegistry = ModelRegistry.inMemory(authStorage);
        const settingsManager = SettingsManager.inMemory({ compaction: { enabled: true, reserveTokens: 1_000, keepRecentTokens: 1_000 } });
        const providerExtension = (pi: ExtensionAPI) => {
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
        const loader = new DefaultResourceLoader({ cwd: roots.cwd, agentDir: roots.agentDir, settingsManager, extensionFactories: [providerExtension, contextLifecycleExtension] });
        await loader.reload();
        const sessionManager = SessionManager.create(roots.cwd, roots.sessions);
        for (let index = 0; index < 5; index += 1) {
          sessionManager.appendMessage({ role: "user", content: `failure-history-${index} ${"u".repeat(4_000)}`, timestamp: Date.now() });
          sessionManager.appendMessage(fauxAssistantMessage(`failure-response-${index} ${"a".repeat(4_000)}`));
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
        let resumeStarts = 0;
        let compactionStarts = 0;
        const unsubscribe = session.subscribe((event) => {
          if (event.type === "compaction_start") compactionStarts += 1;
          if (event.type === "message_start" && event.message.role === "user" && textContent(event.message.content)?.includes("pi-context-lifecycle:v1 resume") === true) resumeStarts += 1;
        });
        const wait = async (promise: Promise<unknown>, label: string): Promise<void> => {
          let timer: NodeJS.Timeout | undefined;
          try {
            await Promise.race([promise, new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 5_000); })]);
          } finally {
            if (timer !== undefined) clearTimeout(timer);
          }
        };
        try {
          const prompt = session.prompt("Run managed self compaction and exercise provider failure.");
          await wait(first.call, "initial provider call");
          first.release();
          await wait(second.call, "post-tool provider call");
          second.release();
          await wait(failure.call, "failing compaction provider call");
          failure.release();
          await wait(prompt, "requesting prompt settlement");
          await vi.waitFor(() => expect(getContextLifecycleSnapshotV1()).toMatchObject({ phase: "idle", lastOutcome: "failed" }), { timeout: 5_000 });

          expect(compactionStarts).toBe(1);
          expect(resumeStarts).toBe(0);
          expect(provider.callCount).toBe(3);
          expect(sessionManager.getEntries().filter((entry) => entry.type === "compaction")).toHaveLength(0);
          const claimStates = sessionManager.getEntries()
            .map((entry) => entry.type === "custom" && entry.customType === "pi-context-lifecycle" ? claimState(entry.data) : undefined)
            .filter((state): state is string => state !== undefined);
          expect(claimStates).toContain("failed");
          expect(claimStates).toContain("released");
        } finally {
          unsubscribe();
          session.dispose();
        }
      });
    } finally {
      await roots.cleanup();
    }
  });

  it("adopts native automatic threshold compaction and returns to idle without self resume", async () => {
    const roots = await createDisposableHarnessRoots();
    try {
      await withDisposableHarnessEnvironment(roots, async () => {
        const provider = createDeferredFakeProvider({ models: [{ id: "threshold-model", contextWindow: 40_000, maxTokens: 8_000 }] });
        const first = provider.enqueue(fauxAssistantMessage(`large-response ${"r".repeat(30_000)}`));
        const historySummary = provider.enqueue(fauxAssistantMessage("Automatic history summary."));
        const turnSummary = provider.enqueue(fauxAssistantMessage("Automatic turn summary."));
        const prefixSummary = provider.enqueue(fauxAssistantMessage("Automatic turn prefix summary."));
        const authStorage = AuthStorage.inMemory();
        const modelRegistry = ModelRegistry.inMemory(authStorage);
        const settingsManager = SettingsManager.inMemory({ compaction: { enabled: true, reserveTokens: 15_000, keepRecentTokens: 2_000 } });
        const reasons: string[] = [];
        const observerExtension = (pi: ExtensionAPI) => {
          pi.on("session_before_compact", (event) => { reasons.push(`before:${event.reason}`); });
          pi.on("session_compact", (event) => { reasons.push(`after:${event.reason}`); });
          pi.on("agent_settled", () => { reasons.push("agent-settled"); });
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
        const loader = new DefaultResourceLoader({ cwd: roots.cwd, agentDir: roots.agentDir, settingsManager, extensionFactories: [observerExtension, contextLifecycleExtension] });
        await loader.reload();
        const sessionManager = SessionManager.create(roots.cwd, roots.sessions);
        for (let index = 0; index < 10; index += 1) {
          sessionManager.appendMessage({ role: "user", content: `threshold-history-${index} ${"u".repeat(4_000)}`, timestamp: Date.now() });
          sessionManager.appendMessage(fauxAssistantMessage(`threshold-response-${index} ${"a".repeat(4_000)}`));
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
          tools: [],
        });
        await session.bindExtensions({ mode: "print" });
        let resumeStarts = 0;
        const unsubscribe = session.subscribe((event) => {
          if (event.type === "compaction_start") reasons.push("session:compaction_start");
          if (event.type === "compaction_end") reasons.push(`session:compaction_end:result=${event.result === undefined ? "none" : "present"}:aborted=${String(event.aborted)}:error=${event.errorMessage ?? "none"}`);
          if (event.type === "message_start" && event.message.role === "user" && textContent(event.message.content)?.includes("pi-context-lifecycle:v1 resume") === true) resumeStarts += 1;
        });
        const wait = async (promise: Promise<unknown>, label: string): Promise<void> => {
          let timer: NodeJS.Timeout | undefined;
          try {
            await Promise.race([promise, new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}; calls=${provider.callCount}; reasons=${JSON.stringify(reasons)}`)), 5_000); })]);
          } finally {
            if (timer !== undefined) clearTimeout(timer);
          }
        };
        try {
          const prompt = session.prompt("Produce the large threshold response.");
          await wait(first.call, "threshold agent response");
          first.release();
          await wait(historySummary.call, "automatic history summary");
          historySummary.release();
          await wait(turnSummary.call, "automatic turn summary");
          turnSummary.release();
          await wait(prefixSummary.call, "automatic turn prefix summary");
          prefixSummary.release();
          await wait(prompt, "automatic threshold prompt");
          await vi.waitFor(() => expect(getContextLifecycleSnapshotV1(), JSON.stringify({ reasons, diagnostics: getContextLifecycleDiagnosticsV1() })).toMatchObject({ phase: "idle", lastOutcome: "completed" }), { timeout: 5_000 });

          expect(reasons).toEqual(["session:compaction_start", "before:threshold", "after:threshold", "session:compaction_end:result=present:aborted=false:error=none", "agent-settled"]);
          expect(resumeStarts).toBe(0);
          expect(provider.callCount).toBe(4);
          expect(sessionManager.getEntries().filter((entry) => entry.type === "compaction")).toHaveLength(1);
        } finally {
          unsubscribe();
          session.dispose();
        }
      });
    } finally {
      await roots.cleanup();
    }
  });

  it("routes fresh handoff through public command context exactly once without a provider turn", async () => {
    const roots = await createDisposableHarnessRoots();
    try {
      await withDisposableHarnessEnvironment(roots, async () => {
        const provider = createDeferredFakeProvider();
        const authStorage = AuthStorage.inMemory();
        const modelRegistry = ModelRegistry.inMemory(authStorage);
        const settingsManager = SettingsManager.inMemory();
        const providerExtension = (pi: ExtensionAPI) => {
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
        const loader = new DefaultResourceLoader({ cwd: roots.cwd, agentDir: roots.agentDir, settingsManager, extensionFactories: [providerExtension, contextLifecycleExtension] });
        await loader.reload();
        const sessionManager = SessionManager.create(roots.cwd, roots.sessions);
        const { session } = await createAgentSession({
          cwd: roots.cwd,
          agentDir: roots.agentDir,
          authStorage,
          modelRegistry,
          settingsManager,
          resourceLoader: loader,
          sessionManager,
          model: provider.getModel(),
          tools: [],
        });
        let replacements = 0;
        const kickoffs: string[] = [];
        const replacementContext = {
          sendUserMessage: (message: string) => { kickoffs.push(message); return Promise.resolve(); },
        } as unknown as ReplacementContext;
        const commandContextActions: ExtensionCommandContextActions = {
          waitForIdle: () => Promise.resolve(),
          newSession: async (options) => {
            replacements += 1;
            await options?.withSession?.(replacementContext);
            return { cancelled: false };
          },
          fork: () => Promise.resolve({ cancelled: true }),
          navigateTree: () => Promise.resolve({ cancelled: true }),
          switchSession: () => Promise.resolve({ cancelled: true }),
          reload: () => Promise.resolve(),
        };
        await session.bindExtensions({ mode: "print", commandContextActions });
        try {
          await session.prompt(`/handoff-new-session ${JSON.stringify({ handoffPath: "HANDOFF.md", nextStep: "Run the next test." })}`);
          expect(replacements).toBe(1);
          expect(kickoffs).toHaveLength(1);
          expect(kickoffs[0]).toContain("HANDOFF.md");
          expect(kickoffs[0]).toContain("Run the next test.");
          expect(provider.callCount).toBe(0);
        } finally {
          session.dispose();
        }
      });
    } finally {
      await roots.cleanup();
    }
  });

  it.each(["handled", "transformed", "unrelated"] as const)("does not admit a %s resume path without an exact user message_start", async (mode) => {
    const roots = await createDisposableHarnessRoots();
    try {
      await withDisposableHarnessEnvironment(roots, async () => {
        const provider = createDeferredFakeProvider();
        const first = provider.enqueue(fauxAssistantMessage(fauxToolCall("self_compact", {}, { id: `self-compact-${mode}` }), { stopReason: "toolUse" }));
        const second = provider.enqueue(fauxAssistantMessage("Ending the requesting run."));
        const historySummary = provider.enqueue(fauxAssistantMessage("History summary."));
        const turnSummary = provider.enqueue(fauxAssistantMessage("Turn summary."));
        const adversarialResponse = mode === "handled" ? undefined : provider.enqueue(fauxAssistantMessage("Handled adversarial unrelated run."));
        const authStorage = AuthStorage.inMemory();
        const modelRegistry = ModelRegistry.inMemory(authStorage);
        const settingsManager = SettingsManager.inMemory({ compaction: { enabled: true, reserveTokens: 1_000, keepRecentTokens: 1_000 } });
        const timeline: TimelineEvent[] = [];
        const pushTimeline = (event: Omit<TimelineEvent, "index">): void => { timeline.push({ index: timeline.length, ...event }); };
        const adversarialText = mode === "transformed" ? "unrelated transformed resume input" : "independent unrelated user input";
        const adversaryExtension = (pi: ExtensionAPI) => {
          pi.on("input", (event) => {
            if (event.source !== "extension" || !event.text.includes("pi-context-lifecycle:v1 resume")) return;
            if (mode === "handled" || mode === "unrelated") return { action: "handled" as const };
            return { action: "transform" as const, text: adversarialText };
          });
          pi.on("message_start", (event) => {
            if (event.message.role === "user" && textContent(event.message.content) === adversarialText) pushTimeline({ type: "adversarial_unrelated_user_message" });
          });
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
          extensionFactories: [adversaryExtension, contextLifecycleExtension],
        });
        await loader.reload();
        const sessionManager = SessionManager.create(roots.cwd, roots.sessions);
        for (let index = 0; index < 5; index += 1) {
          sessionManager.appendMessage({ role: "user", content: `history-${index} ${"u".repeat(4_000)}`, timestamp: Date.now() });
          sessionManager.appendMessage(fauxAssistantMessage(`history-response-${index} ${"a".repeat(4_000)}`));
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
        let settledCount = 0;
        let finishCompaction!: () => void;
        let settleTwice!: () => void;
        const compactionFinished = new Promise<void>((resolve) => { finishCompaction = resolve; });
        const twiceSettled = new Promise<void>((resolve) => { settleTwice = resolve; });
        const unsubscribe = session.subscribe((event) => {
          pushTimeline(timelineRecord(event));
          if (event.type === "compaction_end") finishCompaction();
          if (event.type === "agent_settled") {
            settledCount += 1;
            if (settledCount === 2) settleTwice();
          }
        });
        const wait = async (promise: Promise<unknown>, label: string): Promise<void> => {
          let timer: NodeJS.Timeout | undefined;
          try {
            await Promise.race([
              promise,
              new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 5_000); }),
            ]);
          } finally {
            if (timer !== undefined) clearTimeout(timer);
          }
        };
        try {
          const prompt = session.prompt(`Run the ${mode} resume scenario.`);
          await wait(first.call, "initial call");
          first.release();
          await wait(second.call, "post-tool call");
          second.release();
          await wait(historySummary.call, "history summary");
          historySummary.release();
          await wait(turnSummary.call, "turn summary");
          turnSummary.release();
          await wait(compactionFinished, "compaction end");
          await prompt;
          if (adversarialResponse !== undefined) {
            const unrelatedPrompt = mode === "unrelated" ? session.prompt(adversarialText) : undefined;
            await wait(adversarialResponse.call, "adversarial unrelated run");
            adversarialResponse.release();
            if (unrelatedPrompt !== undefined) await unrelatedPrompt;
            await wait(twiceSettled, "adversarial run settlement");
          } else {
            await new Promise<void>((resolve) => setImmediate(resolve));
          }
          expect(getContextLifecycleSnapshotV1().phase).toBe("resuming");
          expect(getContextLifecycleDiagnosticsV1().some((entry) => entry.code === "resume-message-matched")).toBe(false);
          expect(timeline.filter((event) => event.type === "message_start" && event.role === "user")).toHaveLength(mode === "handled" ? 1 : 2);
          expect(timeline.filter((event) => event.type === "agent_start")).toHaveLength(mode === "handled" ? 1 : 2);
          expect(timeline.filter((event) => event.type === "adversarial_unrelated_user_message")).toHaveLength(mode === "handled" ? 0 : 1);
          expect(provider.callCount).toBe(mode === "handled" ? 4 : 5);
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
