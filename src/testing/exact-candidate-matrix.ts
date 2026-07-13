import { realpath } from "node:fs/promises";
import {
  AuthStorage,
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  type CreateAgentSessionRuntimeFactory,
  type AgentSessionEvent,
  type AgentSessionRuntime,
  type ExtensionAPI,
} from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { createDeferredFakeProvider } from "./deferred-provider.js";
import { loadExactCandidateProbe, type ExactCandidateConsumer, type ExactCandidateProbe, type ExactCandidateProbeInjection } from "./exact-candidate-probe.js";
import type { ExactCandidateScenarioContext } from "./exact-candidate.js";
import { ScenarioBlockedError } from "./scenario-driver.js";

const consumerPackageNames = ["pi-subagents", "remote-pi", "pi-background-tasks"] as const satisfies readonly ExactCandidateConsumer[];

function packageDirectory(context: ExactCandidateScenarioContext, packageName: ExactCandidateConsumer): string {
  const directory = context.packageDirectoriesByName[packageName];
  if (directory === undefined) throw new Error(`candidate manifest has no ${packageName} artifact`);
  return directory;
}

function providerExtension(provider: ReturnType<typeof createDeferredFakeProvider>, onSessionShutdown?: () => void, cancelBeforeCompaction = false): (pi: ExtensionAPI) => void {
  return (pi) => {
    if (onSessionShutdown !== undefined) pi.on("session_shutdown", onSessionShutdown);
    if (cancelBeforeCompaction) pi.on("session_before_compact", () => ({ cancel: true }));
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
}

async function waitFor<T>(promise: Promise<T>, label: string): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => { timeout = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), 10_000); }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

function injectionsFor(scenarioId: string, suffix: string): readonly ExactCandidateProbeInjection[] {
  const subagent = (outcome: "success" | "failure") => ({ consumer: "pi-subagents" as const, kind: "completion" as const, id: `subagent-${suffix}-${outcome}`, outcome });
  const mesh = (lane: "reply" | "unsolicited") => ({ consumer: "remote-pi" as const, kind: "mesh-arrival" as const, id: `mesh-${suffix}-${lane}`, lane });
  const compact = (id: string) => ({ consumer: "remote-pi" as const, kind: "compact-request" as const, id: `remote-${suffix}-${id}`, ownerId: `owner-${suffix}-${id}` });
  const background = (kind: "notify" | "loop" | "cron" | "watchdog") => ({ consumer: "pi-background-tasks" as const, kind, id: `background-${suffix}-${kind}` });
  switch (scenarioId) {
    case "remote-owner-join": return [compact("one"), compact("two")];
    case "subagent-before-during-after": return [subagent("success"), subagent("failure")];
    case "mesh-during-compaction": return [mesh("reply"), mesh("unsolicited")];
    case "background-due-during-compaction": return [background("notify"), background("loop"), background("cron"), background("watchdog")];
    case "all-producers-concurrent": return [subagent("success"), compact("one"), mesh("reply"), background("notify"), background("loop"), background("cron"), background("watchdog")];
    case "automatic-prehook-race": return [subagent("success"), mesh("reply"), background("notify"), background("loop"), background("cron"), background("watchdog")];
    case "large-context-reduction": return [subagent("success"), subagent("failure"), background("notify")];
    default: return [];
  }
}

interface InjectedProbeReceipt {
  input: ExactCandidateProbeInjection;
  receipt: Readonly<{ consumer: ExactCandidateConsumer; id: string; outcome: string }>;
}

async function inject(probes: readonly ExactCandidateProbe[], inputs: readonly ExactCandidateProbeInjection[], context: ExactCandidateScenarioContext): Promise<readonly InjectedProbeReceipt[]> {
  const receipts: InjectedProbeReceipt[] = [];
  for (const input of inputs) {
    const probe = probes.find((candidate) => candidate.consumer === input.consumer);
    if (probe === undefined) throw new Error(`candidate probe unavailable for ${input.consumer}`);
    const receipt = await probe.inject(input);
    receipts.push({ input, receipt });
    context.timeline.record({ type: "consumer-injection", consumerId: receipt.consumer, outcome: receipt.outcome, count: receipt.notificationCount ?? 0 });
  }
  return receipts;
}

function requireHeldIngress(receipts: readonly InjectedProbeReceipt[]): void {
  for (const { input, receipt } of receipts) {
    const expected = input.consumer === "remote-pi" && input.kind === "compact-request"
      ? "remote-operation"
      : "held";
    if (expected === "held" && receipt.outcome !== "held") {
      throw new Error(`expected ${input.consumer}:${input.id} to be held during compaction, got ${receipt.outcome}`);
    }
    if (expected === "remote-operation" && receipt.outcome !== "accepted" && receipt.outcome !== "coalesced") {
      throw new Error(`expected remote compact ${input.id} to join or own the active operation, got ${receipt.outcome}`);
    }
  }
}

async function requireReleasedInOrder(probes: readonly ExactCandidateProbe[], receipts: readonly InjectedProbeReceipt[], context: ExactCandidateScenarioContext): Promise<void> {
  for (const injected of receipts) {
    if (injected.input.consumer === "remote-pi" && injected.input.kind === "compact-request") continue;
    const probe = probes.find((candidate) => candidate.consumer === injected.receipt.consumer);
    if (probe === undefined) throw new Error(`candidate probe unavailable for ${injected.receipt.consumer}`);
    const observations = await probe.observations();
    const heldAt = observations.findIndex((entry) => entry.id === injected.receipt.id && entry.outcome === "held");
    const releasedAt = observations.findIndex((entry, index) => index > heldAt && entry.id === injected.receipt.id && entry.outcome === "released");
    if (heldAt < 0 || releasedAt < 0) {
      throw new Error(`archive probe did not prove held/released disposition for ${injected.receipt.consumer}:${injected.receipt.id}`);
    }
    context.timeline.record({ type: "consumer-release-observed", consumerId: injected.receipt.consumer, outcome: "released", sequence: releasedAt });
  }
}

function blockForPublicSdkLimitation(context: ExactCandidateScenarioContext, limitation: string): never {
  context.timeline.record({ type: "public-sdk-limitation", outcome: limitation });
  throw new ScenarioBlockedError(limitation);
}

async function disposeProbes(probes: readonly ExactCandidateProbe[]): Promise<void> {
  await Promise.all([...probes].reverse().map(async (probe) => probe.dispose()));
}

async function disposeScenarioRuntime(runtime: AgentSessionRuntime, probes: readonly ExactCandidateProbe[], getSessionShutdownCount: () => number, expectedSessionShutdownCount = 1): Promise<void> {
  try {
    // AgentSessionRuntime is the documented host that emits session_shutdown
    // and awaits archive extension cleanup before invalidating its session.
    await runtime.dispose();
  } finally {
    await disposeProbes(probes);
  }
  const sessionShutdownCount = getSessionShutdownCount();
  if (sessionShutdownCount !== expectedSessionShutdownCount) throw new Error(`expected ${expectedSessionShutdownCount} runtime session_shutdown events, got ${sessionShutdownCount}`);
}

/**
 * Runs one packaged managed-compaction race through the documented SDK session
 * surface. All candidate extensions are discovered from the disposable project
 * settings created by the archive runner; no source-worktree extension is used.
 */
export async function executeExactCandidateScenario(context: ExactCandidateScenarioContext): Promise<void> {
  // Pi 0.80.6 documents AgentSession prompt/compact/replacement APIs, but does
  // not expose its command dispatcher or a hook between automatic preparation
  // and extension preflight. Do not relabel a generic tool turn as proof of
  // those surfaces; preserve an explicit blocked receipt instead.
  if (context.scenarioId === "tool-alias-join") {
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-AgentSession-has-no-command-dispatch-for-self-compact-aliases");
  }
  if (context.scenarioId === "command-handoff") {
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-AgentSession-has-no-command-context-dispatch-for-handoff-new-session;-ExtensionCommandContext.newSession-is-command-only");
  }
  if (context.scenarioId === "automatic-prehook-race") {
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-event-stream-has-no-pause-point-between-automatic-preparation-and-session_before_compact");
  }
  if (context.scenarioId === "resume-admission-barrier") {
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-AgentSession-has-no-preflight-handler-delay-control-for-resume-admission");
  }
  if (context.scenarioId === "manual-compact-characterization") {
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-AgentSession.compact-is-available-but-does-not-expose-the-native-manual-hook-cancellation-race-needed-for-characterization");
  }
  if (context.scenarioId === "automatic-threshold-overflow") {
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-AgentSession-exposes-auto-compaction-settings-but-no-supported-deterministic-overflow-injector-before-extension-preflight");
  }
  const provider = createDeferredFakeProvider();
  const providerFailure = context.scenarioId === "provider-failure";
  const cancellation = context.scenarioId === "compaction-cancelled";
  const toolCalls = context.scenarioId === "tool-multi-tool"
    ? [
      fauxToolCall("self_compact", { instructions: "first managed request" }, { id: "self-compact-first" }),
      fauxToolCall("self_compact", { instructions: "joined managed request" }, { id: "self-compact-second" }),
    ]
    : [fauxToolCall("self_compact", { instructions: "reload durable state" }, { id: `self-compact-${context.scenarioId}` })];
  const first = provider.enqueue(fauxAssistantMessage(toolCalls, { stopReason: "toolUse" }));
  const second = provider.enqueue(fauxAssistantMessage("Settling managed request."));
  const history = providerFailure
    ? provider.enqueueFailure()
    : provider.enqueue(fauxAssistantMessage("History summary."));
  const turn = provider.enqueue(fauxAssistantMessage("Turn summary."));
  const resumed = provider.enqueue(fauxAssistantMessage("Resumed after managed compaction."));
  const authStorage = AuthStorage.inMemory();
  const modelRegistry = ModelRegistry.inMemory(authStorage);
  // Use the documented in-memory SettingsManager with exactly the archive
  // directories that Pi registered locally. This keeps compaction settings out
  // of project files while avoiding mutable worktree/module resolution.
  const settingsManager = SettingsManager.inMemory({
    packages: Object.values(context.packageDirectories),
    compaction: { enabled: true, reserveTokens: 1_000, keepRecentTokens: 1_000 },
  }, { projectTrusted: true });
  const sessionManager = SessionManager.create(context.runtimeRoot, context.roots.sessions);
  for (let index = 0; index < 5; index += 1) {
    sessionManager.appendMessage({ role: "user", content: `candidate-history-${index} ${"u".repeat(4_000)}`, timestamp: Date.now() });
    sessionManager.appendMessage(fauxAssistantMessage(`candidate-response-${index} ${"a".repeat(4_000)}`));
  }
  let sessionShutdownCount = 0;
  const createRuntime: CreateAgentSessionRuntimeFactory = async ({ cwd, agentDir, sessionManager: targetSessionManager, sessionStartEvent }) => {
    const services = await createAgentSessionServices({
      cwd,
      agentDir,
      authStorage,
      modelRegistry,
      settingsManager,
      resourceLoaderOptions: {
        extensionFactories: [providerExtension(provider, () => {
          sessionShutdownCount += 1;
          context.timeline.record({ type: "runtime-session-shutdown", count: sessionShutdownCount });
        }, cancellation)],
      },
      resourceLoaderReloadOptions: { resolveProjectTrust: () => Promise.resolve(true) },
    });
    return {
      ...(await createAgentSessionFromServices({
        services,
        sessionManager: targetSessionManager,
        ...(sessionStartEvent === undefined ? {} : { sessionStartEvent }),
        model: provider.getModel(),
        tools: ["self_compact"],
      })),
      services,
      diagnostics: services.diagnostics,
    };
  };
  const runtime = await createAgentSessionRuntime(createRuntime, {
    cwd: context.runtimeRoot,
    agentDir: context.roots.agentDir,
    sessionManager,
  });
  const extensions = runtime.services.resourceLoader.getExtensions().extensions;
  // Package loading canonicalizes macOS /tmp to /private/tmp. Compare the
  // archive runtime's canonical root so the external-root boundary remains
  // strict without rejecting its own archive extensions.
  const actualRuntimeRoot = await realpath(context.runtimeRoot);
  const candidateExtensions = extensions.filter((extension) => extension.path.startsWith(actualRuntimeRoot));
  if (candidateExtensions.length < 4 || extensions.some((extension) => !extension.path.startsWith(actualRuntimeRoot) && !extension.path.startsWith("<inline:"))) {
    throw new Error("candidate package extensions did not load exclusively from the archive runtime");
  }
  context.timeline.record({ type: "archive-extensions-loaded", count: candidateExtensions.length });
  if (context.scenarioId === "reload-replacement-repair") {
    await runtime.session.bindExtensions({ mode: "print" });
    // AgentSessionRuntime.newSession is the documented replacement host. It
    // awaits outgoing archive session_shutdown before loading the replacement;
    // the registered repair command remains deliberately unreachable because
    // AgentSession has no public command dispatcher.
    const replacement = await runtime.newSession();
    if (replacement.cancelled || sessionShutdownCount !== 1) throw new Error("public runtime replacement did not await exactly one archive shutdown");
    const replacementExtensions = runtime.services.resourceLoader.getExtensions().extensions
      .filter((extension) => extension.path.startsWith(actualRuntimeRoot));
    if (replacementExtensions.length < 4) throw new Error("replacement runtime did not reload archive extensions");
    context.timeline.record({ type: "public-runtime-replacement-observed", count: replacementExtensions.length });
    return blockForPublicSdkLimitation(context, "pi-0.80.6-public-AgentSessionRuntime-can-replace-a-session-but-cannot-invoke-the-registered-repair-command-without-command-context");
  }
  const session = runtime.session;
  const probes: ExactCandidateProbe[] = [];
  let activeRuns = 0;
  let maxActiveRuns = 0;
  let compactionStarts = 0;
  let compactionEnds = 0;
  let settledCount = 0;
  let toolExecutionStarts = 0;
  let toolExecutionEnds = 0;
  let toolResultStarts = 0;
  let resolveTwiceSettled!: () => void;
  let resolveStart!: () => void;
  let resolveEnd!: () => void;
  const compactionStarted = new Promise<void>((resolve) => { resolveStart = resolve; });
  const compactionEnded = new Promise<void>((resolve) => { resolveEnd = resolve; });
  const twiceSettled = new Promise<void>((resolve) => { resolveTwiceSettled = resolve; });
  const unsubscribe = session.subscribe((event: AgentSessionEvent) => {
    if (event.type === "agent_start") { activeRuns += 1; maxActiveRuns = Math.max(maxActiveRuns, activeRuns); }
    if (event.type === "agent_settled") {
      activeRuns -= 1;
      settledCount += 1;
      if (settledCount === 2) resolveTwiceSettled();
    }
    if (event.type === "compaction_start") { compactionStarts += 1; resolveStart(); }
    if (event.type === "tool_execution_start") toolExecutionStarts += 1;
    if (event.type === "tool_execution_end") toolExecutionEnds += 1;
    if (event.type === "message_start" && event.message.role === "toolResult") toolResultStarts += 1;
    if (event.type === "compaction_end") {
      compactionEnds += 1;
      context.timeline.record({ type: "compaction-terminal", outcome: event.aborted ? "cancelled" : event.errorMessage === undefined ? "completed" : "failed" });
      resolveEnd();
    }
  });
  try {
    await session.bindExtensions({ mode: "print" });
    context.timeline.record({ type: "public-session-bound" });
    for (const packageName of consumerPackageNames) {
      probes.push(await loadExactCandidateProbe({ packageName, packageDirectory: packageDirectory(context, packageName), session, seed: context.seed }));
      context.timeline.record({ type: "consumer-probe-loaded", consumerId: packageName, count: probes.length });
    }
    const beforeInjections = context.scenarioId === "subagent-before-during-after"
      ? await inject(probes, [{ consumer: "pi-subagents", kind: "completion", id: "subagent-before-success", outcome: "success" }], context)
      : [];
    for (const receipt of beforeInjections) {
      if (receipt.receipt.outcome !== "accepted") throw new Error(`expected pre-compaction ${receipt.receipt.consumer}:${receipt.receipt.id} to be accepted, got ${receipt.receipt.outcome}`);
    }
    const prompt = session.prompt(`Run exact candidate scenario ${context.scenarioId}.`);
    await waitFor(first.call, "initial tool provider call");
    first.release();
    await waitFor(second.call, "post-tool provider call");
    second.release();
    await waitFor(compactionStarted, "compaction_start");
    context.timeline.record({ type: "compaction-start-observed", count: compactionStarts });
    const heldInjections = await inject(probes, injectionsFor(context.scenarioId, "during"), context);
    requireHeldIngress(heldInjections);
    if (cancellation) {
      await waitFor(compactionEnded, "cancelled compaction_end");
      await waitFor(prompt, "cancelled scenario prompt settlement");
      if (compactionStarts !== 1 || compactionEnds !== 1) throw new Error(`expected one cancelled compaction, got starts=${compactionStarts} ends=${compactionEnds}`);
      if (provider.callCount !== 2 || settledCount !== 1) throw new Error("cancelled compaction unexpectedly requested summaries or a resume");
    } else {
      const historyCall = await waitFor(history.call, "history summary provider call");
      history.release();
      if (providerFailure) {
        await waitFor(compactionEnded, "failed compaction_end");
        await waitFor(prompt, "failed scenario prompt settlement");
        if (settledCount !== 1 || provider.callCount !== 3) throw new Error("failed compaction unexpectedly admitted a resume");
      } else {
        await waitFor(turn.call, "turn summary provider call");
        turn.release();
        await waitFor(compactionEnded, "compaction_end");
        const afterInjections = context.scenarioId === "subagent-before-during-after"
          ? await inject(probes, [{ consumer: "pi-subagents", kind: "completion", id: "subagent-after-success", outcome: "success" }], context)
          : [];
        requireHeldIngress(afterInjections);
        await waitFor(resumed.call, "resume provider call");
        resumed.release();
        await waitFor(prompt, "scenario prompt settlement");
        await waitFor(twiceSettled, "resume settlement");
        context.timeline.record({ type: "resume-settlement-observed", count: settledCount });
        if (context.scenarioId === "large-context-reduction") {
          const beforeBytes = JSON.stringify(historyCall.context.messages).length;
          const afterBytes = JSON.stringify((await resumed.call).context.messages).length;
          context.timeline.record({ type: "context-bytes-before", count: beforeBytes });
          context.timeline.record({ type: "context-bytes-after", count: afterBytes });
          if (afterBytes >= beforeBytes) throw new Error(`managed compaction did not reduce measured context bytes: before=${beforeBytes}, after=${afterBytes}`);
        }
        await requireReleasedInOrder(probes, [...heldInjections, ...afterInjections], context);
      }
    }
    const expectedOutcome = cancellation ? "cancelled" : providerFailure ? "failed" : "completed";
    const terminal = context.timeline.events().filter((event) => event.type === "compaction-terminal");
    if (terminal.length !== 1 || terminal[0]?.outcome !== expectedOutcome) throw new Error(`expected ${expectedOutcome} terminal event exactly once`);
    if (compactionStarts !== 1 || compactionEnds !== 1) throw new Error(`expected exactly one managed compaction, got starts=${compactionStarts} ends=${compactionEnds}`);
    const expectedToolCount = toolCalls.length;
    if (toolExecutionStarts !== expectedToolCount || toolExecutionEnds !== expectedToolCount || toolResultStarts !== expectedToolCount) {
      throw new Error(`expected ${expectedToolCount} corresponding tool executions/results, got starts=${toolExecutionStarts} ends=${toolExecutionEnds} results=${toolResultStarts}`);
    }
    if (maxActiveRuns > 1 || activeRuns !== 0) throw new Error("candidate scenario observed overlapping or unsettled model runs");
    if (sessionManager.getEntries().filter((entry) => entry.type === "compaction").length !== (providerFailure || cancellation ? 0 : 1)) throw new Error("candidate scenario persisted an unexpected compaction entry count");
    for (const probe of probes) {
      const observations = await probe.observations();
      context.timeline.record({ type: "consumer-observations", consumerId: probe.consumer, count: observations.length });
    }
  } finally {
    unsubscribe();
    await disposeScenarioRuntime(runtime, probes, () => sessionShutdownCount, context.scenarioId === "reload-replacement-repair" ? 2 : 1);
  }
}

/** A bounded soak cycle uses the same packaged public-SDK managed race. */
export async function runExactCandidateSoakCycle(context: ExactCandidateScenarioContext, cycle: number): Promise<void> {
  const cycleContext: ExactCandidateScenarioContext = { ...context, scenarioId: `soak-${cycle}` };
  await executeExactCandidateScenario(cycleContext);
}
