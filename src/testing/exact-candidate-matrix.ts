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
import { createDeferredFakeProvider } from "./deferred-provider.js";
import { loadExactCandidateProbe, type ExactCandidateConsumer, type ExactCandidateProbe, type ExactCandidateProbeInjection } from "./exact-candidate-probe.js";
import type { ExactCandidateScenarioContext } from "./exact-candidate.js";

const consumerPackageNames = ["pi-subagents", "remote-pi", "pi-background-tasks"] as const satisfies readonly ExactCandidateConsumer[];

function packageDirectory(context: ExactCandidateScenarioContext, packageName: ExactCandidateConsumer): string {
  const directory = context.packageDirectoriesByName[packageName];
  if (directory === undefined) throw new Error(`candidate manifest has no ${packageName} artifact`);
  return directory;
}

function providerExtension(provider: ReturnType<typeof createDeferredFakeProvider>): (pi: ExtensionAPI) => void {
  return (pi) => {
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

async function inject(probes: readonly ExactCandidateProbe[], inputs: readonly ExactCandidateProbeInjection[], context: ExactCandidateScenarioContext): Promise<void> {
  for (const input of inputs) {
    const probe = probes.find((candidate) => candidate.consumer === input.consumer);
    if (probe === undefined) throw new Error(`candidate probe unavailable for ${input.consumer}`);
    const receipt = await probe.inject(input);
    context.timeline.record({ type: "consumer-injection", consumerId: receipt.consumer, outcome: receipt.outcome, count: receipt.notificationCount ?? 0 });
  }
}

async function disposeProbes(probes: readonly ExactCandidateProbe[]): Promise<void> {
  await Promise.all([...probes].reverse().map(async (probe) => probe.dispose()));
}

/**
 * Runs one packaged managed-compaction race through the documented SDK session
 * surface. All candidate extensions are discovered from the disposable project
 * settings created by the archive runner; no source-worktree extension is used.
 */
export async function executeExactCandidateScenario(context: ExactCandidateScenarioContext): Promise<void> {
  const provider = createDeferredFakeProvider();
  const first = provider.enqueue(fauxAssistantMessage(fauxToolCall("self_compact", { instructions: "reload durable state" }, { id: `self-compact-${context.scenarioId}` }), { stopReason: "toolUse" }));
  const second = provider.enqueue(fauxAssistantMessage("Settling managed request."));
  const history = provider.enqueue(fauxAssistantMessage("History summary."));
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
  const loader = new DefaultResourceLoader({
    cwd: context.runtimeRoot,
    agentDir: context.roots.agentDir,
    settingsManager,
    extensionFactories: [providerExtension(provider)],
  });
  await loader.reload({ resolveProjectTrust: () => Promise.resolve(true) });
  const extensions = loader.getExtensions().extensions;
  const candidateExtensions = extensions.filter((extension) => extension.path.startsWith(context.runtimeRoot));
  if (candidateExtensions.length < 4 || extensions.some((extension) => !extension.path.startsWith(context.runtimeRoot) && !extension.path.startsWith("<inline:"))) {
    throw new Error("candidate package extensions did not load exclusively from the archive runtime");
  }
  context.timeline.record({ type: "archive-extensions-loaded", count: candidateExtensions.length });
  const sessionManager = SessionManager.create(context.runtimeRoot, context.roots.sessions);
  for (let index = 0; index < 5; index += 1) {
    sessionManager.appendMessage({ role: "user", content: `candidate-history-${index} ${"u".repeat(4_000)}`, timestamp: Date.now() });
    sessionManager.appendMessage(fauxAssistantMessage(`candidate-response-${index} ${"a".repeat(4_000)}`));
  }
  const { session } = await createAgentSession({
    cwd: context.runtimeRoot,
    agentDir: context.roots.agentDir,
    authStorage,
    modelRegistry,
    settingsManager,
    resourceLoader: loader,
    sessionManager,
    model: provider.getModel(),
    tools: ["self_compact"],
  });
  const probes: ExactCandidateProbe[] = [];
  let activeRuns = 0;
  let maxActiveRuns = 0;
  let compactionStarts = 0;
  let compactionEnds = 0;
  let settledCount = 0;
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
    const prompt = session.prompt(`Run exact candidate scenario ${context.scenarioId}.`);
    await waitFor(first.call, "initial tool provider call");
    first.release();
    await waitFor(second.call, "post-tool provider call");
    second.release();
    await waitFor(compactionStarted, "compaction_start");
    context.timeline.record({ type: "compaction-start-observed", count: compactionStarts });
    await inject(probes, injectionsFor(context.scenarioId, "during"), context);
    await waitFor(history.call, "history summary provider call");
    history.release();
    await waitFor(turn.call, "turn summary provider call");
    turn.release();
    await waitFor(compactionEnded, "compaction_end");
    if (context.scenarioId === "subagent-before-during-after") await inject(probes, [{ consumer: "pi-subagents", kind: "completion", id: "subagent-after-success", outcome: "success" }], context);
    await waitFor(resumed.call, "resume provider call");
    resumed.release();
    await waitFor(prompt, "scenario prompt settlement");
    await waitFor(twiceSettled, "resume settlement");
    context.timeline.record({ type: "resume-settlement-observed", count: settledCount });
    if (compactionStarts !== 1 || compactionEnds !== 1) throw new Error(`expected exactly one managed compaction, got starts=${compactionStarts} ends=${compactionEnds}`);
    if (maxActiveRuns > 1 || activeRuns !== 0) throw new Error("candidate scenario observed overlapping or unsettled model runs");
    if (sessionManager.getEntries().filter((entry) => entry.type === "compaction").length !== 1) throw new Error("candidate scenario did not persist exactly one compaction entry");
    for (const probe of probes) {
      const observations = await probe.observations();
      context.timeline.record({ type: "consumer-observations", consumerId: probe.consumer, count: observations.length });
    }
  } finally {
    unsubscribe();
    await disposeProbes(probes);
    session.dispose();
  }
}

/** A bounded soak cycle uses the same packaged public-SDK managed race. */
export async function runExactCandidateSoakCycle(context: ExactCandidateScenarioContext, cycle: number): Promise<void> {
  const cycleContext: ExactCandidateScenarioContext = { ...context, scenarioId: `soak-${cycle}` };
  await executeExactCandidateScenario(cycleContext);
}
