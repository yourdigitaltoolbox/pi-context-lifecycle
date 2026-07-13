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
import { getContextLifecycleSnapshotV1 } from "../registry.js";
import { CONTEXT_LIFECYCLE_RELEASE_LANES, type LifecycleLane } from "../types.js";
import { createDeferredFakeProvider, type DeferredProviderCall, type DeferredResponseSequence } from "./deferred-provider.js";
import { loadExactCandidateProbe, type ExactCandidateConsumer, type ExactCandidateProbe, type ExactCandidateProbeInjection, type ExactCandidateProbeReceipt } from "./exact-candidate-probe.js";
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

interface ExpectedProducerBatch {
  readonly laneId: LifecycleLane;
  readonly customType: string;
}

export interface ProductionReleaseLaneExpectation {
  readonly consumer: ExactCandidateConsumer;
  readonly id: string;
  readonly laneId: LifecycleLane;
}

const LANE_ORDER = new Map<LifecycleLane, number>(CONTEXT_LIFECYCLE_RELEASE_LANES.map((laneId, index) => [laneId, index]));

function receiptLaneId(receipt: Readonly<ExactCandidateProbeReceipt>): LifecycleLane | undefined {
  return receipt.laneId ?? receipt.lane;
}

function expectedReceiptLane(input: ExactCandidateProbeInjection): LifecycleLane | undefined {
  if (input.consumer === "pi-subagents") return input.outcome === "failure" ? "failure-attention-decision" : "subagent-success";
  if (input.consumer === "remote-pi" && input.kind === "mesh-arrival") return input.lane === "reply" ? "mesh-reply" : "mesh-unsolicited";
  return undefined;
}

/**
 * Verifies lane ordering from the consumer's redacted production release
 * receipts. Custom-message types are deliberately absent: reply/unsolicited
 * mesh and failure/success subagent batches share a type and cannot prove this.
 */
export function assertProductionReleaseLaneOrder(
  observations: readonly Readonly<ExactCandidateProbeReceipt>[],
  expected: readonly ProductionReleaseLaneExpectation[],
): readonly Readonly<ExactCandidateProbeReceipt>[] {
  const orderedEvidence: Readonly<ExactCandidateProbeReceipt>[] = [];
  for (const consumer of consumerPackageNames) {
    const expectedForConsumer = expected
      .filter((entry) => entry.consumer === consumer)
      .sort((left, right) => (LANE_ORDER.get(left.laneId) ?? Number.MAX_SAFE_INTEGER) - (LANE_ORDER.get(right.laneId) ?? Number.MAX_SAFE_INTEGER));
    if (expectedForConsumer.length === 0) continue;
    const expectedIds = new Set(expectedForConsumer.map((entry) => entry.id));
    const released = observations.filter((entry) => entry.consumer === consumer && entry.outcome === "released" && expectedIds.has(entry.id));
    if (released.length !== expectedForConsumer.length) {
      throw new Error(`${consumer} produced ${released.length} redacted release receipts; expected ${expectedForConsumer.length}`);
    }
    // pi-subagents acknowledges its real public send asynchronously. Its
    // receipt's production dispatch sequence, rather than Promise-settlement
    // arrival order, is the redacted ordering witness for shared custom types.
    const orderedReleased = consumer === "pi-subagents"
      ? released.map((receipt) => {
        const dispatchSequence = receipt.dispatchSequence;
        if (typeof dispatchSequence !== "number" || !Number.isSafeInteger(dispatchSequence) || dispatchSequence < 0) {
          throw new Error(`${consumer} redacted release receipt lacks a dispatchSequence`);
        }
        return receipt;
      }).sort((left, right) => (left.dispatchSequence ?? -1) - (right.dispatchSequence ?? -1))
      : released;
    for (const [sequence, expectedReceipt] of expectedForConsumer.entries()) {
      const actual = orderedReleased[sequence];
      const actualLaneId = actual === undefined ? undefined : receiptLaneId(actual);
      if (actualLaneId !== expectedReceipt.laneId) {
        throw new Error(`${consumer} redacted release receipt ${sequence} expected lane ${expectedReceipt.laneId}, got ${actualLaneId ?? "none"}`);
      }
    }
    orderedEvidence.push(...orderedReleased);
  }
  return Object.freeze(orderedEvidence);
}

const ALL_PRODUCER_BATCHES: readonly ExpectedProducerBatch[] = [
  { laneId: "mesh-reply", customType: "remote-pi:mesh-batch" },
  { laneId: "subagent-success", customType: "subagent-notify" },
  { laneId: "background-notify", customType: "background-task-completion" },
  { laneId: "loop-tick", customType: "loop-timer-tick" },
  { laneId: "cron-tick", customType: "cron-timer-tick" },
];

function expectedProducerBatches(scenarioId: string): readonly ExpectedProducerBatch[] | undefined {
  switch (scenarioId) {
    case "all-producers-concurrent": return ALL_PRODUCER_BATCHES;
    // The two success completions share one production subagent-success batch,
    // while failure is emitted through its earlier lifecycle lane.
    case "subagent-before-during-after": return [
      { laneId: "failure-attention-decision", customType: "subagent-notify" },
      { laneId: "subagent-success", customType: "subagent-notify" },
    ];
    // MeshSpool releases reply before unsolicited traffic, each through its
    // production follow-up submission boundary.
    case "mesh-during-compaction": return [
      { laneId: "mesh-reply", customType: "remote-pi:mesh-batch" },
      { laneId: "mesh-unsolicited", customType: "remote-pi:mesh-batch" },
    ];
    // The loop and watchdog share the bounded loop-tick batch. Lifecycle lane
    // order keeps the completion, loop, and cron submits deterministic.
    case "background-due-during-compaction": return [
      { laneId: "background-notify", customType: "background-task-completion" },
      { laneId: "loop-tick", customType: "loop-timer-tick" },
      { laneId: "cron-tick", customType: "cron-timer-tick" },
    ];
    // Failure/attention has priority over successful subagent completion.
    case "large-context-reduction": return [
      { laneId: "failure-attention-decision", customType: "subagent-notify" },
      { laneId: "subagent-success", customType: "subagent-notify" },
      { laneId: "background-notify", customType: "background-task-completion" },
    ];
    default: return undefined;
  }
}

function assertAllProducerBatches(submissions: readonly Readonly<{ customType: string; sequence: number }>[], context: ExactCandidateScenarioContext): void {
  assertExpectedProducerBatches(submissions, ALL_PRODUCER_BATCHES, context);
}

function assertExpectedProducerBatches(submissions: readonly Readonly<{ customType: string; sequence: number }>[], batches: readonly ExpectedProducerBatch[], context: ExactCandidateScenarioContext): void {
  if (submissions.length !== batches.length) {
    throw new Error(`${context.scenarioId} submitted ${submissions.length} producer batches; expected ${batches.length}`);
  }
  for (const [sequence, batch] of batches.entries()) {
    const actual = submissions[sequence];
    if (actual?.customType !== batch.customType || actual.sequence !== sequence) {
      throw new Error(`${context.scenarioId} lane ${batch.laneId} expected ${batch.customType} at sequence ${sequence}, got ${actual?.customType ?? "none"} at ${actual?.sequence ?? -1}`);
    }
    context.timeline.record({ type: context.scenarioId === "all-producers-concurrent" ? "all-producers-batch-submitted" : "producer-batch-submitted", outcome: batch.laneId, count: 1, sequence });
  }
}

function isOrdinaryProviderWork(label: DeferredProviderCall["label"]): boolean {
  return label === "agent-initial" || label === "agent-post-tool" || label === "producer-drain" || label === "pre-compaction-producer";
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
  receipt: Readonly<ExactCandidateProbeReceipt>;
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
    // `agent_settled` is observed synchronously by the test subscriber, while
    // the archive lifecycle extension starts its documented asynchronous
    // drainer release from that same event. Wait for the probe's real release
    // receipt rather than treating the subscriber observation as completion.
    const deadline = Date.now() + 10_000;
    let observations = await probe.observations();
    let heldAt = observations.findIndex((entry) => entry.id === injected.receipt.id && entry.outcome === "held");
    let releasedAt = observations.findIndex((entry, index) => index > heldAt && entry.id === injected.receipt.id && entry.outcome === "released");
    while ((heldAt < 0 || releasedAt < 0) && Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      observations = await probe.observations();
      heldAt = observations.findIndex((entry) => entry.id === injected.receipt.id && entry.outcome === "held");
      releasedAt = observations.findIndex((entry, index) => index > heldAt && entry.id === injected.receipt.id && entry.outcome === "released");
    }
    if (heldAt < 0 || releasedAt < 0) {
      throw new Error(`archive probe did not prove held/released disposition for ${injected.receipt.consumer}:${injected.receipt.id}`);
    }
    context.timeline.record({ type: "consumer-release-observed", consumerId: injected.receipt.consumer, outcome: "released", sequence: releasedAt });
  }
  const expectedLaneReceipts = receipts.flatMap((injected): readonly ProductionReleaseLaneExpectation[] => {
    const laneId = expectedReceiptLane(injected.input);
    return laneId === undefined ? [] : [{ consumer: injected.receipt.consumer, id: injected.receipt.id, laneId }];
  });
  const allObservations = (await Promise.all(probes.map(async (probe) => probe.observations()))).flat();
  for (const receipt of allObservations) {
    if (receipt.outcome !== "released" || !expectedLaneReceipts.some((expected) => expected.consumer === receipt.consumer && expected.id === receipt.id)) continue;
    context.timeline.record({ type: "consumer-release-receipt-observed", consumerId: receipt.consumer, outcome: receiptLaneId(receipt) ?? "lane-missing", ...(receipt.dispatchSequence === undefined ? {} : { sequence: receipt.dispatchSequence }) });
  }
  const orderedEvidence = assertProductionReleaseLaneOrder(allObservations, expectedLaneReceipts);
  for (const receipt of orderedEvidence) {
    const laneId = receiptLaneId(receipt);
    if (laneId === undefined) throw new Error(`${receipt.consumer} ordered release evidence lacks a lane`);
    context.timeline.record({ type: "consumer-release-lane-observed", consumerId: receipt.consumer, outcome: laneId, ...(receipt.dispatchSequence === undefined ? {} : { sequence: receipt.dispatchSequence }) });
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
  const producerBatches = expectedProducerBatches(context.scenarioId);
  const traceProducerReleases = producerBatches !== undefined;
  const traceAllProducers = context.scenarioId === "all-producers-concurrent";
  let compactionOpen = false;
  let resumeReleaseBarrier = false;
  let providerInvariantError: Error | undefined;
  let allProducersPhase = "initial";
  const recordAllProducersPhase = (phase: string, settledCount = 0): void => {
    if (!traceAllProducers) return;
    allProducersPhase = phase;
    context.timeline.record({ type: "all-producers-phase", outcome: phase, count: provider.callCount, sequence: settledCount });
  };
  const providerFailure = context.scenarioId === "provider-failure";
  const cancellation = context.scenarioId === "compaction-cancelled";
  const toolCalls = context.scenarioId === "tool-multi-tool"
    ? [
      fauxToolCall("self_compact", { instructions: "first managed request" }, { id: "self-compact-first" }),
      fauxToolCall("self_compact", { instructions: "joined managed request" }, { id: "self-compact-second" }),
    ]
    : [fauxToolCall("self_compact", { instructions: "reload durable state" }, { id: `self-compact-${context.scenarioId}` })];
  // The production pi-subagents completion delivery starts a real parent turn.
  // Queue its disposable response first, so the accepted before-compaction
  // injection cannot consume the self_compact response below.
  const preCompactionProducer = context.scenarioId === "subagent-before-during-after"
    ? provider.enqueue(fauxAssistantMessage("Settling pre-compaction producer response."), { label: "pre-compaction-producer" })
    : undefined;
  const first = provider.enqueue(fauxAssistantMessage(toolCalls, { stopReason: "toolUse" }), { label: "agent-initial" });
  const second = provider.enqueue(fauxAssistantMessage("Settling managed request."), { label: "agent-post-tool" });
  const history = providerFailure
    ? provider.enqueueFailure({ label: "compaction-history" })
    : provider.enqueue(fauxAssistantMessage("History summary."), { label: "compaction-history" });
  const turn = provider.enqueue(fauxAssistantMessage("Turn summary."), { label: "compaction-turn" });
  const resumed = provider.enqueue(fauxAssistantMessage("Resumed after managed compaction."), { label: "self-resume" });
  // This repeating deferred response is deliberately bound to actual provider
  // entries. It avoids the invalid ingress=count(model-turn) assumption while
  // keeping every observed producer request held until the driver releases it.
  const producerDrains: DeferredResponseSequence | undefined = traceProducerReleases
    ? provider.enqueueProducerDrain(fauxAssistantMessage("Settling producer drain."))
    : undefined;
  const unsubscribeProvider = provider.onEntry((entry) => {
    if (traceProducerReleases) context.timeline.record({ type: traceAllProducers ? "all-producers-provider-entered" : "producer-provider-entered", outcome: entry.label, count: provider.tracker.inFlight, sequence: entry.callCount });
    if (compactionOpen && isOrdinaryProviderWork(entry.label)) providerInvariantError ??= new Error(`ordinary provider work entered during compaction: ${entry.label}`);
    if ((entry.label === "compaction-history" || entry.label === "compaction-turn") && !compactionOpen) providerInvariantError ??= new Error(`compaction summary entered outside compaction: ${entry.label}`);
    if (entry.label === "self-resume" && compactionOpen) providerInvariantError ??= new Error("self-resume provider work entered before compaction terminal");
    if (entry.label === "producer-drain" && !resumeReleaseBarrier) providerInvariantError ??= new Error("producer provider work entered before the resume settlement/release barrier");
  });
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
  let agentStartTotal = 0;
  let agentSettledTotal = 0;
  let compactionStarts = 0;
  let compactionEnds = 0;
  let settledCount = 0;
  let toolExecutionStarts = 0;
  let toolExecutionEnds = 0;
  let toolResultStarts = 0;
  const producerCustomTypes = new Set(ALL_PRODUCER_BATCHES.map((batch) => batch.customType));
  const actualProducerSubmissions: Array<Readonly<{ customType: string; sequence: number }>> = [];
  let queuedSteering = 0;
  let queuedFollowUp = 0;
  let resolveTwiceSettled!: () => void;
  let resolveStart!: () => void;
  let resolveEnd!: () => void;
  let resolveMultiToolResults!: () => void;
  const compactionStarted = new Promise<void>((resolve) => { resolveStart = resolve; });
  const compactionEnded = new Promise<void>((resolve) => { resolveEnd = resolve; });
  const twiceSettled = new Promise<void>((resolve) => { resolveTwiceSettled = resolve; });
  const multiToolResults = new Promise<void>((resolve) => { resolveMultiToolResults = resolve; });
  const unsubscribe = session.subscribe((event: AgentSessionEvent) => {
    if (event.type === "agent_start") {
      agentStartTotal += 1;
      if (traceAllProducers) context.timeline.record({ type: "all-producers-agent-start", outcome: allProducersPhase, count: agentStartTotal, sequence: provider.callCount });
    }
    if (event.type === "queue_update") {
      queuedSteering = event.steering.length;
      queuedFollowUp = event.followUp.length;
    }
    if (event.type === "agent_settled") {
      agentSettledTotal += 1;
      settledCount += 1;
      if (traceAllProducers) context.timeline.record({ type: "all-producers-agent-settled", outcome: allProducersPhase, count: agentSettledTotal, sequence: provider.callCount });
      if (settledCount === 2) resolveTwiceSettled();
    }
    if (event.type === "compaction_start") {
      compactionStarts += 1;
      compactionOpen = true;
      recordAllProducersPhase("compaction", settledCount);
      resolveStart();
    }
    if (event.type === "tool_execution_start") toolExecutionStarts += 1;
    if (event.type === "tool_execution_end") toolExecutionEnds += 1;
    if (event.type === "message_start" && event.message.role === "toolResult") toolResultStarts += 1;
    if (context.scenarioId === "tool-multi-tool" && toolExecutionStarts >= 2 && toolExecutionEnds >= 2 && toolResultStarts >= 2) {
      resolveMultiToolResults();
    }
    if (traceProducerReleases && event.type === "message_start" && event.message.role === "custom" && producerCustomTypes.has(event.message.customType)) {
      const submission = Object.freeze({ customType: event.message.customType, sequence: actualProducerSubmissions.length });
      actualProducerSubmissions.push(submission);
      context.timeline.record({ type: traceAllProducers ? "all-producers-custom-message-submitted" : "producer-custom-message-submitted", outcome: submission.customType, count: actualProducerSubmissions.length, sequence: submission.sequence });
    }
    if (event.type === "compaction_end") {
      compactionEnds += 1;
      compactionOpen = false;
      context.timeline.record({ type: "compaction-terminal", outcome: event.aborted ? "cancelled" : event.errorMessage === undefined ? "completed" : "failed" });
      recordAllProducersPhase("resume", settledCount);
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
    const beforeInjection = context.scenarioId === "subagent-before-during-after"
      ? inject(probes, [{ consumer: "pi-subagents", kind: "completion", id: "subagent-before-success", outcome: "success" }], context)
      : undefined;
    if (preCompactionProducer !== undefined) {
      const preCompactionCall = await waitFor(preCompactionProducer.call, "pre-compaction producer provider call");
      context.timeline.record({ type: "pre-compaction-producer-entered", consumerId: "pi-subagents", count: preCompactionCall.callCount });
      preCompactionProducer.release();
      await waitFor(preCompactionProducer.completed, "pre-compaction producer terminal settlement");
      await session.waitForIdle();
      const preCompactionCalls = provider.tracker.calls();
      const snapshot = getContextLifecycleSnapshotV1();
      if (preCompactionCalls.length !== 1 || preCompactionCalls[0]?.label !== "pre-compaction-producer" || provider.tracker.inFlight !== 0 || provider.tracker.maxInFlight !== 1 || !session.isIdle || snapshot.phase !== "idle" || queuedSteering !== 0 || queuedFollowUp !== 0 || agentStartTotal !== agentSettledTotal || compactionStarts !== 0 || compactionEnds !== 0) {
        throw new Error(`pre-compaction producer did not settle to an idle public boundary: calls=${preCompactionCalls.length} label=${preCompactionCalls[0]?.label ?? "none"} inFlight=${provider.tracker.inFlight} maxInFlight=${provider.tracker.maxInFlight} sessionIdle=${session.isIdle} phase=${snapshot.phase ?? "none"} steering=${queuedSteering} followUp=${queuedFollowUp} starts=${agentStartTotal} settlements=${agentSettledTotal} compactionStarts=${compactionStarts} compactionEnds=${compactionEnds}`);
      }
      provider.tracker.assertNoOverlap();
      // This accepted pre-compaction notification establishes the ordinary
      // parent-turn boundary only. Post-resume batch assertions must count
      // solely the two held-during and one held-after completions below.
      actualProducerSubmissions.length = 0;
      context.timeline.record({ type: "pre-compaction-producer-settled", consumerId: "pi-subagents", count: provider.tracker.completed });
    }
    const beforeInjections = beforeInjection === undefined ? [] : await beforeInjection;
    for (const receipt of beforeInjections) {
      if (receipt.receipt.outcome !== "accepted") throw new Error(`expected pre-compaction ${receipt.receipt.consumer}:${receipt.receipt.id} to be accepted, got ${receipt.receipt.outcome}`);
    }
    const prompt = session.prompt(`Run exact candidate scenario ${context.scenarioId}.`);
    await waitFor(first.call, "initial tool provider call");
    first.release();
    // A two-tool assistant response remains an active provider request until
    // its returned public event stream reaches terminal settlement. Do not
    // release the next response merely because Pi has entered another tool
    // transition: that would mask an overlapping stream lifetime.
    if (context.scenarioId === "tool-multi-tool") {
      await waitFor(first.completed, "two-tool initial response terminal settlement");
      await waitFor(multiToolResults, "both multi-tool result settlements");
      if (toolExecutionStarts !== 2 || toolExecutionEnds !== 2 || toolResultStarts !== 2) {
        throw new Error(`expected both multi-tool executions/results before post-tool release, got starts=${toolExecutionStarts} ends=${toolExecutionEnds} results=${toolResultStarts}`);
      }
    }
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
        recordAllProducersPhase("release", settledCount);
        if (context.scenarioId === "large-context-reduction") {
          const beforeBytes = JSON.stringify(historyCall.context.messages).length;
          const afterBytes = JSON.stringify((await resumed.call).context.messages).length;
          context.timeline.record({ type: "context-bytes-before", count: beforeBytes });
          context.timeline.record({ type: "context-bytes-after", count: afterBytes });
          if (afterBytes >= beforeBytes) throw new Error(`managed compaction did not reduce measured context bytes: before=${beforeBytes}, after=${afterBytes}`);
        }
        resumeReleaseBarrier = true;
        if (producerDrains !== undefined && producerBatches !== undefined) {
          const firstProducer = await producerDrains.responseAt(0);
          const releases = requireReleasedInOrder(probes, [...heldInjections, ...afterInjections], context);
          await waitFor(firstProducer.call, "first actual producer provider call");
          if (providerInvariantError !== undefined || provider.tracker.maxInFlight !== 1 || provider.tracker.calls().filter((entry) => entry.label === "producer-drain").length !== 1) {
            throw providerInvariantError ?? new Error("a producer submission entered a second provider request while the first was held");
          }
          provider.tracker.assertNoOverlap();
          let producerIndex = 0;
          const drainDeadline = Date.now() + 10_000;
          if (traceAllProducers) {
            // The all-producer finite-cut proof additionally establishes that
            // all release receipts can arrive while the first real producer
            // request stays held.
            await releases;
            provider.tracker.assertNoOverlap();
            for (;;) {
              const producerDrain = await producerDrains.responseAt(producerIndex);
              await waitFor(producerDrain.call, `producer drain ${producerIndex} provider call`);
              producerDrain.release();
              await waitFor(producerDrain.completed, `producer drain ${producerIndex} completion`);
              const next = await producerDrains.responseAt(producerIndex + 1);
              let nextEntered = false;
              while (!nextEntered && Date.now() < drainDeadline) {
                nextEntered = await Promise.race([
                  next.call.then(() => true),
                  new Promise<false>((resolve) => setTimeout(resolve, actualProducerSubmissions.length === producerBatches.length ? 50 : 10)),
                ]);
                if (actualProducerSubmissions.length === producerBatches.length && !nextEntered) break;
              }
              if (nextEntered) {
                producerIndex += 1;
                continue;
              }
              if (actualProducerSubmissions.length !== producerBatches.length) {
                throw new Error(`${context.scenarioId} did not expose all expected producer batches before quiescence: observed=${actualProducerSubmissions.length} expected=${producerBatches.length}`);
              }
              break;
            }
          } else {
            // These real adapters can await the preceding follow-up turn
            // before they acknowledge the next release. Settle each observed
            // public stream in order; do not hold an earlier response and
            // deadlock the later production drainer.
            const releaseStates = new Set<"completed" | "failed">();
            // Observe rejection now so a failing archive receipt cannot become
            // an unhandled rejection while the bounded stream driver waits.
            void releases.then(() => { releaseStates.add("completed"); }, () => { releaseStates.add("failed"); });
            drainReleasedStreams: for (;;) {
              const producerDrain = await producerDrains.responseAt(producerIndex);
              await waitFor(producerDrain.call, `producer drain ${producerIndex} provider call`);
              producerDrain.release();
              await waitFor(producerDrain.completed, `producer drain ${producerIndex} completion`);
              const next = await producerDrains.responseAt(producerIndex + 1);
              for (;;) {
                if (Date.now() >= drainDeadline) {
                  throw new Error(`${context.scenarioId} did not expose all expected producer batches/releases before quiescence: observed=${actualProducerSubmissions.length} expected=${producerBatches.length} releaseState=${[...releaseStates].join(",") || "pending"}`);
                }
                const nextEntered = await Promise.race([
                  next.call.then(() => true),
                  new Promise<false>((resolve) => setTimeout(resolve, releaseStates.has("completed") && actualProducerSubmissions.length === producerBatches.length ? 50 : 10)),
                ]);
                if (nextEntered) {
                  producerIndex += 1;
                  continue drainReleasedStreams;
                }
                if (releaseStates.has("failed")) await releases;
                if (releaseStates.has("completed") && actualProducerSubmissions.length === producerBatches.length) {
                  await releases;
                  break drainReleasedStreams;
                }
              }
            }
          }
          await waitFor(session.waitForIdle(), `${context.scenarioId} final release quiescence`);
          await new Promise<void>((resolve) => setImmediate(resolve));
          await new Promise<void>((resolve) => setImmediate(resolve));
          if (traceAllProducers) assertAllProducerBatches(actualProducerSubmissions, context);
          else assertExpectedProducerBatches(actualProducerSubmissions, producerBatches, context);
          const snapshot = getContextLifecycleSnapshotV1();
          if (!session.isIdle || snapshot.phase !== "idle" || queuedSteering !== 0 || queuedFollowUp !== 0 || agentStartTotal !== agentSettledTotal) {
            throw new Error(`${context.scenarioId} did not reach final lifecycle/session quiescence: sessionIdle=${session.isIdle} phase=${snapshot.phase ?? "none"} steering=${queuedSteering} followUp=${queuedFollowUp} starts=${agentStartTotal} settlements=${agentSettledTotal}`);
          }
          context.timeline.record({ type: traceAllProducers ? "all-producers-final-quiescence" : "producer-final-quiescence", count: provider.tracker.completed, sequence: agentSettledTotal });
        } else {
          await requireReleasedInOrder(probes, [...heldInjections, ...afterInjections], context);
        }
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
    if (providerInvariantError !== undefined) throw providerInvariantError;
    if (agentStartTotal !== agentSettledTotal) throw new Error(`candidate scenario has unbalanced public agent events: starts=${agentStartTotal} settlements=${agentSettledTotal}`);
    if (provider.tracker.maxInFlight > 1 || provider.tracker.inFlight !== 0 || provider.tracker.entered !== provider.tracker.completed || provider.callCount !== provider.tracker.entered) {
      throw new Error(`candidate scenario provider work was overlapping, incomplete, or unclassified: calls=${provider.callCount} entered=${provider.tracker.entered} completed=${provider.tracker.completed} inFlight=${provider.tracker.inFlight} max=${provider.tracker.maxInFlight}`);
    }
    provider.tracker.assertNoOverlap();
    if (sessionManager.getEntries().filter((entry) => entry.type === "compaction").length !== (providerFailure || cancellation ? 0 : 1)) throw new Error("candidate scenario persisted an unexpected compaction entry count");
    for (const probe of probes) {
      const observations = await probe.observations();
      context.timeline.record({ type: "consumer-observations", consumerId: probe.consumer, count: observations.length });
    }
  } finally {
    unsubscribe();
    unsubscribeProvider();
    await disposeScenarioRuntime(runtime, probes, () => sessionShutdownCount, context.scenarioId === "reload-replacement-repair" ? 2 : 1);
  }
}

/** A bounded soak cycle uses the same packaged public-SDK managed race. */
export async function runExactCandidateSoakCycle(context: ExactCandidateScenarioContext, cycle: number): Promise<void> {
  const cycleContext: ExactCandidateScenarioContext = { ...context, scenarioId: `soak-${cycle}` };
  await executeExactCandidateScenario(cycleContext);
}
