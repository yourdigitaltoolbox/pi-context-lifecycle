import { randomUUID } from "node:crypto";
import { CONTEXT_LIFECYCLE_RELEASE_LANES } from "./types.js";
import type {
  CompactDisposition,
  CompactRequest,
  CompactionReason,
  CoordinatorPublisherV1,
  DiagnosticRecord,
  DrainAck,
  DrainerRegistration,
  LifecycleClaim,
  LifecycleClaimState,
  LifecycleLane,
  OperationOutcome,
  Phase,
  ReleasePermit,
  RepairDisposition,
  RepairRequest,
  ReleaseWatermark,
  Snapshot,
  WakeAdmission,
  WakeDisposition,
} from "./types.js";
import type { CoordinatorPublicationV1 } from "./registry.js";

const MAX_DIAGNOSTICS = 100;
const COMPACTION_WARNING_MS = 2 * 60 * 1000;
const COMPACTION_BLOCK_MS = 10 * 60 * 1000;
const RESUME_WARNING_MS = 30 * 1000;
const RESUME_BLOCK_MS = 60 * 1000;
const DRAINER_BLOCK_MS = 5 * 1000;
const LANE_ORDER = new Map<LifecycleLane, number>(CONTEXT_LIFECYCLE_RELEASE_LANES.map((laneId, index) => [laneId, index]));

function drainerKey(consumerId: string, laneId: LifecycleLane): string {
  return `${consumerId}\u0000${laneId}`;
}

function isLifecycleLane(value: unknown): value is LifecycleLane {
  return typeof value === "string" && (CONTEXT_LIFECYCLE_RELEASE_LANES as readonly string[]).includes(value);
}

function isCancellationError(error: Error): boolean {
  const code = (error as Error & { code?: unknown }).code;
  return error.name === "AbortError" || code === "ABORT_ERR" || /\b(?:abort(?:ed)?|cancelled|canceled)\b/i.test(error.message);
}

export interface ManagedCompactionAdapter {
  compact(options: { customInstructions: string; onComplete(): void; onError(error: Error): void }): void;
  sendResume(message: string): void;
  /** Public Pi context guards; never settlement authority by themselves. */
  isIdle?(): boolean;
  hasPendingMessages?(): boolean;
  appendLifecycleEntry?(claim: LifecycleClaim): void;
  verifyRepairEvidence?(request: RepairRequest): boolean;
}

interface SettlementProof {
  generationId: string;
  source: "session-start" | "agent-settled";
  epoch: number;
}

interface ActiveOperation {
  id: string;
  reason: CompactionReason;
  managed: boolean;
  originOwnerInstanceId: string;
  claimState: LifecycleClaimState;
  startedAt: number;
  resume: boolean;
  customInstructions: string;
  resumeMessage: string;
  compactStarted: boolean;
  matchingManagedSuccessEvents: number;
  managedCompleteObserved: boolean;
  /** Native threshold/overflow compaction adopted before this request started its own compact call. */
  adoptedAutomaticReason?: "threshold" | "overflow";
  resumeMessageMatched: boolean;
}

interface RegisteredDrainer extends DrainerRegistration {
  token: symbol;
}

interface CapturedDrainer {
  registration: RegisteredDrainer;
  cut: ReleaseWatermark;
}

export const DEFAULT_COMPACTION_INSTRUCTIONS = [
  "Preserve the current task, active plan/workspace/status file paths, modified files, commits, validation results, blockers, and next requested action.",
  "Do not over-summarize source custody, review URLs, branch/workspace ids, or explicit user constraints.",
  "Keep enough detail for the next turn to reload durable context from files before continuing.",
].join(" ");

export const DEFAULT_RESUME_MESSAGE = [
  "Continue after /self_compact.",
  "First reload durable context from the relevant project instructions, active plan/status files, and workspace status before taking the next action.",
  "If the active plan/workspace is ambiguous, ask before mutating runtime state.",
].join(" ");

function withFocus(base: string, label: string, focus: string): string {
  const trimmed = focus.trim();
  return trimmed.length === 0 ? base : `${base}\n\n${label}: ${trimmed}`;
}

function markedResumeMessage(message: string, operationId: string, generationId: string): string {
  return `${message}\n\n<!-- pi-context-lifecycle:v1 resume operationId=${operationId} generationId=${generationId} -->`;
}

function exactTextContent(content: unknown): string | undefined {
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

export class ContextLifecycleCoordinatorV1 implements CoordinatorPublisherV1 {
  readonly protocolVersion = 1 as const;
  readonly ownerInstanceId: string;
  private publication: CoordinatorPublicationV1 | undefined;
  private sessionId: string | undefined;
  private generationId: string | undefined;
  private phase: Phase | undefined;
  private operation: ActiveOperation | undefined;
  private lastOutcome: OperationOutcome | undefined;
  private blockedReason: string | undefined;
  private adapter: ManagedCompactionAdapter | undefined;
  private settlementProof: SettlementProof | undefined;
  private settlementEpoch = 0;
  private disposed = false;
  private diagnosticSequence = 0;
  private readonly records: DiagnosticRecord[] = [];
  private readonly drainers = new Map<string, RegisteredDrainer>();
  private readonly activePermits = new Set<ReleasePermit>();
  private releaseCut: CapturedDrainer[] | undefined;
  private releaseIndex = 0;
  /** Settlement epoch that must be exceeded before the next submitted cut drains. */
  private releaseAwaitingSettlementEpoch: number | undefined;
  private blockedDrainerConsumerId: string | undefined;
  private blockedDrainerLaneId: LifecycleLane | undefined;
  private compactionWarningTimer: ReturnType<typeof setTimeout> | undefined;
  private compactionBlockTimer: ReturnType<typeof setTimeout> | undefined;
  private resumeWarningTimer: ReturnType<typeof setTimeout> | undefined;
  private resumeBlockTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(ownerInstanceId: string = randomUUID()) {
    this.ownerInstanceId = ownerInstanceId;
  }

  attachPublication(publication: CoordinatorPublicationV1): void {
    if (this.publication !== undefined) throw new Error("Coordinator publication already attached");
    if (publication.ownerInstanceId !== this.ownerInstanceId) throw new Error("Coordinator owner identity mismatch");
    this.publication = publication;
  }

  bindSession(sessionId: string, adapter: ManagedCompactionAdapter): string {
    if (this.disposed) throw new Error("Coordinator is disposed");
    if (this.sessionId !== undefined) throw new Error("Coordinator session already bound");
    this.sessionId = sessionId;
    this.generationId = randomUUID();
    this.adapter = adapter;
    this.phase = "idle";
    this.transition("session-bound");
    this.observeSettlement("session-start");
    return this.generationId;
  }

  restoreClaims(claims: readonly LifecycleClaim[]): void {
    if (this.disposed || !this.sessionId || !this.generationId || this.phase !== "idle" || this.operation !== undefined) throw new Error("Coordinator cannot restore claims in the current state");
    const latest = [...claims].reverse().find((claim) => claim.sessionId === this.sessionId);
    if (latest === undefined || latest.state === "released" || latest.state === "failed" || latest.state === "cancelled") return;
    this.operation = {
      id: latest.operationId,
      reason: latest.reason,
      managed: latest.reason === "self" || latest.reason === "remote",
      originOwnerInstanceId: latest.originOwnerInstanceId,
      claimState: latest.state,
      startedAt: latest.timestamp,
      resume: latest.resumeIntent,
      customInstructions: "",
      resumeMessage: markedResumeMessage(DEFAULT_RESUME_MESSAGE, latest.operationId, this.generationId),
      compactStarted: latest.state !== "requested",
      matchingManagedSuccessEvents: 0,
      managedCompleteObserved: latest.state !== "requested" && latest.state !== "compacting",
      resumeMessageMatched: latest.state === "resume-admitted" || latest.state === "resume-settled",
    };
    if (latest.state === "compacted" || latest.state.startsWith("resume-")) this.lastOutcome = "completed";
    this.blockedReason = `restored-${latest.state}`;
    this.phase = "blocked-unknown";
    this.transition("operation-restored-blocked");
  }

  requestSelfCompaction(focus: string, requestId: string): CompactDisposition {
    return this.request({ requestId, sessionId: this.sessionId ?? "", generationId: this.generationId ?? "", reason: "self", resume: true }, {
      customInstructions: withFocus(DEFAULT_COMPACTION_INSTRUCTIONS, "User focus for this compact/resume", focus),
      resumeMessage: withFocus(DEFAULT_RESUME_MESSAGE, "User focus", focus),
    });
  }

  requestSelfCompactionFromCommand(focus: string, requestId: string): CompactDisposition {
    const disposition = this.requestSelfCompaction(focus, requestId);
    if (disposition.disposition === "accepted") this.startCompaction();
    return disposition;
  }

  requestCompaction(request: CompactRequest): CompactDisposition {
    return this.request(request, {
      customInstructions: DEFAULT_COMPACTION_INSTRUCTIONS,
      resumeMessage: DEFAULT_RESUME_MESSAGE,
    });
  }

  private request(request: CompactRequest, content: { customInstructions: string; resumeMessage: string }): CompactDisposition {
    if (typeof request.generationId !== "string" || request.generationId.length === 0) return { disposition: "rejected", code: "generation-required" };
    if (this.disposed || !this.sessionId || !this.generationId || !this.phase) return { disposition: "rejected", code: "session-unavailable" };
    if (request.sessionId !== this.sessionId) return { disposition: "rejected", code: "session-mismatch", generationId: this.generationId };
    if (request.generationId !== this.generationId) return { disposition: "rejected", code: "generation-mismatch", generationId: this.generationId };
    const suppliedSettlementPolicy: unknown = request.settlementPolicy;
    if (suppliedSettlementPolicy !== undefined && suppliedSettlementPolicy !== "next-agent-settled" && suppliedSettlementPolicy !== "current-or-next-settled-boundary") return { disposition: "rejected", code: "invalid-start-authority", generationId: this.generationId };
    const settlementPolicy = suppliedSettlementPolicy ?? "next-agent-settled";
    const requestsCurrentBoundary = settlementPolicy === "current-or-next-settled-boundary";
    if (requestsCurrentBoundary && (request.reason !== "remote" || request.actor !== "operator" || request.channel !== "remote" || request.source !== "remote-pi-action")) {
      return { disposition: "rejected", code: "invalid-start-authority", generationId: this.generationId };
    }
    if (this.operation !== undefined) {
      if (!this.operation.managed) return { disposition: "rejected", code: "automatic-compaction-active", generationId: this.generationId };
      if (this.phase !== "pending-settle" && this.phase !== "compacting" && this.phase !== "resuming") return { disposition: "rejected", code: "operation-not-joinable", generationId: this.generationId };
      const addsResumeIntent = !this.operation.resume && (request.resume === true || request.reason === "self");
      if (request.resume === true || request.reason === "self") {
        this.operation.resume = true;
        if (this.phase !== "resuming") this.operation.resumeMessage = markedResumeMessage(content.resumeMessage, this.operation.id, this.generationId);
        if (!this.operation.compactStarted) this.operation.customInstructions = content.customInstructions;
      }
      if (addsResumeIntent && !this.persistClaim(this.operation.claimState)) {
        this.block("claim-persist-failed", false);
        return { disposition: "rejected", code: "claim-persist-failed", generationId: this.generationId };
      }
      this.record(addsResumeIntent ? "request-joined-resume-intent" : "request-joined");
      return { disposition: "joined", operationId: this.operation.id, generationId: this.generationId };
    }
    if (this.phase !== "idle") return { disposition: "rejected", code: "not-idle", generationId: this.generationId };
    let startAtCurrentBoundary = false;
    if (requestsCurrentBoundary) {
      let isIdle: boolean;
      let hasPendingMessages: boolean;
      try {
        isIdle = this.adapter?.isIdle?.() === true;
        hasPendingMessages = this.adapter?.hasPendingMessages?.() === true;
      } catch {
        this.record("settlement-proof-unavailable");
        return { disposition: "rejected", code: "settlement-proof-unavailable", generationId: this.generationId };
      }
      if (isIdle && !hasPendingMessages) {
        if (!this.hasCurrentSettlementProof()) {
          this.record("settlement-proof-unavailable");
          return { disposition: "rejected", code: "settlement-proof-unavailable", generationId: this.generationId };
        }
        startAtCurrentBoundary = true;
      }
    }
    const operationId = randomUUID();
    this.operation = {
      id: operationId,
      reason: request.reason,
      managed: true,
      originOwnerInstanceId: this.ownerInstanceId,
      claimState: "requested",
      startedAt: Date.now(),
      resume: request.resume === true || request.reason === "self",
      customInstructions: content.customInstructions,
      resumeMessage: markedResumeMessage(content.resumeMessage, operationId, this.generationId),
      compactStarted: false,
      matchingManagedSuccessEvents: 0,
      managedCompleteObserved: false,
      resumeMessageMatched: false,
    };
    this.phase = "pending-settle";
    if (!this.persistClaim("requested")) {
      this.block("claim-persist-failed", false);
      return { disposition: "rejected", code: "claim-persist-failed", generationId: this.generationId };
    }
    this.transition("request-accepted");
    if (startAtCurrentBoundary) {
      this.record("remote-idle-started");
      this.startCompaction();
    }
    return { disposition: "accepted", operationId: this.operation.id, generationId: this.generationId };
  }

  onAgentSettled(generationId: string): void {
    if (!this.isCurrentGeneration(generationId)) return;
    this.observeSettlement("agent-settled");
    if (!this.operation) return;
    if (this.phase === "pending-settle") {
      if (this.operation.adoptedAutomaticReason !== undefined && this.operation.managedCompleteObserved) {
        if (this.lastOutcome === "completed" && this.operation.resume) {
          this.phase = "resuming";
          if (!this.persistClaim("resume-pending") || !this.persistClaim("resume-admitting")) {
            this.block("claim-persist-failed", false);
            return;
          }
          this.transition("compaction-succeeded");
          this.scheduleResumeDeadlines(this.operation.id);
          this.adapter?.sendResume(this.operation.resumeMessage);
          this.record("resume-sent");
        } else {
          void this.release();
        }
        return;
      }
      this.startCompaction();
      return;
    }
    if (this.phase === "observed-preflight") {
      if (this.operation.managed && this.operation.adoptedAutomaticReason === undefined) return;
      this.lastOutcome = "failed";
      if (!this.persistClaim("failed")) {
        this.block("claim-persist-failed", false);
        return;
      }
      this.record(this.operation.managed ? "adopted-automatic-compaction-failed" : "automatic-compaction-failed");
      void this.release();
      return;
    }
    if (this.phase === "resuming" && this.operation.resumeMessageMatched) {
      if (!this.persistClaim("resume-settled")) {
        this.block("claim-persist-failed", false);
        return;
      }
      void this.release();
      return;
    }
    // A submitted drain starts exactly one Pi turn. Do not admit another
    // drainer (or return idle) until that same-generation turn settles.
    if (this.phase === "releasing"
      && this.releaseAwaitingSettlementEpoch !== undefined
      && this.settlementEpoch > this.releaseAwaitingSettlementEpoch) {
      this.releaseAwaitingSettlementEpoch = undefined;
      this.releaseIndex += 1;
      void this.release();
    }
  }

  /**
   * Activity invalidates remembered settlement before Pi can expose a new run.
   * It is intentionally conservative: only a later genuine settled boundary
   * may restore immediate-Remote eligibility.
   */
  onActivity(generationId: string): void {
    if (!this.isCurrentGeneration(generationId)) return;
    this.invalidateSettlementProof();
  }

  onSessionBeforeCompact(generationId: string, reason: "manual" | "threshold" | "overflow"): string | undefined {
    if (!this.isCurrentGeneration(generationId)) return undefined;
    this.invalidateSettlementProof();
    if (reason === "manual" && this.phase === "compacting" && this.operation?.managed) {
      this.record("managed-compaction-preflight-observed");
      return this.operation.id;
    }
    if (reason !== "manual"
      && this.phase === "pending-settle"
      && this.operation?.managed
      && !this.operation.compactStarted) {
      // Pi may need native threshold/overflow compaction before the managed
      // request reaches its settlement boundary. This preflight is observable
      // and the later session_compact/abort/settled events are authoritative,
      // so adopt it instead of wedging a deterministic overlap as ambiguous.
      this.operation.compactStarted = true;
      this.operation.adoptedAutomaticReason = reason;
      this.phase = "observed-preflight";
      if (!this.persistClaim("compacting")) {
        this.block("claim-persist-failed", false);
        return undefined;
      }
      this.transition("automatic-compaction-adopted-for-managed-request");
      this.scheduleCompactionDeadlines(this.operation.id);
      return this.operation.id;
    }
    if (this.phase !== "idle" || this.operation !== undefined) {
      this.block(reason === "manual" ? "builtin-compaction-overlapped-active-operation" : "automatic-compaction-overlapped-active-operation");
      return undefined;
    }
    const observedReason: CompactionReason = reason === "manual" ? "builtin" : reason;
    this.operation = {
      id: randomUUID(),
      reason: observedReason,
      managed: false,
      originOwnerInstanceId: this.ownerInstanceId,
      claimState: "compacting",
      startedAt: Date.now(),
      resume: false,
      customInstructions: "",
      resumeMessage: "",
      compactStarted: false,
      matchingManagedSuccessEvents: 0,
      managedCompleteObserved: false,
      resumeMessageMatched: false,
    };
    this.phase = "observed-preflight";
    if (!this.persistClaim("compacting")) {
      this.block("claim-persist-failed", false);
      return undefined;
    }
    this.transition(reason === "manual" ? "builtin-compaction-preflight-observed" : "automatic-compaction-preflight-observed");
    this.scheduleCompactionDeadlines(this.operation.id);
    return this.operation.id;
  }

  onCompactionCancelled(generationId: string, operationId: string): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation || this.operation.id !== operationId) return;
    if (this.phase !== "observed-preflight" && this.phase !== "compacting") return;
    this.clearCompactionDeadlines();
    this.lastOutcome = "cancelled";
    if (!this.persistClaim("cancelled")) {
      this.block("claim-persist-failed", false);
      return;
    }
    this.record(this.operation.managed ? "managed-compaction-cancelled" : "automatic-compaction-cancelled");
    if (this.operation.adoptedAutomaticReason !== undefined) {
      // Native compaction cancellation can fire while the enclosing run is
      // still active. Wait for its genuine settlement before draining wakes.
      this.operation.managedCompleteObserved = true;
      this.phase = "pending-settle";
      this.transition("adopted-automatic-cancellation-awaiting-settlement");
      return;
    }
    void this.release();
  }

  onSessionCompact(generationId: string, reason: "manual" | "threshold" | "overflow"): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation) return;
    if (reason !== "manual") {
      const resolvesDeadlineBlock = this.phase === "blocked-unknown" && this.blockedReason === "compaction-deadline";
      const adoptedManaged = this.operation.managed && this.operation.adoptedAutomaticReason === reason;
      if ((!adoptedManaged && (this.operation.managed || this.operation.reason !== reason))
        || (this.phase !== "observed-preflight" && !resolvesDeadlineBlock)) {
        this.block("automatic-compaction-event-mismatch");
        return;
      }
      this.clearCompactionDeadlines();
      this.lastOutcome = "completed";
      if (!this.persistClaim("compacted")) {
        this.block("claim-persist-failed", false);
        return;
      }
      if (!adoptedManaged) {
        this.record(resolvesDeadlineBlock ? "late-automatic-compaction-success" : "automatic-compaction-succeeded");
        void this.release(resolvesDeadlineBlock);
        return;
      }
      this.operation.managedCompleteObserved = true;
      this.record(resolvesDeadlineBlock ? "late-adopted-automatic-compaction-success" : "adopted-automatic-compaction-succeeded");
      // Native automatic compaction may finish inside the still-active agent
      // loop. Defer resume/release until that enclosing run genuinely settles.
      this.phase = "pending-settle";
      this.transition("adopted-automatic-compaction-awaiting-settlement");
      return;
    }
    const resolvesDeadlineBlock = this.phase === "blocked-unknown" && this.blockedReason === "compaction-deadline";
    if (!this.operation.managed && this.operation.reason === "builtin" && (this.phase === "observed-preflight" || resolvesDeadlineBlock)) {
      this.clearCompactionDeadlines();
      this.lastOutcome = "completed";
      if (!this.persistClaim("compacted")) {
        this.block("claim-persist-failed", false);
        return;
      }
      this.record(resolvesDeadlineBlock ? "late-builtin-compaction-success" : "builtin-compaction-succeeded");
      void this.release(resolvesDeadlineBlock);
      return;
    }
    if ((this.phase !== "compacting" && !resolvesDeadlineBlock) || !this.operation.managed) return;
    if (this.operation.managedCompleteObserved) {
      this.record("late-manual-compaction-event-ignored");
      return;
    }
    this.operation.matchingManagedSuccessEvents += 1;
    this.record("managed-compaction-event-observed");
    if (this.operation.matchingManagedSuccessEvents > 1) this.block("multiple-managed-compaction-events");
  }

  onManagedCompactionComplete(generationId: string, operationId: string): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation) return;
    const resolvesDeadlineBlock = this.phase === "blocked-unknown" && this.blockedReason === "compaction-deadline";
    if (this.phase !== "compacting" && !resolvesDeadlineBlock) return;
    if (this.operation.id !== operationId) {
      this.record("stale-operation-callback-dropped");
      return;
    }
    this.operation.managedCompleteObserved = true;
    if (this.operation.matchingManagedSuccessEvents === 0) {
      this.block("managed-complete-without-success-event");
      return;
    }
    if (this.operation.matchingManagedSuccessEvents !== 1) {
      this.block("multiple-managed-compaction-events");
      return;
    }
    this.clearCompactionDeadlines();
    this.lastOutcome = "completed";
    if (!this.persistClaim("compacted")) {
      this.block("claim-persist-failed", false);
      return;
    }
    if (!this.operation.resume) {
      void this.release(resolvesDeadlineBlock);
      return;
    }
    this.phase = "resuming";
    if (!this.persistClaim("resume-pending") || !this.persistClaim("resume-admitting")) {
      this.block("claim-persist-failed", false);
      return;
    }
    this.transition("compaction-succeeded");
    this.scheduleResumeDeadlines(this.operation.id);
    this.adapter?.sendResume(this.operation.resumeMessage);
    this.record("resume-sent");
  }

  onManagedCompactionError(generationId: string, operationId: string, error: Error): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation) return;
    const resolvesDeadlineBlock = this.phase === "blocked-unknown" && this.blockedReason === "compaction-deadline";
    if (this.phase !== "compacting" && !resolvesDeadlineBlock) return;
    if (this.operation.id !== operationId) {
      this.record("stale-operation-callback-dropped");
      return;
    }
    if (this.operation.matchingManagedSuccessEvents !== 0) {
      this.block("managed-error-after-success-event");
      return;
    }
    this.clearCompactionDeadlines();
    const cancelled = isCancellationError(error);
    this.lastOutcome = cancelled ? "cancelled" : "failed";
    if (!this.persistClaim(cancelled ? "cancelled" : "failed")) {
      this.block("claim-persist-failed", false);
      return;
    }
    this.record(cancelled
      ? (resolvesDeadlineBlock ? "late-managed-compaction-cancellation" : "managed-compaction-cancelled")
      : (resolvesDeadlineBlock ? "late-managed-compaction-failure" : "managed-compaction-failed"));
    void this.release(resolvesDeadlineBlock);
  }

  onMessageStart(generationId: string, message: { role: string; content?: unknown }): void {
    if (!this.isCurrentGeneration(generationId) || this.phase !== "resuming" || !this.operation) return;
    if (message.role !== "user" || exactTextContent(message.content) !== this.operation.resumeMessage) return;
    this.operation.resumeMessageMatched = true;
    this.clearResumeDeadlines();
    if (!this.persistClaim("resume-admitted")) {
      this.block("claim-persist-failed", false);
      return;
    }
    this.record("resume-message-matched");
  }

  onResumeAdmissionDeadline(generationId: string, operationId: string): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation) return;
    if (this.operation.id !== operationId) {
      this.record("stale-operation-deadline-dropped");
      return;
    }
    if (this.phase !== "resuming" || this.operation.resumeMessageMatched) return;
    this.block("resume-admission-deadline");
  }

  admitWake(request: WakeAdmission, permit?: ReleasePermit): WakeDisposition {
    if (typeof request.generationId !== "string" || request.generationId.length === 0) return { disposition: "reject", code: "generation-required" };
    if (!isLifecycleLane(request.laneId)) return { disposition: "reject", code: "lane-invalid" };
    if (this.disposed || !this.sessionId || !this.generationId || !this.phase) return { disposition: "reject", code: "session-unavailable" };
    const identity = { phase: this.phase, generationId: this.generationId, ...(this.operation === undefined ? {} : { operationId: this.operation.id }) };
    if (request.sessionId !== this.sessionId) return { disposition: "reject", code: "session-mismatch", ...identity };
    if (request.generationId !== this.generationId) return { disposition: "reject", code: "generation-mismatch", ...identity };
    if (this.phase === "idle") {
      this.invalidateSettlementProof();
      return { disposition: "deliver", code: "idle", ...identity };
    }
    if (this.phase === "releasing" && permit !== undefined && this.activePermits.has(permit) && permit.consumerId === request.consumerId && permit.laneId === request.laneId && permit.sessionId === this.sessionId && permit.generationId === this.generationId && permit.operationId === this.operation?.id) {
      return { disposition: "deliver", code: "release-permit", ...identity };
    }
    return { disposition: "hold", code: this.phase === "releasing" ? "release-permit-required" : "lifecycle-active", ...identity };
  }

  registerDrainer(registration: DrainerRegistration): () => void {
    if (this.disposed || registration.generationId !== this.generationId) throw new Error("Drainer generation mismatch");
    if (!isLifecycleLane(registration.laneId)) throw new Error("Drainer lane is invalid");
    const key = drainerKey(registration.consumerId, registration.laneId);
    if (this.drainers.has(key)) throw new Error(`Drainer already registered: ${registration.consumerId}/${registration.laneId}`);
    const stored: RegisteredDrainer = { ...registration, token: Symbol(key) };
    this.drainers.set(key, stored);
    let registered = true;
    return () => {
      if (!registered) return;
      registered = false;
      if (this.drainers.get(key)?.token === stored.token) this.drainers.delete(key);
    };
  }

  repair(request: RepairRequest): RepairDisposition {
    const reject = (code: string): RepairDisposition => ({ disposition: "rejected", code, ...(this.generationId === undefined ? {} : { generationId: this.generationId }) });
    if (this.disposed || !this.sessionId || !this.generationId || !this.operation || !this.phase) return reject("session-unavailable");
    if (request.sessionId !== this.sessionId) return reject("session-mismatch");
    if (request.generationId !== this.generationId) return reject("generation-mismatch");
    if (request.operationId !== this.operation.id) return reject("operation-mismatch");
    if (request.expectedPhase !== this.phase) return reject("phase-mismatch");
    const oldOwnerCannotExecute = this.operation.originOwnerInstanceId !== this.ownerInstanceId;
    if (request.action === "recognize-resume-admitted") {
      const restorableResume = this.blockedReason === "restored-resume-admitting" || this.blockedReason === "restored-resume-admitted";
      const evidenceMatches = request.evidenceClass === "persisted-resume-message" || request.evidenceClass === "persisted-resume-run-settled";
      const evidenceVerified = request.evidenceEntryId !== undefined && this.adapter?.verifyRepairEvidence?.(request) === true;
      if (!oldOwnerCannotExecute || !restorableResume || !evidenceMatches || !evidenceVerified || !this.operation.resume || this.lastOutcome !== "completed") return reject("repair-not-applicable");
      this.record("repair-applied", {
        action: request.action,
        evidenceClass: request.evidenceClass,
        actor: request.actor,
        channel: request.channel,
        priorPhase: "blocked-unknown",
        newPhase: request.evidenceClass === "persisted-resume-run-settled" ? "releasing" : "resuming",
      });
      if (this.blockedReason === "restored-resume-admitting" && !this.persistClaim("resume-admitted")) {
        this.block("claim-persist-failed", false);
        return reject("claim-persist-failed");
      }
      this.operation.resumeMessageMatched = true;
      if (request.evidenceClass === "persisted-resume-message") {
        this.blockedReason = undefined;
        this.phase = "resuming";
        this.transition("persisted-resume-admission-recognized");
      } else {
        if (!this.persistClaim("resume-settled")) {
          this.block("claim-persist-failed", false);
          return reject("claim-persist-failed");
        }
        this.operation.resume = false;
        void this.release(true);
      }
      return { disposition: "applied", action: request.action, operationId: request.operationId, generationId: this.generationId };
    }
    if (request.action === "retry-resume-pending") {
      if (!oldOwnerCannotExecute || this.blockedReason !== "restored-resume-pending" || request.evidenceClass !== "no-admission-attempt" || !this.operation.resume || this.lastOutcome !== "completed") return reject("repair-not-applicable");
      this.record("repair-applied", {
        action: request.action,
        evidenceClass: request.evidenceClass,
        actor: request.actor,
        channel: request.channel,
        priorPhase: "blocked-unknown",
        newPhase: "resuming",
      });
      this.blockedReason = undefined;
      this.phase = "resuming";
      if (!this.persistClaim("resume-admitting")) {
        this.block("claim-persist-failed", false);
        return reject("claim-persist-failed");
      }
      this.transition("resume-pending-retried");
      this.scheduleResumeDeadlines(this.operation.id);
      this.adapter?.sendResume(this.operation.resumeMessage);
      return { disposition: "applied", action: request.action, operationId: request.operationId, generationId: this.generationId };
    }
    if (request.action === "abandon-ambiguous-resume") {
      if (!oldOwnerCannotExecute || this.blockedReason !== "restored-resume-admitting" || request.evidenceClass !== "owner-process-replaced" || !this.operation.resume || this.operation.resumeMessageMatched || this.lastOutcome !== "completed") return reject("repair-not-applicable");
      this.record("repair-applied", {
        action: request.action,
        evidenceClass: request.evidenceClass,
        actor: request.actor,
        channel: request.channel,
        priorPhase: "blocked-unknown",
        newPhase: "releasing",
      });
      this.operation.resume = false;
      void this.release(true);
      return { disposition: "applied", action: request.action, operationId: request.operationId, generationId: this.generationId };
    }
    if (request.action === "retry-blocked-drainer") {
      const drainer = this.releaseCut?.[this.releaseIndex]?.registration;
      const retryableBlock = this.blockedReason === "drainer-deadline" || this.blockedReason === "drainer-threw" || this.blockedReason === "drainer-blocked";
      if (!retryableBlock || request.evidenceClass !== "idempotent-drainer-state" || request.consumerId === undefined || request.laneId === undefined || request.consumerId !== this.blockedDrainerConsumerId || request.laneId !== this.blockedDrainerLaneId || drainer?.consumerId !== request.consumerId || drainer.laneId !== request.laneId) return reject("repair-not-applicable");
      this.record("repair-applied", {
        action: request.action,
        evidenceClass: request.evidenceClass,
        actor: request.actor,
        channel: request.channel,
        priorPhase: "blocked-unknown",
        newPhase: "releasing",
      });
      void this.release(true, true);
      return { disposition: "applied", action: request.action, operationId: request.operationId, generationId: this.generationId };
    }
    if (!oldOwnerCannotExecute || request.evidenceClass !== "branch-validated-owner-replaced") return reject("repair-not-applicable");
    if (this.blockedReason !== "restored-requested") return reject("fresh-session-required");
    this.lastOutcome = "cancelled";
    if (!this.persistClaim("cancelled")) {
      this.block("claim-persist-failed", false);
      return reject("claim-persist-failed");
    }
    this.record("repair-applied", {
      action: request.action,
      evidenceClass: request.evidenceClass,
      actor: request.actor,
      channel: request.channel,
      priorPhase: "blocked-unknown",
      newPhase: "releasing",
    });
    this.operation.resume = false;
    void this.release(true);
    return { disposition: "applied", action: request.action, operationId: request.operationId, generationId: this.generationId };
  }

  diagnostics(): readonly DiagnosticRecord[] {
    return this.records.map((record) => ({ ...record }));
  }

  dispose(): boolean {
    if (this.disposed) return false;
    this.disposed = true;
    this.clearAllDeadlines();
    this.activePermits.clear();
    this.drainers.clear();
    this.releaseCut = undefined;
    this.releaseIndex = 0;
    this.releaseAwaitingSettlementEpoch = undefined;
    this.blockedDrainerConsumerId = undefined;
    this.blockedDrainerLaneId = undefined;
    this.phase = undefined;
    this.settlementProof = undefined;
    this.adapter = undefined;
    const result = this.publication?.dispose() ?? false;
    this.publication = undefined;
    return result;
  }

  private observeSettlement(source: SettlementProof["source"]): void {
    if (!this.generationId) return;
    this.settlementProof = { generationId: this.generationId, source, epoch: ++this.settlementEpoch };
    this.record("settlement-observed");
  }

  private hasCurrentSettlementProof(): boolean {
    return this.settlementProof?.generationId === this.generationId;
  }

  private invalidateSettlementProof(): void {
    if (!this.hasCurrentSettlementProof()) return;
    this.settlementProof = undefined;
    this.record("settlement-invalidated");
  }

  private startCompaction(): void {
    if (!this.operation || this.operation.compactStarted || !this.adapter) return;
    this.operation.compactStarted = true;
    this.phase = "compacting";
    if (!this.persistClaim("compacting")) {
      this.block("claim-persist-failed", false);
      return;
    }
    this.transition("compaction-started");
    const generation = this.generationId;
    const operationId = this.operation.id;
    this.scheduleCompactionDeadlines(operationId);
    this.adapter.compact({
      customInstructions: this.operation.customInstructions,
      onComplete: () => {
        if (generation !== undefined) this.onManagedCompactionComplete(generation, operationId);
      },
      onError: (error) => {
        if (generation !== undefined) this.onManagedCompactionError(generation, operationId, error);
      },
    });
  }

  private async release(fromBlockedRepair = false, retryBlockedDrainer = false): Promise<void> {
    const adoptedTerminalAtSettlement = this.phase === "pending-settle"
      && this.operation?.adoptedAutomaticReason !== undefined
      && this.operation.managedCompleteObserved;
    const continuingRelease = this.phase === "releasing";
    if (!this.operation || !this.sessionId || !this.generationId || (this.phase !== "resuming" && this.phase !== "compacting" && this.phase !== "observed-preflight" && !adoptedTerminalAtSettlement && !(fromBlockedRepair && this.phase === "blocked-unknown") && !continuingRelease)) return;
    if (continuingRelease && this.releaseAwaitingSettlementEpoch !== undefined) return;
    this.clearAllDeadlines();
    this.blockedReason = undefined;
    this.blockedDrainerConsumerId = undefined;
    this.blockedDrainerLaneId = undefined;
    this.phase = "releasing";
    if (!retryBlockedDrainer && !continuingRelease) {
      const registrations = [...this.drainers.values()].sort((left, right) => (LANE_ORDER.get(left.laneId) ?? Number.MAX_SAFE_INTEGER) - (LANE_ORDER.get(right.laneId) ?? Number.MAX_SAFE_INTEGER) || left.consumerId.localeCompare(right.consumerId));
      try {
        this.releaseCut = registrations.map((registration) => {
          const cut = registration.capture();
          if (!Number.isSafeInteger(cut.watermark) || cut.watermark < 0 || !Number.isSafeInteger(cut.heldCount) || cut.heldCount < 0) throw new Error("invalid release watermark");
          return { registration, cut: Object.freeze({ ...cut }) };
        });
      } catch {
        this.block("release-cut-capture-failed");
        return;
      }
      this.releaseIndex = 0;
    }
    if (this.releaseCut === undefined) {
      this.block("release-cut-unavailable");
      return;
    }
    this.transition(retryBlockedDrainer ? "blocked-drainer-retry-started" : "release-started");
    const operation = this.operation;
    while (this.releaseIndex < this.releaseCut.length) {
      const captured = this.releaseCut[this.releaseIndex];
      const drainer = captured?.registration;
      if (captured === undefined || drainer === undefined || !this.isCurrentGeneration(drainer.generationId) || this.operation !== operation) return;
      const permit: ReleasePermit = Object.freeze({
        protocolVersion: 1,
        sessionId: this.sessionId,
        generationId: this.generationId,
        operationId: operation.id,
        releaseId: randomUUID(),
        consumerId: drainer.consumerId,
        laneId: drainer.laneId,
        cut: captured.cut,
      });
      this.activePermits.add(permit);
      // Capture before drain() so a settlement during submission but before
      // its acknowledgement is processed satisfies the barrier.
      const submissionSettlementEpoch = this.settlementEpoch;
      let pendingAck: Promise<DrainAck> | DrainAck;
      try {
        pendingAck = drainer.drain(permit);
      } catch {
        this.activePermits.delete(permit);
        this.blockedDrainerConsumerId = drainer.consumerId;
        this.blockedDrainerLaneId = drainer.laneId;
        this.block("drainer-threw");
        return;
      }
      const outcome = await this.waitForDrainer(pendingAck);
      if (this.disposed || this.operation !== operation || drainer.generationId !== this.generationId) return;
      this.activePermits.delete(permit);
      if (outcome.kind === "timeout") {
        this.blockedDrainerConsumerId = drainer.consumerId;
        this.blockedDrainerLaneId = drainer.laneId;
        this.block("drainer-deadline");
        return;
      }
      if (outcome.kind === "threw") {
        this.blockedDrainerConsumerId = drainer.consumerId;
        this.blockedDrainerLaneId = drainer.laneId;
        this.block("drainer-threw");
        return;
      }
      const ack = outcome.ack;
      const disposition: unknown = (ack as { disposition?: unknown }).disposition;
      if (ack.releaseId !== permit.releaseId
        || ack.consumerId !== permit.consumerId
        || ack.laneId !== permit.laneId
        || !Number.isSafeInteger(ack.submittedCount) || ack.submittedCount < 0
        || !Number.isSafeInteger(ack.handledCount) || ack.handledCount !== permit.cut.heldCount
        || ack.handledThrough !== permit.cut.watermark
        || (disposition !== "empty" && disposition !== "submitted" && disposition !== "blocked")
        || (disposition === "empty" && (permit.cut.heldCount !== 0 || ack.submittedCount !== 0))
        || (disposition === "submitted" && (permit.cut.heldCount === 0 || ack.submittedCount !== 1))
        || (disposition === "blocked" && ack.submittedCount !== 0)
        || disposition === "blocked") {
        this.blockedDrainerConsumerId = drainer.consumerId;
        this.blockedDrainerLaneId = drainer.laneId;
        this.block("drainer-blocked");
        return;
      }
      if (disposition === "submitted" && this.settlementEpoch <= submissionSettlementEpoch) {
        this.releaseAwaitingSettlementEpoch = submissionSettlementEpoch;
        this.record("release-submission-awaiting-settlement");
        return;
      }
      this.releaseIndex += 1;
    }
    if (this.operation !== operation || this.disposed) return;
    if (!this.persistClaim("released")) {
      this.block("claim-persist-failed", false);
      return;
    }
    this.operation = undefined;
    this.releaseCut = undefined;
    this.releaseIndex = 0;
    this.releaseAwaitingSettlementEpoch = undefined;
    this.phase = "idle";
    this.transition("release-completed");
  }

  private block(code: string, persist = true): void {
    if (this.phase === "blocked-unknown") return;
    this.clearAllDeadlines();
    this.releaseAwaitingSettlementEpoch = undefined;
    this.blockedReason = code;
    this.phase = "blocked-unknown";
    if (persist) this.persistClaim("blocked-unknown");
    this.transition(code);
  }

  private persistClaim(state: LifecycleClaimState): boolean {
    if (!this.operation || !this.sessionId || !this.generationId) return false;
    try {
      this.adapter?.appendLifecycleEntry?.({
        schemaVersion: 1,
        ownerInstanceId: this.ownerInstanceId,
        originOwnerInstanceId: this.operation.originOwnerInstanceId,
        operationId: this.operation.id,
        sessionId: this.sessionId,
        generationId: this.generationId,
        state,
        reason: this.operation.reason,
        resumeIntent: this.operation.resume,
        timestamp: Date.now(),
      });
      this.operation.claimState = state;
      return true;
    } catch {
      this.record("claim-persist-error");
      return false;
    }
  }

  private async waitForDrainer(pendingAck: Promise<DrainAck> | DrainAck): Promise<{ kind: "ack"; ack: DrainAck } | { kind: "threw" } | { kind: "timeout" }> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const acknowledgement = Promise.resolve(pendingAck).then(
      (ack) => ({ kind: "ack" as const, ack }),
      () => ({ kind: "threw" as const }),
    );
    const timeout = new Promise<{ kind: "timeout" }>((resolve) => {
      timer = setTimeout(() => resolve({ kind: "timeout" }), DRAINER_BLOCK_MS);
      timer.unref();
    });
    const outcome = await Promise.race([acknowledgement, timeout]);
    if (timer !== undefined) clearTimeout(timer);
    return outcome;
  }

  private scheduleCompactionDeadlines(operationId: string): void {
    this.clearCompactionDeadlines();
    const generationId = this.generationId;
    this.compactionWarningTimer = setTimeout(() => {
      if (generationId !== undefined && this.isCurrentOperation(generationId, operationId, ["observed-preflight", "compacting"])) this.record("compaction-attention-warning");
    }, COMPACTION_WARNING_MS);
    this.compactionBlockTimer = setTimeout(() => {
      if (generationId !== undefined && this.isCurrentOperation(generationId, operationId, ["observed-preflight", "compacting"])) this.block("compaction-deadline");
    }, COMPACTION_BLOCK_MS);
    this.compactionWarningTimer.unref();
    this.compactionBlockTimer.unref();
  }

  private scheduleResumeDeadlines(operationId: string): void {
    this.clearResumeDeadlines();
    const generationId = this.generationId;
    this.resumeWarningTimer = setTimeout(() => {
      if (generationId !== undefined && this.isCurrentOperation(generationId, operationId, ["resuming"]) && this.operation !== undefined && !this.operation.resumeMessageMatched) this.record("resume-admission-attention-warning");
    }, RESUME_WARNING_MS);
    this.resumeBlockTimer = setTimeout(() => {
      if (generationId !== undefined && this.isCurrentOperation(generationId, operationId, ["resuming"]) && this.operation !== undefined && !this.operation.resumeMessageMatched) this.block("resume-admission-deadline");
    }, RESUME_BLOCK_MS);
    this.resumeWarningTimer.unref();
    this.resumeBlockTimer.unref();
  }

  private clearCompactionDeadlines(): void {
    if (this.compactionWarningTimer !== undefined) clearTimeout(this.compactionWarningTimer);
    if (this.compactionBlockTimer !== undefined) clearTimeout(this.compactionBlockTimer);
    this.compactionWarningTimer = undefined;
    this.compactionBlockTimer = undefined;
  }

  private clearResumeDeadlines(): void {
    if (this.resumeWarningTimer !== undefined) clearTimeout(this.resumeWarningTimer);
    if (this.resumeBlockTimer !== undefined) clearTimeout(this.resumeBlockTimer);
    this.resumeWarningTimer = undefined;
    this.resumeBlockTimer = undefined;
  }

  private clearAllDeadlines(): void {
    this.clearCompactionDeadlines();
    this.clearResumeDeadlines();
  }

  private isCurrentOperation(generationId: string, operationId: string, phases: Phase[]): boolean {
    return !this.disposed && generationId === this.generationId && operationId === this.operation?.id && this.phase !== undefined && phases.includes(this.phase);
  }

  private isCurrentGeneration(generationId: string): boolean {
    if (!this.disposed && generationId === this.generationId) return true;
    this.record("stale-generation-callback-dropped");
    return false;
  }

  private snapshotFields(): Omit<Snapshot, "protocolVersion" | "registryState" | "sequence" | "ownerInstanceId"> {
    return {
      ...(this.sessionId === undefined ? {} : { sessionId: this.sessionId }),
      ...(this.generationId === undefined ? {} : { generationId: this.generationId }),
      ...(this.phase === undefined ? {} : { phase: this.phase }),
      ...(this.operation === undefined ? {} : { operationId: this.operation.id, reason: this.operation.reason, resumeIntent: this.operation.resume, startedAt: this.operation.startedAt }),
      ...(this.lastOutcome === undefined ? {} : { lastOutcome: this.lastOutcome }),
    };
  }

  private transition(code: string): void {
    this.publication?.update(this.snapshotFields());
    this.record(code);
  }

  private record(code: string, details: Partial<Pick<DiagnosticRecord, "action" | "evidenceClass" | "actor" | "channel" | "priorPhase" | "newPhase">> = {}): void {
    const record: DiagnosticRecord = {
      protocolVersion: 1,
      sequence: ++this.diagnosticSequence,
      timestamp: Date.now(),
      code,
      ownerInstanceId: this.ownerInstanceId,
      ...(this.sessionId === undefined ? {} : { sessionId: this.sessionId }),
      ...(this.generationId === undefined ? {} : { generationId: this.generationId }),
      ...(this.operation === undefined ? {} : { operationId: this.operation.id }),
      ...(this.phase === undefined ? {} : { phase: this.phase }),
      ...(this.lastOutcome === undefined ? {} : { outcome: this.lastOutcome }),
      ...details,
    };
    this.records.push(record);
    if (this.records.length > MAX_DIAGNOSTICS) this.records.splice(0, this.records.length - MAX_DIAGNOSTICS);
  }
}
