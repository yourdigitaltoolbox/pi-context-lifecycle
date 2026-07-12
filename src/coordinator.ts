import { randomUUID } from "node:crypto";
import type {
  CompactDisposition,
  CompactRequest,
  CompactionReason,
  CoordinatorPublisherV1,
  DiagnosticRecord,
  DrainAck,
  DrainerRegistration,
  OperationOutcome,
  Phase,
  ReleasePermit,
  RepairDisposition,
  RepairRequest,
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

export interface ManagedCompactionAdapter {
  compact(options: { customInstructions: string; onComplete(): void; onError(error: Error): void }): void;
  sendResume(message: string): void;
}

interface ActiveOperation {
  id: string;
  reason: CompactionReason;
  managed: boolean;
  startedAt: number;
  resume: boolean;
  customInstructions: string;
  resumeMessage: string;
  compactStarted: boolean;
  matchingManagedSuccessEvents: number;
  managedCompleteObserved: boolean;
  resumeMessageMatched: boolean;
}

interface RegisteredDrainer extends DrainerRegistration {
  token: symbol;
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
  private disposed = false;
  private diagnosticSequence = 0;
  private readonly records: DiagnosticRecord[] = [];
  private readonly drainers = new Map<string, RegisteredDrainer>();
  private readonly activePermits = new Set<ReleasePermit>();
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
    return this.generationId;
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
    if (this.operation !== undefined) {
      if (request.resume === true || request.reason === "self") this.operation.resume = true;
      this.record("request-joined");
      return { disposition: "joined", operationId: this.operation.id, generationId: this.generationId };
    }
    if (this.phase !== "idle") return { disposition: "rejected", code: "not-idle", generationId: this.generationId };
    const operationId = randomUUID();
    this.operation = {
      id: operationId,
      reason: request.reason,
      managed: true,
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
    this.transition("request-accepted");
    return { disposition: "accepted", operationId: this.operation.id, generationId: this.generationId };
  }

  onAgentSettled(generationId: string): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation) return;
    if (this.phase === "pending-settle") {
      this.startCompaction();
      return;
    }
    if (this.phase === "observed-preflight" && !this.operation.managed) {
      this.lastOutcome = "failed";
      this.record("automatic-compaction-failed");
      void this.release();
      return;
    }
    if (this.phase === "resuming" && this.operation.resumeMessageMatched) void this.release();
  }

  onSessionBeforeCompact(generationId: string, reason: "manual" | "threshold" | "overflow"): void {
    if (!this.isCurrentGeneration(generationId)) return;
    if (reason === "manual") {
      if (this.phase === "compacting" && this.operation?.managed) this.record("managed-compaction-preflight-observed");
      return;
    }
    if (this.phase !== "idle" || this.operation !== undefined) {
      this.block("automatic-compaction-overlapped-active-operation");
      return;
    }
    this.operation = {
      id: randomUUID(),
      reason,
      managed: false,
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
    this.transition("automatic-compaction-preflight-observed");
    this.scheduleCompactionDeadlines(this.operation.id);
  }

  onSessionCompact(generationId: string, reason: "manual" | "threshold" | "overflow"): void {
    if (!this.isCurrentGeneration(generationId) || !this.operation) return;
    if (reason !== "manual") {
      if (this.operation.managed || this.operation.reason !== reason || this.phase !== "observed-preflight") {
        this.block("automatic-compaction-event-mismatch");
        return;
      }
      this.clearCompactionDeadlines();
      this.lastOutcome = "completed";
      this.record("automatic-compaction-succeeded");
      void this.release();
      return;
    }
    if (this.phase !== "compacting" || !this.operation.managed) return;
    if (this.operation.managedCompleteObserved) {
      this.record("late-manual-compaction-event-ignored");
      return;
    }
    this.operation.matchingManagedSuccessEvents += 1;
    this.record("managed-compaction-event-observed");
    if (this.operation.matchingManagedSuccessEvents > 1) this.block("multiple-managed-compaction-events");
  }

  onManagedCompactionComplete(generationId: string, operationId: string): void {
    if (!this.isCurrentGeneration(generationId) || this.phase !== "compacting" || !this.operation) return;
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
    if (!this.operation.resume) {
      void this.release();
      return;
    }
    this.phase = "resuming";
    this.transition("compaction-succeeded");
    this.scheduleResumeDeadlines(this.operation.id);
    this.adapter?.sendResume(this.operation.resumeMessage);
    this.record("resume-sent");
  }

  onManagedCompactionError(generationId: string, operationId: string): void {
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
    this.lastOutcome = "failed";
    this.record(resolvesDeadlineBlock ? "late-managed-compaction-failure" : "managed-compaction-failed");
    void this.release(resolvesDeadlineBlock);
  }

  onMessageStart(generationId: string, message: { role: string; content?: unknown }): void {
    if (!this.isCurrentGeneration(generationId) || this.phase !== "resuming" || !this.operation) return;
    if (message.role !== "user" || exactTextContent(message.content) !== this.operation.resumeMessage) return;
    this.operation.resumeMessageMatched = true;
    this.clearResumeDeadlines();
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
    if (this.disposed || !this.sessionId || !this.generationId || !this.phase) return { disposition: "reject", code: "session-unavailable" };
    const identity = { phase: this.phase, generationId: this.generationId, ...(this.operation === undefined ? {} : { operationId: this.operation.id }) };
    if (request.sessionId !== this.sessionId) return { disposition: "reject", code: "session-mismatch", ...identity };
    if (request.generationId !== this.generationId) return { disposition: "reject", code: "generation-mismatch", ...identity };
    if (this.phase === "idle") return { disposition: "deliver", code: "idle", ...identity };
    if (this.phase === "releasing" && permit !== undefined && this.activePermits.has(permit) && permit.consumerId === request.consumerId && permit.sessionId === this.sessionId && permit.generationId === this.generationId && permit.operationId === this.operation?.id) {
      return { disposition: "deliver", code: "release-permit", ...identity };
    }
    return { disposition: "hold", code: this.phase === "releasing" ? "release-permit-required" : "lifecycle-active", ...identity };
  }

  registerDrainer(registration: DrainerRegistration): () => void {
    if (this.disposed || registration.generationId !== this.generationId) throw new Error("Drainer generation mismatch");
    if (this.drainers.has(registration.consumerId)) throw new Error(`Drainer already registered: ${registration.consumerId}`);
    const stored: RegisteredDrainer = { ...registration, token: Symbol(registration.consumerId) };
    this.drainers.set(registration.consumerId, stored);
    let registered = true;
    return () => {
      if (!registered) return;
      registered = false;
      if (this.drainers.get(registration.consumerId)?.token === stored.token) this.drainers.delete(registration.consumerId);
    };
  }

  repair(request: RepairRequest): RepairDisposition {
    const reject = (code: string): RepairDisposition => ({ disposition: "rejected", code, ...(this.generationId === undefined ? {} : { generationId: this.generationId }) });
    if (this.disposed || !this.sessionId || !this.generationId || !this.operation || !this.phase) return reject("session-unavailable");
    if (request.sessionId !== this.sessionId) return reject("session-mismatch");
    if (request.generationId !== this.generationId) return reject("generation-mismatch");
    if (request.operationId !== this.operation.id) return reject("operation-mismatch");
    if (request.expectedPhase !== this.phase) return reject("phase-mismatch");
    if (this.blockedReason !== "resume-admission-deadline" || !this.operation.resume || this.operation.resumeMessageMatched || this.lastOutcome !== "completed") return reject("repair-not-applicable");

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
    this.phase = undefined;
    this.adapter = undefined;
    const result = this.publication?.dispose() ?? false;
    this.publication = undefined;
    return result;
  }

  private startCompaction(): void {
    if (!this.operation || this.operation.compactStarted || !this.adapter) return;
    this.operation.compactStarted = true;
    this.phase = "compacting";
    this.transition("compaction-started");
    const generation = this.generationId;
    const operationId = this.operation.id;
    this.scheduleCompactionDeadlines(operationId);
    this.adapter.compact({
      customInstructions: this.operation.customInstructions,
      onComplete: () => {
        if (generation !== undefined) this.onManagedCompactionComplete(generation, operationId);
      },
      onError: () => {
        if (generation !== undefined) this.onManagedCompactionError(generation, operationId);
      },
    });
  }

  private async release(fromBlockedRepair = false): Promise<void> {
    if (!this.operation || !this.sessionId || !this.generationId || (this.phase !== "resuming" && this.phase !== "compacting" && this.phase !== "observed-preflight" && !(fromBlockedRepair && this.phase === "blocked-unknown"))) return;
    this.clearAllDeadlines();
    this.blockedReason = undefined;
    this.phase = "releasing";
    this.transition("release-started");
    const operation = this.operation;
    const drainers = [...this.drainers.values()].sort((left, right) => left.priority - right.priority || left.consumerId.localeCompare(right.consumerId));
    for (const drainer of drainers) {
      if (!this.isCurrentGeneration(drainer.generationId) || this.operation !== operation) return;
      const permit: ReleasePermit = Object.freeze({
        protocolVersion: 1,
        sessionId: this.sessionId,
        generationId: this.generationId,
        operationId: operation.id,
        releaseId: randomUUID(),
        consumerId: drainer.consumerId,
      });
      this.activePermits.add(permit);
      let pendingAck: Promise<DrainAck> | DrainAck;
      try {
        pendingAck = drainer.drain(permit);
      } catch {
        this.activePermits.delete(permit);
        this.block("drainer-threw");
        return;
      }
      const outcome = await this.waitForDrainer(pendingAck);
      if (this.disposed || this.operation !== operation || drainer.generationId !== this.generationId) return;
      this.activePermits.delete(permit);
      if (outcome.kind === "timeout") {
        this.block("drainer-deadline");
        return;
      }
      if (outcome.kind === "threw") {
        this.block("drainer-threw");
        return;
      }
      const ack = outcome.ack;
      if (ack.releaseId !== permit.releaseId || ack.consumerId !== permit.consumerId || ack.submittedCount < 0 || ack.disposition === "blocked") {
        this.block("drainer-blocked");
        return;
      }
    }
    if (this.operation !== operation || this.disposed) return;
    this.operation = undefined;
    this.phase = "idle";
    this.transition("release-completed");
  }

  private block(code: string): void {
    if (this.phase === "blocked-unknown") return;
    this.clearAllDeadlines();
    this.blockedReason = code;
    this.phase = "blocked-unknown";
    this.transition(code);
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
      ...(this.operation === undefined ? {} : { operationId: this.operation.id, reason: this.operation.reason, startedAt: this.operation.startedAt }),
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
