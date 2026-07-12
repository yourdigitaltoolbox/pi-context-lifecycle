import { randomUUID } from "node:crypto";
import type {
  CompactDisposition,
  CompactRequest,
  CoordinatorPublisherV1,
  DiagnosticRecord,
  DrainAck,
  DrainerRegistration,
  OperationOutcome,
  Phase,
  ReleasePermit,
  Snapshot,
  WakeAdmission,
  WakeDisposition,
} from "./types.js";
import type { CoordinatorPublicationV1 } from "./registry.js";

const MAX_DIAGNOSTICS = 100;

export interface ManagedCompactionAdapter {
  compact(options: { customInstructions: string; onComplete(): void; onError(error: Error): void }): void;
  sendResume(message: string): void;
}

interface ActiveOperation {
  id: string;
  reason: "self" | "remote";
  startedAt: number;
  resume: boolean;
  customInstructions: string;
  resumeMessage: string;
  compactStarted: boolean;
  resumeInputObserved: boolean;
  resumeAdmitted: boolean;
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

export class ContextLifecycleCoordinatorV1 implements CoordinatorPublisherV1 {
  readonly protocolVersion = 1 as const;
  readonly ownerInstanceId: string;
  private publication: CoordinatorPublicationV1 | undefined;
  private sessionId: string | undefined;
  private generationId: string | undefined;
  private phase: Phase | undefined;
  private operation: ActiveOperation | undefined;
  private lastOutcome: OperationOutcome | undefined;
  private adapter: ManagedCompactionAdapter | undefined;
  private disposed = false;
  private diagnosticSequence = 0;
  private readonly records: DiagnosticRecord[] = [];
  private readonly drainers = new Map<string, RegisteredDrainer>();
  private readonly activePermits = new Set<ReleasePermit>();

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
    return this.request({ requestId, sessionId: this.sessionId ?? "", reason: "self", resume: true }, {
      customInstructions: withFocus(DEFAULT_COMPACTION_INSTRUCTIONS, "User focus for this compact/resume", focus),
      resumeMessage: withFocus(DEFAULT_RESUME_MESSAGE, "User focus", focus),
    });
  }

  requestCompaction(request: CompactRequest): CompactDisposition {
    return this.request(request, {
      customInstructions: DEFAULT_COMPACTION_INSTRUCTIONS,
      resumeMessage: DEFAULT_RESUME_MESSAGE,
    });
  }

  private request(request: CompactRequest, content: { customInstructions: string; resumeMessage: string }): CompactDisposition {
    if (this.disposed || !this.sessionId || !this.generationId || !this.phase) return { disposition: "rejected", code: "session-unavailable" };
    if (request.sessionId !== this.sessionId) return { disposition: "rejected", code: "session-mismatch", generationId: this.generationId };
    if (request.generationId !== undefined && request.generationId !== this.generationId) return { disposition: "rejected", code: "generation-mismatch", generationId: this.generationId };
    if (this.operation !== undefined) {
      if (request.resume === true || request.reason === "self") this.operation.resume = true;
      this.record("request-joined");
      return { disposition: "joined", operationId: this.operation.id, generationId: this.generationId };
    }
    if (this.phase !== "idle") return { disposition: "rejected", code: "not-idle", generationId: this.generationId };
    this.operation = {
      id: randomUUID(),
      reason: request.reason,
      startedAt: Date.now(),
      resume: request.resume === true || request.reason === "self",
      customInstructions: content.customInstructions,
      resumeMessage: content.resumeMessage,
      compactStarted: false,
      resumeInputObserved: false,
      resumeAdmitted: false,
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
    if (this.phase === "resuming" && this.operation.resumeAdmitted) void this.release();
  }

  onCompactionSuccess(generationId: string): void {
    if (!this.isCurrentGeneration(generationId) || this.phase !== "compacting" || !this.operation) return;
    this.lastOutcome = "completed";
    if (!this.operation.resume) {
      void this.release();
      return;
    }
    this.phase = "resuming";
    this.transition("compaction-succeeded");
    this.adapter?.sendResume(this.operation.resumeMessage);
    this.record("resume-sent");
  }

  onInput(generationId: string, input: { source: string; text: string }): void {
    if (!this.isCurrentGeneration(generationId) || this.phase !== "resuming" || !this.operation) return;
    if (input.source === "extension" && input.text === this.operation.resumeMessage) this.operation.resumeInputObserved = true;
  }

  onAgentStart(generationId: string): void {
    if (!this.isCurrentGeneration(generationId) || this.phase !== "resuming" || !this.operation?.resumeInputObserved) return;
    this.operation.resumeAdmitted = true;
    this.record("resume-admitted");
  }

  admitWake(request: WakeAdmission, permit?: ReleasePermit): WakeDisposition {
    if (this.disposed || !this.sessionId || !this.generationId || !this.phase) return { disposition: "reject", code: "session-unavailable" };
    const identity = { phase: this.phase, generationId: this.generationId, ...(this.operation === undefined ? {} : { operationId: this.operation.id }) };
    if (request.sessionId !== this.sessionId) return { disposition: "reject", code: "session-mismatch", ...identity };
    if (request.generationId !== undefined && request.generationId !== this.generationId) return { disposition: "reject", code: "generation-mismatch", ...identity };
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

  diagnostics(): readonly DiagnosticRecord[] {
    return this.records.map((record) => ({ ...record }));
  }

  dispose(): boolean {
    if (this.disposed) return false;
    this.disposed = true;
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
    this.adapter.compact({
      customInstructions: this.operation.customInstructions,
      onComplete: () => { /* session_compact is the durable success authority */ },
      onError: () => {
        if (generation === this.generationId) this.record("compaction-error-observed");
      },
    });
  }

  private async release(): Promise<void> {
    if (!this.operation || !this.sessionId || !this.generationId || (this.phase !== "resuming" && this.phase !== "compacting")) return;
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
      let ack: DrainAck;
      try {
        ack = await drainer.drain(permit);
      } catch {
        this.activePermits.delete(permit);
        this.phase = "blocked-unknown";
        this.transition("drainer-threw");
        return;
      }
      this.activePermits.delete(permit);
      if (ack.releaseId !== permit.releaseId || ack.consumerId !== permit.consumerId || ack.submittedCount < 0 || ack.disposition === "blocked") {
        this.phase = "blocked-unknown";
        this.transition("drainer-blocked");
        return;
      }
    }
    if (this.operation !== operation || this.disposed) return;
    this.operation = undefined;
    this.phase = "idle";
    this.transition("release-completed");
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

  private record(code: string): void {
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
    };
    this.records.push(record);
    if (this.records.length > MAX_DIAGNOSTICS) this.records.splice(0, this.records.length - MAX_DIAGNOSTICS);
  }
}
