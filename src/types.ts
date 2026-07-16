export const CONTEXT_LIFECYCLE_PROTOCOL_VERSION = 1 as const;
export const CONTEXT_LIFECYCLE_REGISTRY_SYMBOL = Symbol.for("yourdigitaltoolbox.pi-context-lifecycle.v1");
export const CONTEXT_LIFECYCLE_RELEASE_LANES = [
  "failure-attention-decision",
  "mesh-reply",
  "mesh-unsolicited",
  "subagent-success",
  "background-notify",
  "loop-tick",
  "cron-tick",
] as const;
export type LifecycleLane = typeof CONTEXT_LIFECYCLE_RELEASE_LANES[number];

export type RegistryState = "unavailable" | "ready" | "disposing" | "incompatible";
export type Phase =
  | "idle"
  | "pending-settle"
  | "observed-preflight"
  | "compacting"
  | "resuming"
  | "releasing"
  | "blocked-unknown";
export type CompactionReason = "self" | "remote" | "builtin" | "threshold" | "overflow";
export type OperationOutcome = "completed" | "failed" | "cancelled" | "timed-out";
export type LifecycleClaimState =
  | "requested"
  | "compacting"
  | "compacted"
  | "resume-pending"
  | "resume-admitting"
  | "resume-admitted"
  | "resume-settled"
  | "released"
  | "failed"
  | "cancelled"
  | "blocked-unknown";

export interface LifecycleClaim {
  schemaVersion: 1;
  ownerInstanceId: string;
  originOwnerInstanceId: string;
  operationId: string;
  sessionId: string;
  generationId: string;
  state: LifecycleClaimState;
  reason: CompactionReason;
  resumeIntent: boolean;
  timestamp: number;
}

export interface Snapshot {
  protocolVersion: 1;
  registryState: RegistryState;
  sequence: number;
  ownerInstanceId?: string;
  sessionId?: string;
  generationId?: string;
  phase?: Phase;
  operationId?: string;
  reason?: CompactionReason;
  resumeIntent?: boolean;
  startedAt?: number;
  lastOutcome?: OperationOutcome;
}

export interface LifecycleEvent extends Snapshot {
  event: "snapshot";
}

export type CompactSettlementPolicy = "next-agent-settled" | "current-or-next-settled-boundary";

export interface CompactRequest {
  requestId: string;
  sessionId: string;
  generationId: string;
  reason: "self" | "remote";
  resume?: boolean;
  source?: string;
  /**
   * Defaults to the next genuine agent_settled boundary. The immediate policy
   * is accepted only from the fixed authenticated Remote adapter attestation.
   */
  settlementPolicy?: CompactSettlementPolicy;
  actor?: "operator";
  channel?: "remote";
}

export type CompactDisposition =
  | { disposition: "accepted" | "joined"; operationId: string; generationId: string }
  | { disposition: "rejected"; code: string; generationId?: string };

export interface WakeAdmission {
  consumerId: string;
  laneId: LifecycleLane;
  wakeId: string;
  sessionId: string;
  generationId: string;
  source?: string;
}

export interface WakeDisposition {
  disposition: "deliver" | "hold" | "reject";
  phase?: Phase;
  generationId?: string;
  operationId?: string;
  code: string;
}

export interface ReleaseWatermark {
  watermark: number;
  heldCount: number;
}

export interface ReleasePermit {
  readonly protocolVersion: 1;
  readonly sessionId: string;
  readonly generationId: string;
  readonly operationId: string;
  readonly releaseId: string;
  readonly consumerId: string;
  readonly laneId: LifecycleLane;
  readonly cut: ReleaseWatermark;
}

export interface DrainAck {
  releaseId: string;
  consumerId: string;
  laneId: LifecycleLane;
  disposition: "empty" | "submitted" | "blocked";
  /**
   * V1 submission receipt: 0 for empty or blocked drains, and exactly 1
   * for a submitted non-empty drain. A submitted receipt represents one Pi
   * sendMessage(..., { triggerTurn: true }) invocation.
   */
  submittedCount: number;
  handledCount: number;
  handledThrough: number;
}

export interface DrainerRegistration {
  consumerId: string;
  laneId: LifecycleLane;
  generationId: string;
  capture(): ReleaseWatermark;
  drain(permit: ReleasePermit): Promise<DrainAck> | DrainAck;
}

export type RepairAction =
  | "recognize-resume-admitted"
  | "retry-resume-pending"
  | "abandon-ambiguous-resume"
  | "retry-blocked-drainer"
  | "abandon-interrupted-operation";
export type RepairEvidenceClass =
  | "persisted-resume-message"
  | "persisted-resume-run-settled"
  | "no-admission-attempt"
  | "current-process-quiescent"
  | "owner-process-replaced"
  | "idempotent-drainer-state"
  | "branch-validated-owner-replaced";
export type RepairActor = "operator";
export type RepairChannel = "command" | "remote";

export interface RepairRequest {
  action: RepairAction;
  operationId: string;
  sessionId: string;
  generationId: string;
  expectedPhase: "blocked-unknown";
  expectedSequence: number;
  evidenceClass: RepairEvidenceClass;
  actor: RepairActor;
  channel: RepairChannel;
  consumerId?: string;
  laneId?: LifecycleLane;
  evidenceEntryId?: string;
}

export type RepairDisposition =
  | { disposition: "applied"; action: RepairAction; operationId: string; generationId: string }
  | { disposition: "rejected"; code: string; generationId?: string; sequence?: number };

export interface DiagnosticRecord {
  protocolVersion: 1;
  sequence: number;
  timestamp: number;
  code: string;
  ownerInstanceId?: string;
  sessionId?: string;
  generationId?: string;
  operationId?: string;
  consumerId?: string;
  laneId?: LifecycleLane;
  phase?: Phase;
  priorPhase?: Phase;
  newPhase?: Phase;
  outcome?: OperationOutcome;
  count?: number;
  action?: RepairAction;
  evidenceClass?: RepairEvidenceClass;
  actor?: RepairActor;
  channel?: RepairChannel;
}

export interface ContextLifecycleV1 {
  snapshot(): Snapshot;
  observe(listener: (event: LifecycleEvent) => void): { snapshot: Snapshot; unsubscribe(): void };
  requestCompaction(request: CompactRequest): CompactDisposition;
  admitWake(request: WakeAdmission, permit?: ReleasePermit): WakeDisposition;
  registerDrainer(registration: DrainerRegistration): () => void;
  repair(request: RepairRequest): RepairDisposition;
  diagnostics(): readonly DiagnosticRecord[];
}

export interface CoordinatorPublisherV1 {
  readonly protocolVersion: 1;
  requestCompaction(request: CompactRequest): CompactDisposition;
  admitWake(request: WakeAdmission, permit?: ReleasePermit): WakeDisposition;
  registerDrainer(registration: DrainerRegistration): () => void;
  repair(request: RepairRequest): RepairDisposition;
  diagnostics(): readonly DiagnosticRecord[];
}
