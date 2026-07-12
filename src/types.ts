export const CONTEXT_LIFECYCLE_PROTOCOL_VERSION = 1 as const;
export const CONTEXT_LIFECYCLE_REGISTRY_SYMBOL = Symbol.for("yourdigitaltoolbox.pi-context-lifecycle.v1");

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
  startedAt?: number;
  lastOutcome?: OperationOutcome;
}

export interface LifecycleEvent extends Snapshot {
  event: "snapshot";
}

export interface CompactRequest {
  requestId: string;
  sessionId: string;
  generationId: string;
  reason: "self" | "remote";
  resume?: boolean;
  source?: string;
}

export type CompactDisposition =
  | { disposition: "accepted" | "joined"; operationId: string; generationId: string }
  | { disposition: "rejected"; code: string; generationId?: string };

export interface WakeAdmission {
  consumerId: string;
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

export interface ReleasePermit {
  readonly protocolVersion: 1;
  readonly sessionId: string;
  readonly generationId: string;
  readonly operationId: string;
  readonly releaseId: string;
  readonly consumerId: string;
}

export interface DrainAck {
  releaseId: string;
  consumerId: string;
  disposition: "empty" | "submitted" | "blocked";
  submittedCount: number;
}

export interface DrainerRegistration {
  consumerId: string;
  priority: number;
  generationId: string;
  drain(permit: ReleasePermit): Promise<DrainAck> | DrainAck;
}

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
  phase?: Phase;
  outcome?: OperationOutcome;
  count?: number;
}

export interface ContextLifecycleV1 {
  snapshot(): Snapshot;
  observe(listener: (event: LifecycleEvent) => void): { snapshot: Snapshot; unsubscribe(): void };
  requestCompaction(request: CompactRequest): CompactDisposition;
  admitWake(request: WakeAdmission, permit?: ReleasePermit): WakeDisposition;
  registerDrainer(registration: DrainerRegistration): () => void;
  diagnostics(): readonly DiagnosticRecord[];
}

export interface CoordinatorPublisherV1 {
  readonly protocolVersion: 1;
  requestCompaction(request: CompactRequest): CompactDisposition;
  admitWake(request: WakeAdmission, permit?: ReleasePermit): WakeDisposition;
  registerDrainer(registration: DrainerRegistration): () => void;
  diagnostics(): readonly DiagnosticRecord[];
}
