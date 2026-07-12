import {
  CONTEXT_LIFECYCLE_PROTOCOL_VERSION,
  CONTEXT_LIFECYCLE_REGISTRY_SYMBOL,
  type CompactDisposition,
  type CompactRequest,
  type ContextLifecycleV1,
  type CoordinatorPublisherV1,
  type DiagnosticRecord,
  type DrainerRegistration,
  type LifecycleEvent,
  type ReleasePermit,
  type RepairDisposition,
  type RepairRequest,
  type Snapshot,
  type WakeAdmission,
  type WakeDisposition,
} from "./types.js";

const MAX_DIAGNOSTICS = 100;
type Listener = (event: LifecycleEvent) => void;
type RegistryHost = Record<PropertyKey, unknown>;

export interface CoordinatorPublicationV1 {
  ownerInstanceId: string;
  update(snapshot: Omit<Snapshot, "protocolVersion" | "registryState" | "sequence" | "ownerInstanceId">): boolean;
  dispose(): boolean;
}

export interface StructuralRegistryV1 extends ContextLifecycleV1 {
  readonly protocolVersion: 1;
  publish(ownerInstanceId: string, publisher: CoordinatorPublisherV1, initial: Omit<Snapshot, "protocolVersion" | "registryState" | "sequence" | "ownerInstanceId">): CoordinatorPublicationV1;
}

function cloneSnapshot(snapshot: Snapshot): Snapshot {
  return { ...snapshot };
}

function createRegistry(): StructuralRegistryV1 {
  let snapshot: Snapshot = { protocolVersion: 1, registryState: "unavailable", sequence: 0 };
  let owner: { id: string; publisher: CoordinatorPublisherV1 } | undefined;
  const listeners = new Set<Listener>();
  const localDiagnostics: DiagnosticRecord[] = [];

  const diagnose = (code: string): void => {
    localDiagnostics.push({
      protocolVersion: 1,
      sequence: snapshot.sequence,
      timestamp: Date.now(),
      code,
      ...(snapshot.ownerInstanceId === undefined ? {} : { ownerInstanceId: snapshot.ownerInstanceId }),
      ...(snapshot.sessionId === undefined ? {} : { sessionId: snapshot.sessionId }),
      ...(snapshot.generationId === undefined ? {} : { generationId: snapshot.generationId }),
      ...(snapshot.operationId === undefined ? {} : { operationId: snapshot.operationId }),
      ...(snapshot.phase === undefined ? {} : { phase: snapshot.phase }),
    });
    if (localDiagnostics.length > MAX_DIAGNOSTICS) localDiagnostics.splice(0, localDiagnostics.length - MAX_DIAGNOSTICS);
  };

  const emit = (next: Omit<Snapshot, "protocolVersion" | "sequence">): void => {
    snapshot = { protocolVersion: 1, sequence: snapshot.sequence + 1, ...next };
    const event: LifecycleEvent = { ...snapshot, event: "snapshot" };
    for (const listener of [...listeners]) {
      try {
        listener(event);
      } catch {
        diagnose("listener-threw");
      }
    }
  };

  const registry: StructuralRegistryV1 = {
    protocolVersion: CONTEXT_LIFECYCLE_PROTOCOL_VERSION,
    snapshot: () => cloneSnapshot(snapshot),
    observe(listener) {
      listeners.add(listener);
      let subscribed = true;
      return {
        snapshot: cloneSnapshot(snapshot),
        unsubscribe() {
          if (!subscribed) return;
          subscribed = false;
          listeners.delete(listener);
        },
      };
    },
    requestCompaction(request): CompactDisposition {
      if (typeof request.generationId !== "string" || request.generationId.length === 0) return { disposition: "rejected", code: "generation-required" };
      if (snapshot.generationId !== undefined && request.generationId !== snapshot.generationId) return { disposition: "rejected", code: "generation-mismatch", generationId: snapshot.generationId };
      if (!owner || snapshot.registryState !== "ready") {
        return { disposition: "rejected", code: snapshot.registryState === "incompatible" ? "incompatible" : "authority-unavailable", ...(snapshot.generationId === undefined ? {} : { generationId: snapshot.generationId }) };
      }
      return owner.publisher.requestCompaction(request);
    },
    admitWake(request, permit): WakeDisposition {
      if (typeof request.generationId !== "string" || request.generationId.length === 0) return { disposition: "reject", code: "generation-required" };
      if (snapshot.generationId !== undefined && request.generationId !== snapshot.generationId) return { disposition: "reject", code: "generation-mismatch", generationId: snapshot.generationId, ...(snapshot.phase === undefined ? {} : { phase: snapshot.phase }), ...(snapshot.operationId === undefined ? {} : { operationId: snapshot.operationId }) };
      if (!owner || snapshot.registryState !== "ready") {
        return { disposition: "reject", code: snapshot.registryState === "incompatible" ? "incompatible" : "authority-unavailable", ...(snapshot.phase === undefined ? {} : { phase: snapshot.phase }), ...(snapshot.generationId === undefined ? {} : { generationId: snapshot.generationId }) };
      }
      return owner.publisher.admitWake(request, permit);
    },
    registerDrainer(registration: DrainerRegistration): () => void {
      if (!owner || snapshot.registryState !== "ready") throw new Error("Context lifecycle authority unavailable");
      return owner.publisher.registerDrainer(registration);
    },
    repair(request: RepairRequest): RepairDisposition {
      const rejected = (code: string): RepairDisposition => ({ disposition: "rejected", code, ...(snapshot.generationId === undefined ? {} : { generationId: snapshot.generationId }), sequence: snapshot.sequence });
      if (typeof request.generationId !== "string" || request.generationId.length === 0) return { disposition: "rejected", code: "generation-required" };
      if (snapshot.generationId !== undefined && request.generationId !== snapshot.generationId) return rejected("generation-mismatch");
      if (!owner || snapshot.registryState !== "ready") return rejected(snapshot.registryState === "incompatible" ? "incompatible" : "authority-unavailable");
      if (request.expectedSequence !== snapshot.sequence) return rejected("snapshot-sequence-mismatch");
      if (request.sessionId !== snapshot.sessionId) return rejected("session-mismatch");
      if (request.operationId !== snapshot.operationId) return rejected("operation-mismatch");
      if (request.expectedPhase !== snapshot.phase) return rejected("phase-mismatch");
      return owner.publisher.repair(request);
    },
    diagnostics(): readonly DiagnosticRecord[] {
      return [...localDiagnostics, ...(owner?.publisher.diagnostics() ?? [])].slice(-MAX_DIAGNOSTICS).map((record) => ({ ...record }));
    },
    publish(ownerInstanceId, publisher, initial): CoordinatorPublicationV1 {
      if (owner !== undefined) {
        diagnose("duplicate-owner-rejected");
        throw new Error(`Context lifecycle v1 owner already published: ${owner.id}`);
      }
      if ((publisher as { protocolVersion: unknown }).protocolVersion !== 1) throw new Error("Context lifecycle publisher protocol mismatch");
      owner = { id: ownerInstanceId, publisher };
      emit({ registryState: "ready", ownerInstanceId, ...initial });
      let live = true;
      return {
        ownerInstanceId,
        update(next) {
          if (!live || owner?.id !== ownerInstanceId) {
            diagnose("stale-owner-update-dropped");
            return false;
          }
          emit({ registryState: "ready", ownerInstanceId, ...next });
          return true;
        },
        dispose() {
          if (!live || owner?.id !== ownerInstanceId) {
            diagnose("stale-owner-dispose-dropped");
            return false;
          }
          live = false;
          emit({
            registryState: "disposing",
            ownerInstanceId,
            ...(snapshot.sessionId === undefined ? {} : { sessionId: snapshot.sessionId }),
            ...(snapshot.generationId === undefined ? {} : { generationId: snapshot.generationId }),
            ...(snapshot.phase === undefined ? {} : { phase: snapshot.phase }),
            ...(snapshot.operationId === undefined ? {} : { operationId: snapshot.operationId }),
            ...(snapshot.reason === undefined ? {} : { reason: snapshot.reason }),
            ...(snapshot.startedAt === undefined ? {} : { startedAt: snapshot.startedAt }),
            ...(snapshot.lastOutcome === undefined ? {} : { lastOutcome: snapshot.lastOutcome }),
          });
          owner = undefined;
          emit({ registryState: "unavailable" });
          return true;
        },
      };
    },
  };
  return registry;
}

function isStructuralRegistry(value: unknown): value is StructuralRegistryV1 {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<StructuralRegistryV1>;
  return candidate.protocolVersion === 1
    && typeof candidate.snapshot === "function"
    && typeof candidate.observe === "function"
    && typeof candidate.requestCompaction === "function"
    && typeof candidate.admitWake === "function"
    && typeof candidate.registerDrainer === "function"
    && typeof candidate.repair === "function"
    && typeof candidate.diagnostics === "function"
    && typeof candidate.publish === "function";
}

export function registryForHost(host: RegistryHost): StructuralRegistryV1 {
  const existing = host[CONTEXT_LIFECYCLE_REGISTRY_SYMBOL];
  if (existing === undefined) {
    const registry = createRegistry();
    Object.defineProperty(host, CONTEXT_LIFECYCLE_REGISTRY_SYMBOL, { value: registry, configurable: true, writable: false });
    return registry;
  }
  if (isStructuralRegistry(existing)) return existing;
  const incompatibleSnapshot: Snapshot = { protocolVersion: 1, registryState: "incompatible", sequence: 0 };
  return {
    protocolVersion: 1,
    snapshot: () => cloneSnapshot(incompatibleSnapshot),
    observe: () => ({ snapshot: cloneSnapshot(incompatibleSnapshot), unsubscribe() { /* static incompatible authority */ } }),
    requestCompaction: (request) => typeof request.generationId !== "string" || request.generationId.length === 0
      ? { disposition: "rejected", code: "generation-required" }
      : { disposition: "rejected", code: "incompatible" },
    admitWake: (request) => typeof request.generationId !== "string" || request.generationId.length === 0
      ? { disposition: "reject", code: "generation-required" }
      : { disposition: "reject", code: "incompatible" },
    registerDrainer: () => { throw new Error("Incompatible context lifecycle registry"); },
    repair: (request) => typeof request.generationId !== "string" || request.generationId.length === 0
      ? { disposition: "rejected", code: "generation-required" }
      : { disposition: "rejected", code: "incompatible" },
    diagnostics: () => [],
    publish: () => { throw new Error("Incompatible context lifecycle registry"); },
  };
}

const globalRegistry = (): StructuralRegistryV1 => registryForHost(globalThis);
export const getContextLifecycleSnapshotV1 = (): Snapshot => globalRegistry().snapshot();
export const observeContextLifecycleV1 = (listener: Listener): ReturnType<ContextLifecycleV1["observe"]> => globalRegistry().observe(listener);
export const requestCompaction = (request: CompactRequest): CompactDisposition => globalRegistry().requestCompaction(request);
export const admitWake = (request: WakeAdmission, permit?: ReleasePermit): WakeDisposition => globalRegistry().admitWake(request, permit);
export const registerContextLifecycleDrainerV1 = (registration: DrainerRegistration): (() => void) => globalRegistry().registerDrainer(registration);
export const repairContextLifecycleV1 = (request: RepairRequest): RepairDisposition => globalRegistry().repair(request);
export const getContextLifecycleDiagnosticsV1 = (): readonly DiagnosticRecord[] => globalRegistry().diagnostics();
export const publishContextLifecycleV1 = (ownerInstanceId: string, publisher: CoordinatorPublisherV1, initial: Omit<Snapshot, "protocolVersion" | "registryState" | "sequence" | "ownerInstanceId">): CoordinatorPublicationV1 => globalRegistry().publish(ownerInstanceId, publisher, initial);
