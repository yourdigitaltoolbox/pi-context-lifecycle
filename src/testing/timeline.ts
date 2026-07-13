export interface TimelineEventInput {
  type: string;
  sessionId?: string;
  generationId?: string;
  operationId?: string;
  consumerId?: string;
  releaseId?: string;
  outcome?: string;
  count?: number;
  sequence?: number;
}

export interface StructuredTimelineEvent extends TimelineEventInput {
  index: number;
  timestamp: number;
  scenarioId: string;
  seed: string | number;
}

export interface StructuredTimeline {
  record(event: TimelineEventInput): StructuredTimelineEvent;
  events(): readonly Readonly<StructuredTimelineEvent>[];
}

const ALLOWED_FIELDS = new Set<keyof TimelineEventInput>([
  "type",
  "sessionId",
  "generationId",
  "operationId",
  "consumerId",
  "releaseId",
  "outcome",
  "count",
  "sequence",
]);

function optionalString(value: unknown, field: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.length === 0 || value.length > 256) throw new Error(`timeline ${field} must be a non-empty bounded string`);
  return value;
}

export function createStructuredTimeline(options: { scenarioId: string; seed: string | number; maxEvents?: number; now?: () => number }): StructuredTimeline {
  if (options.scenarioId.length === 0 || options.scenarioId.length > 128) throw new Error("timeline scenarioId must be a non-empty bounded string");
  if ((typeof options.seed !== "string" || options.seed.length === 0) && (typeof options.seed !== "number" || !Number.isSafeInteger(options.seed))) throw new Error("timeline seed must be a non-empty string or safe integer");
  const maxEvents = options.maxEvents ?? 10_000;
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > 100_000) throw new Error("timeline maxEvents must be between 1 and 100000");
  const now = options.now ?? Date.now;
  const records: Readonly<StructuredTimelineEvent>[] = [];

  return {
    record(input) {
      for (const field of Object.keys(input)) {
        if (!ALLOWED_FIELDS.has(field as keyof TimelineEventInput)) throw new Error(`unsupported timeline field: ${field}`);
      }
      if (records.length >= maxEvents) throw new Error("timeline capacity exceeded");
      if (typeof input.type !== "string" || input.type.length === 0 || input.type.length > 128) throw new Error("timeline type must be a non-empty bounded string");
      const count = input.count;
      const sequence = input.sequence;
      if (count !== undefined && (!Number.isSafeInteger(count) || count < 0)) throw new Error("timeline count must be a non-negative safe integer");
      if (sequence !== undefined && (!Number.isSafeInteger(sequence) || sequence < 0)) throw new Error("timeline sequence must be a non-negative safe integer");
      const sessionId = optionalString(input.sessionId, "sessionId");
      const generationId = optionalString(input.generationId, "generationId");
      const operationId = optionalString(input.operationId, "operationId");
      const consumerId = optionalString(input.consumerId, "consumerId");
      const releaseId = optionalString(input.releaseId, "releaseId");
      const outcome = optionalString(input.outcome, "outcome");
      const event: StructuredTimelineEvent = {
        index: records.length,
        timestamp: now(),
        scenarioId: options.scenarioId,
        seed: options.seed,
        type: input.type,
        ...(sessionId === undefined ? {} : { sessionId }),
        ...(generationId === undefined ? {} : { generationId }),
        ...(operationId === undefined ? {} : { operationId }),
        ...(consumerId === undefined ? {} : { consumerId }),
        ...(releaseId === undefined ? {} : { releaseId }),
        ...(outcome === undefined ? {} : { outcome }),
        ...(count === undefined ? {} : { count }),
        ...(sequence === undefined ? {} : { sequence }),
      };
      const frozen = Object.freeze(event);
      records.push(frozen);
      return frozen;
    },
    events() {
      return Object.freeze(records.map((event) => Object.freeze({ ...event })));
    },
  };
}
