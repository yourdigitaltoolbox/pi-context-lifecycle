import {
  createFauxCore,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  type Model,
  type RegisterFauxProviderOptions,
  type SimpleStreamOptions,
} from "@earendil-works/pi-ai";

export type DeferredProviderLabel =
  | "agent-initial"
  | "agent-post-tool"
  | "compaction-history"
  | "compaction-turn"
  | "self-resume"
  | "producer-drain"
  /** The real pi-subagents pre-compaction completion turn. */
  | "pre-compaction-producer";

export type DeferredProviderCompletion = "completed" | "failed" | "cancelled";

export interface DeferredProviderCall {
  readonly label: DeferredProviderLabel;
  readonly callCount: number;
}

export interface DeferredProviderTracker {
  readonly entered: number;
  readonly completed: number;
  readonly inFlight: number;
  readonly maxInFlight: number;
  calls(): readonly Readonly<DeferredProviderCall>[];
  assertNoOverlap(): void;
}

export interface DeferredResponse {
  readonly label: DeferredProviderLabel;
  readonly call: Promise<{ context: Context; callCount: number; label: DeferredProviderLabel }>;
  /** Resolves when the faux response factory returns, before its stream necessarily settles. */
  readonly factoryReturned: Promise<{ callCount: number; label: DeferredProviderLabel }>;
  /** Resolves only when the returned public event stream reaches terminal settlement. */
  readonly completed: Promise<{ callCount: number; label: DeferredProviderLabel; outcome: DeferredProviderCompletion }>;
  release(): void;
  cancel(): void;
}

export interface DeferredResponseSequence {
  responseAt(index: number): Promise<DeferredResponse>;
}

export interface DeferredFakeProvider {
  readonly api: string;
  readonly provider: string;
  readonly models: readonly Model<string>[];
  readonly callCount: number;
  readonly tracker: DeferredProviderTracker;
  getModel(): Model<string>;
  streamSimple(model: Model<string>, context: Context, options?: SimpleStreamOptions): AssistantMessageEventStream;
  enqueue(response: AssistantMessage, options?: { label: Exclude<DeferredProviderLabel, "producer-drain"> }): DeferredResponse;
  enqueueFailure(options?: { label: Exclude<DeferredProviderLabel, "producer-drain"> }): DeferredResponse;
  /** Supplies one held response for every actual producer request, without assuming requests per ingress. */
  enqueueProducerDrain(response: AssistantMessage): DeferredResponseSequence;
  onEntry(listener: (entry: Readonly<DeferredProviderCall>) => void): () => void;
}

interface PendingDeferredRequest {
  readonly label: DeferredProviderLabel;
  callCount: number | undefined;
  readonly repeat: (() => void) | undefined;
  readonly observeEntry: (value: { context: Context; callCount: number; label: DeferredProviderLabel }) => void;
  readonly observeFactoryReturned: (value: { callCount: number; label: DeferredProviderLabel }) => void;
  readonly observeCompletion: (value: { callCount: number; label: DeferredProviderLabel; outcome: DeferredProviderCompletion }) => void;
  entered: boolean;
  finished: boolean;
  outcome: DeferredProviderCompletion;
}

class DeferredProviderCancellation extends Error {
  constructor() {
    super("deterministic deferred provider cancellation");
    this.name = "DeferredProviderCancellation";
  }
}

export function createDeferredFakeProvider(options: RegisterFauxProviderOptions = {}): DeferredFakeProvider {
  const core = createFauxCore(options);
  const calls: DeferredProviderCall[] = [];
  const pendingRequests: PendingDeferredRequest[] = [];
  const entryListeners = new Set<(entry: Readonly<DeferredProviderCall>) => void>();
  let entered = 0;
  let completed = 0;
  let inFlight = 0;
  let maxInFlight = 0;

  const tracker: DeferredProviderTracker = {
    get entered() { return entered; },
    get completed() { return completed; },
    get inFlight() { return inFlight; },
    get maxInFlight() { return maxInFlight; },
    calls: () => Object.freeze(calls.map((entry) => Object.freeze({ ...entry }))),
    assertNoOverlap() {
      if (maxInFlight > 1) throw new Error(`deferred provider observed overlapping requests: maxInFlight=${maxInFlight}`);
    },
  };

  const enterRequest = (request: PendingDeferredRequest, context: Context, callCount: number): void => {
    if (request.entered) throw new Error("deferred provider request entered more than once");
    request.entered = true;
    const entry = Object.freeze({ label: request.label, callCount });
    calls.push(entry);
    entered += 1;
    inFlight += 1;
    maxInFlight = Math.max(maxInFlight, inFlight);
    request.observeEntry({ context, callCount, label: request.label });
    for (const listener of [...entryListeners]) listener(entry);
  };

  const finishRequest = (request: PendingDeferredRequest, outcome: DeferredProviderCompletion): void => {
    if (request.finished) return;
    request.finished = true;
    if (!request.entered) throw new Error("deferred provider request settled before entry");
    const callCount = request.callCount;
    if (callCount === undefined) throw new Error("deferred provider request settled without a call count");
    inFlight -= 1;
    completed += 1;
    request.observeCompletion({ callCount, label: request.label, outcome });
  };

  const createDeferredResponse = (
    response: AssistantMessage | undefined,
    label: DeferredProviderLabel,
    outcomeAfterRelease: DeferredProviderCompletion,
    repeat: (() => void) | undefined,
  ): DeferredResponse => {
    let releaseGate!: () => void;
    let cancelGate!: () => void;
    let observeEntry!: (value: { context: Context; callCount: number; label: DeferredProviderLabel }) => void;
    let observeFactoryReturned!: (value: { callCount: number; label: DeferredProviderLabel }) => void;
    let observeCompletion!: (value: { callCount: number; label: DeferredProviderLabel; outcome: DeferredProviderCompletion }) => void;
    const gate = new Promise<void>((resolve, reject) => {
      releaseGate = resolve;
      cancelGate = () => reject(new DeferredProviderCancellation());
    });
    const call = new Promise<{ context: Context; callCount: number; label: DeferredProviderLabel }>((resolve) => { observeEntry = resolve; });
    const factoryReturned = new Promise<{ callCount: number; label: DeferredProviderLabel }>((resolve) => { observeFactoryReturned = resolve; });
    const completion = new Promise<{ callCount: number; label: DeferredProviderLabel; outcome: DeferredProviderCompletion }>((resolve) => { observeCompletion = resolve; });
    const request: PendingDeferredRequest = {
      label,
      callCount: undefined,
      repeat,
      observeEntry,
      observeFactoryReturned,
      observeCompletion,
      entered: false,
      finished: false,
      outcome: outcomeAfterRelease,
    };
    pendingRequests.push(request);
    let terminal = false;
    core.appendResponses([
      async (_context, _streamOptions, state) => {
        request.repeat?.();
        try {
          await gate;
          if (response === undefined) throw new Error("deterministic deferred provider failure");
          return response;
        } catch (error) {
          request.outcome = error instanceof DeferredProviderCancellation ? "cancelled" : "failed";
          throw error;
        } finally {
          request.observeFactoryReturned({ callCount: state.callCount, label });
        }
      },
    ]);
    return Object.freeze({
      label,
      call,
      factoryReturned,
      completed: completion,
      release() {
        if (terminal) return;
        terminal = true;
        releaseGate();
      },
      cancel() {
        if (terminal) return;
        terminal = true;
        cancelGate();
      },
    });
  };

  const facade: DeferredFakeProvider = {
    api: core.api,
    provider: options.provider ?? "faux",
    models: core.models,
    get callCount() { return core.state.callCount; },
    tracker,
    getModel: () => core.getModel(),
    streamSimple(model, context, streamOptions) {
      const request = pendingRequests.shift();
      if (request === undefined) throw new Error("deferred provider stream entered without a queued response");
      let stream: AssistantMessageEventStream;
      try {
        stream = core.streamSimple(model, context, streamOptions);
      } catch (error) {
        request.callCount = core.state.callCount;
        enterRequest(request, context, core.state.callCount);
        finishRequest(request, "failed");
        throw error;
      }
      request.callCount = core.state.callCount;
      enterRequest(request, context, core.state.callCount);
      let terminalOutcome: DeferredProviderCompletion = "failed";
      void Promise.resolve(stream.result())
        .then(
          (message) => {
            terminalOutcome = message.stopReason === "aborted"
              ? "cancelled"
              : message.stopReason === "error" && request.outcome === "completed"
                ? "failed"
                : request.outcome;
          },
          () => { terminalOutcome = "failed"; },
        )
        .finally(() => { finishRequest(request, terminalOutcome); });
      return stream;
    },
    enqueue(response, enqueueOptions = { label: "agent-initial" as const }) {
      return createDeferredResponse(response, enqueueOptions.label, "completed", undefined);
    },
    enqueueFailure(enqueueOptions = { label: "agent-initial" as const }) {
      return createDeferredResponse(undefined, enqueueOptions.label, "failed", undefined);
    },
    enqueueProducerDrain(response) {
      const responses: DeferredResponse[] = [];
      const waiters = new Set<() => void>();
      const append = (): void => {
        const deferred = createDeferredResponse(response, "producer-drain", "completed", append);
        responses.push(deferred);
        for (const waiter of waiters) waiter();
        waiters.clear();
      };
      append();
      return Object.freeze({
        async responseAt(index: number): Promise<DeferredResponse> {
          while (responses[index] === undefined) await new Promise<void>((resolve) => waiters.add(resolve));
          return responses[index];
        },
      });
    },
    onEntry(listener) {
      entryListeners.add(listener);
      return () => { entryListeners.delete(listener); };
    },
  };
  return facade;
}
