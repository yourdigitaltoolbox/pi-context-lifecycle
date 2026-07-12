import {
  createFauxCore,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  type FauxModelDefinition,
  type Model,
  type SimpleStreamOptions,
} from "@earendil-works/pi-ai";

export interface DeferredResponse {
  readonly call: Promise<{ context: Context; callCount: number }>;
  release(): void;
}

export interface DeferredFakeProvider {
  readonly api: string;
  readonly provider: string;
  readonly models: readonly Model<string>[];
  readonly callCount: number;
  getModel(): Model<string>;
  streamSimple(model: Model<string>, context: Context, options?: SimpleStreamOptions): AssistantMessageEventStream;
  enqueue(response: AssistantMessage): DeferredResponse;
}

export function createDeferredFakeProvider(options: { api?: string; provider?: string; models?: FauxModelDefinition[] } = {}): DeferredFakeProvider {
  const core = createFauxCore(options);
  const facade: DeferredFakeProvider = {
    api: core.api,
    provider: options.provider ?? "faux",
    models: core.models,
    get callCount() { return core.state.callCount; },
    getModel: () => core.getModel(),
    streamSimple: (model, context, streamOptions) => core.streamSimple(model, context, streamOptions),
    enqueue(response) {
      let release!: () => void;
      let observe!: (value: { context: Context; callCount: number }) => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      const call = new Promise<{ context: Context; callCount: number }>((resolve) => { observe = resolve; });
      core.appendResponses([
        async (context, _streamOptions, state) => {
          observe({ context, callCount: state.callCount });
          await gate;
          return response;
        },
      ]);
      let released = false;
      return {
        call,
        release() {
          if (released) return;
          released = true;
          release();
        },
      };
    },
  };
  return facade;
}
