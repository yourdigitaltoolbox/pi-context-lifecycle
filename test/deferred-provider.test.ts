import { describe, expect, it } from "vitest";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { createDeferredFakeProvider } from "../src/testing/deferred-provider.js";

const context = { messages: [] };

async function result(provider: ReturnType<typeof createDeferredFakeProvider>) {
  return provider.streamSimple(provider.getModel(), context).result();
}

describe("deferred fake provider tracker", () => {
  it("tracks the complete normal deferred request lifetime", async () => {
    const provider = createDeferredFakeProvider();
    const response = provider.enqueue(fauxAssistantMessage("normal"), { label: "agent-initial" });
    const pending = result(provider);
    await response.call;
    expect(provider.tracker).toMatchObject({ entered: 1, completed: 0, inFlight: 1, maxInFlight: 1 });
    response.release();
    await pending;
    await expect(response.completed).resolves.toMatchObject({ label: "agent-initial", outcome: "completed" });
    expect(provider.tracker).toMatchObject({ entered: 1, completed: 1, inFlight: 0, maxInFlight: 1 });
  });

  it("decrements after failure and cancellation through finally", async () => {
    const provider = createDeferredFakeProvider();
    const failure = provider.enqueueFailure({ label: "compaction-history" });
    const cancellation = provider.enqueue(fauxAssistantMessage("unused"), { label: "compaction-turn" });
    const failed = result(provider);
    await failure.call;
    failure.release();
    await failed;
    await expect(failure.completed).resolves.toMatchObject({ outcome: "failed" });

    const cancelled = result(provider);
    await cancellation.call;
    cancellation.cancel();
    await cancelled;
    await expect(cancellation.completed).resolves.toMatchObject({ outcome: "cancelled" });
    expect(provider.tracker).toMatchObject({ entered: 2, completed: 2, inFlight: 0, maxInFlight: 1 });
  });

  it("keeps a released response active until its returned stream reaches terminal settlement", async () => {
    const provider = createDeferredFakeProvider({ tokensPerSecond: 100, tokenSize: { min: 1, max: 1 } });
    const first = provider.enqueue(fauxAssistantMessage("x".repeat(80)), { label: "agent-initial" });
    const second = provider.enqueue(fauxAssistantMessage("two"), { label: "agent-post-tool" });
    const firstStream = provider.streamSimple(provider.getModel(), context);
    await first.call;
    first.release();
    // This is the old tracker completion boundary: the response factory has
    // returned, while the deliberately long returned stream is still active.
    await first.factoryReturned;
    let firstStreamSettled = false;
    void firstStream.result().then(() => { firstStreamSettled = true; });

    const secondStream = provider.streamSimple(provider.getModel(), context);
    await second.call;
    expect(firstStreamSettled).toBe(false);
    expect(provider.tracker).toMatchObject({ entered: 2, completed: 0, inFlight: 2, maxInFlight: 2 });
    expect(() => provider.tracker.assertNoOverlap()).toThrow(/overlapping/);

    second.release();
    await Promise.all([firstStream.result(), secondStream.result(), first.completed, second.completed]);
    expect(provider.tracker).toMatchObject({ inFlight: 0, completed: 2, maxInFlight: 2 });
  });
});
