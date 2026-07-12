import { describe, expect, it, vi } from "vitest";
import { registryForHost } from "../src/registry.js";
import type { CoordinatorPublisherV1 } from "../src/types.js";

function publisher(): CoordinatorPublisherV1 {
  return {
    protocolVersion: 1,
    requestCompaction: () => ({ disposition: "rejected", code: "unused" }),
    admitWake: () => ({ disposition: "deliver", code: "unused" }),
    registerDrainer: () => () => {},
    diagnostics: () => [],
  };
}

describe("structural v1 registry", () => {
  it("publishes with compare-and-swap and fences stale owners", () => {
    const registry = registryForHost({});
    const first = registry.publish("owner-1", publisher(), { sessionId: "s", generationId: "g1", phase: "idle" });
    expect(() => registry.publish("owner-2", publisher(), {})).toThrow(/already published/);
    expect(first.dispose()).toBe(true);
    const second = registry.publish("owner-2", publisher(), { sessionId: "s", generationId: "g2", phase: "idle" });
    expect(first.dispose()).toBe(false);
    expect(first.update({ phase: "blocked-unknown" })).toBe(false);
    expect(registry.snapshot()).toMatchObject({ ownerInstanceId: "owner-2", generationId: "g2", registryState: "ready" });
    expect(second.dispose()).toBe(true);
  });

  it("emits distinct monotonic disposing and unavailable transitions", () => {
    const registry = registryForHost({});
    const publication = registry.publish("owner", publisher(), { sessionId: "s", generationId: "g", phase: "idle" });
    const sequences: number[] = [];
    const states: string[] = [];
    registry.observe((event) => { sequences.push(event.sequence); states.push(event.registryState); });
    const before = registry.snapshot().sequence;
    publication.dispose();
    expect(states).toEqual(["disposing", "unavailable"]);
    expect(sequences).toEqual([before + 1, before + 2]);
  });

  it("atomically subscribes and returns a sequenced snapshot", () => {
    const registry = registryForHost({});
    const publication = registry.publish("owner", publisher(), { sessionId: "s", generationId: "g", phase: "idle" });
    const events: number[] = [];
    const observation = registry.observe((event) => events.push(event.sequence));
    const observedSequence = observation.snapshot.sequence;
    publication.update({ sessionId: "s", generationId: "g", phase: "pending-settle", operationId: "op" });
    expect(events).toEqual([observedSequence + 1]);
    observation.unsubscribe();
    observation.unsubscribe();
    publication.update({ sessionId: "s", generationId: "g", phase: "compacting", operationId: "op" });
    expect(events).toHaveLength(1);
  });

  it("supports sequence-gap refresh and isolates throwing listeners", () => {
    const registry = registryForHost({});
    const publication = registry.publish("owner", publisher(), { phase: "idle" });
    let prior = registry.snapshot().sequence;
    let refreshed = 0;
    let ignoreOne = true;
    registry.observe((event) => {
      if (ignoreOne) { ignoreOne = false; return; }
      if (event.sequence !== prior + 1) refreshed = registry.snapshot().sequence;
      prior = event.sequence;
    });
    registry.observe(() => { throw new Error("listener secret should not propagate"); });
    expect(() => publication.update({ phase: "pending-settle", operationId: "one" })).not.toThrow();
    expect(() => publication.update({ phase: "compacting", operationId: "one" })).not.toThrow();
    expect(refreshed).toBe(registry.snapshot().sequence);
    expect(JSON.stringify(registry.diagnostics())).not.toContain("listener secret");
    expect(registry.diagnostics().some((entry) => entry.code === "listener-threw")).toBe(true);
  });

  it("returns structured fail-closed dispositions while unavailable or incompatible", () => {
    const unavailable = registryForHost({});
    expect(unavailable.requestCompaction({ requestId: "r", sessionId: "s", reason: "self" })).toEqual({ disposition: "rejected", code: "authority-unavailable" });
    expect(unavailable.admitWake({ consumerId: "c", wakeId: "w", sessionId: "s" })).toEqual({ disposition: "reject", code: "authority-unavailable" });

    const symbol = Symbol.for("yourdigitaltoolbox.pi-context-lifecycle.v1");
    const incompatible = registryForHost({ [symbol]: { protocolVersion: 2 } });
    expect(incompatible.snapshot().registryState).toBe("incompatible");
    expect(incompatible.admitWake({ consumerId: "c", wakeId: "w", sessionId: "s" }).code).toBe("incompatible");
  });

  it("delegates synchronous admission without accepting a wake body", () => {
    const admit = vi.fn(() => ({ disposition: "hold" as const, code: "active" }));
    const registry = registryForHost({});
    registry.publish("owner", { ...publisher(), admitWake: admit }, { phase: "compacting" });
    const result = registry.admitWake({ consumerId: "consumer", wakeId: "wake", sessionId: "session" });
    expect(result.disposition).toBe("hold");
    expect(admit).toHaveBeenCalledWith({ consumerId: "consumer", wakeId: "wake", sessionId: "session" }, undefined);
  });
});
