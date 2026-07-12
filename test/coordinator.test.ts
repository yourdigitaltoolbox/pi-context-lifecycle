import { describe, expect, it, vi } from "vitest";
import { ContextLifecycleCoordinatorV1, type ManagedCompactionAdapter } from "../src/coordinator.js";
import { registryForHost } from "../src/registry.js";
import type { CompactRequest, ReleasePermit, WakeAdmission } from "../src/types.js";

function setup() {
  const registry = registryForHost({});
  const coordinator = new ContextLifecycleCoordinatorV1("owner");
  const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
  const sendResume = vi.fn<ManagedCompactionAdapter["sendResume"]>();
  const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
  coordinator.attachPublication(publication);
  const generationId = coordinator.bindSession("session", { compact, sendResume });
  return { registry, coordinator, compact, sendResume, generationId };
}

function completeManagedCompaction(test: ReturnType<typeof setup>): void {
  test.coordinator.onSessionCompact(test.generationId, "manual");
  expect(test.sendResume).not.toHaveBeenCalled();
  test.compact.mock.calls[0]?.[0].onComplete();
}

describe("managed lifecycle coordinator", () => {
  it("starts once after settled and releases only after the exact marked resume message settles", async () => {
    const test = setup();
    const accepted = test.coordinator.requestSelfCompaction("reload HANDOFF.md", "tool-1");
    expect(accepted.disposition).toBe("accepted");
    expect(test.registry.snapshot().phase).toBe("pending-settle");
    expect(test.compact).not.toHaveBeenCalled();

    test.coordinator.onAgentSettled(test.generationId);
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.compact).toHaveBeenCalledTimes(1);
    expect(test.registry.snapshot().phase).toBe("compacting");

    completeManagedCompaction(test);
    expect(test.sendResume).toHaveBeenCalledTimes(1);
    expect(test.registry.snapshot().phase).toBe("resuming");

    const resume = test.sendResume.mock.calls[0]?.[0] ?? "";
    const operationId = accepted.disposition === "accepted" ? accepted.operationId : "missing";
    expect(resume).toContain("reload HANDOFF.md");
    expect(resume).toContain(`operationId=${operationId}`);
    expect(resume).toContain(`generationId=${test.generationId}`);

    // A handled input or unrelated run has no matching user message and cannot release.
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.registry.snapshot().phase).toBe("resuming");
    test.coordinator.onMessageStart(test.generationId, { role: "assistant", content: resume });
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.registry.snapshot().phase).toBe("resuming");
    test.coordinator.onMessageStart(test.generationId, { role: "user", content: `${resume} transformed` });
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.registry.snapshot().phase).toBe("resuming");
    test.coordinator.onMessageStart(test.generationId, { role: "user", content: "unrelated user message" });
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.registry.snapshot().phase).toBe("resuming");

    test.coordinator.onMessageStart(test.generationId, { role: "user", content: resume });
    expect(test.registry.snapshot().phase).toBe("resuming");
    test.coordinator.onAgentSettled(test.generationId);
    await Promise.resolve();
    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
  });

  it("requires exactly one manual durable event before managed onComplete", () => {
    const missing = setup();
    missing.coordinator.requestSelfCompaction("", "missing");
    missing.coordinator.onAgentSettled(missing.generationId);
    missing.compact.mock.calls[0]?.[0].onComplete();
    expect(missing.registry.snapshot().phase).toBe("blocked-unknown");
    expect(missing.sendResume).not.toHaveBeenCalled();

    const automatic = setup();
    automatic.coordinator.requestSelfCompaction("", "automatic");
    automatic.coordinator.onAgentSettled(automatic.generationId);
    automatic.coordinator.onSessionCompact(automatic.generationId, "threshold");
    automatic.compact.mock.calls[0]?.[0].onComplete();
    expect(automatic.registry.snapshot().phase).toBe("blocked-unknown");
    expect(automatic.sendResume).not.toHaveBeenCalled();

    const multiple = setup();
    multiple.coordinator.requestSelfCompaction("", "multiple");
    multiple.coordinator.onAgentSettled(multiple.generationId);
    multiple.coordinator.onSessionCompact(multiple.generationId, "manual");
    multiple.coordinator.onSessionCompact(multiple.generationId, "manual");
    multiple.compact.mock.calls[0]?.[0].onComplete();
    expect(multiple.registry.snapshot().phase).toBe("blocked-unknown");
    expect(multiple.sendResume).not.toHaveBeenCalled();
  });

  it("does not send resume from a manual event until managed onComplete", () => {
    const test = setup();
    test.coordinator.requestSelfCompaction("", "tool");
    test.coordinator.onAgentSettled(test.generationId);
    test.coordinator.onSessionCompact(test.generationId, "manual");
    expect(test.registry.snapshot().phase).toBe("compacting");
    expect(test.sendResume).not.toHaveBeenCalled();
    test.coordinator.onManagedCompactionComplete(test.generationId, "different-operation");
    expect(test.registry.snapshot().phase).toBe("compacting");
    expect(test.sendResume).not.toHaveBeenCalled();
    test.compact.mock.calls[0]?.[0].onComplete();
    expect(test.registry.snapshot().phase).toBe("resuming");
    expect(test.sendResume).toHaveBeenCalledTimes(1);
  });

  it("requires generation identity and rejects omission or mismatch", () => {
    const test = setup();
    const missingRequest = { requestId: "missing", sessionId: "session", reason: "remote" } as unknown as CompactRequest;
    expect(test.coordinator.requestCompaction(missingRequest)).toEqual({ disposition: "rejected", code: "generation-required" });
    expect(test.coordinator.requestCompaction({ requestId: "stale", sessionId: "session", generationId: "stale", reason: "remote" })).toMatchObject({ disposition: "rejected", code: "generation-mismatch", generationId: test.generationId });

    const missingWake = { consumerId: "consumer", wakeId: "missing", sessionId: "session" } as unknown as WakeAdmission;
    expect(test.coordinator.admitWake(missingWake)).toEqual({ disposition: "reject", code: "generation-required" });
    expect(test.coordinator.admitWake({ consumerId: "consumer", wakeId: "stale", sessionId: "session", generationId: "stale" })).toMatchObject({ disposition: "reject", code: "generation-mismatch", generationId: test.generationId });
  });

  it("joins duplicate self requests and rejects stale callback generations", () => {
    const test = setup();
    const first = test.coordinator.requestSelfCompaction("", "tool-1");
    const second = test.coordinator.requestSelfCompaction("ignored later focus", "tool-2");
    expect(second).toMatchObject({ disposition: "joined", operationId: first.disposition === "accepted" ? first.operationId : "" });
    test.coordinator.onAgentSettled("stale-generation");
    expect(test.compact).not.toHaveBeenCalled();
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.compact).toHaveBeenCalledTimes(1);
    expect(test.coordinator.diagnostics().some((entry) => entry.code === "stale-generation-callback-dropped")).toBe(true);
  });

  it("authorizes release by opaque permit identity, not copied fields", async () => {
    const test = setup();
    let permit!: ReleasePermit;
    let finish!: () => void;
    const waiting = new Promise<void>((resolve) => { finish = resolve; });
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      priority: 1,
      generationId: test.generationId,
      async drain(value) {
        permit = value;
        await waiting;
        return { releaseId: value.releaseId, consumerId: value.consumerId, disposition: "empty", submittedCount: 0 };
      },
    });
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    expect(permit).toBeDefined();
    const wake = { consumerId: "consumer", wakeId: "wake", sessionId: "session", generationId: test.generationId };
    expect(test.coordinator.admitWake(wake, { ...permit }).disposition).toBe("hold");
    expect(test.coordinator.admitWake(wake, permit)).toMatchObject({ disposition: "deliver", code: "release-permit" });
    finish();
    await waiting;
    await Promise.resolve();
    expect(test.coordinator.admitWake(wake, permit).code).toBe("idle");
  });

  it("keeps focus and error content out of bounded diagnostics", () => {
    const test = setup();
    const secret = "secret prompt token mesh-body-123";
    test.coordinator.requestSelfCompaction(secret, "tool");
    test.coordinator.onAgentSettled(test.generationId);
    test.compact.mock.calls[0]?.[0].onError(new Error(secret));
    const encoded = JSON.stringify(test.coordinator.diagnostics());
    expect(encoded).not.toContain(secret);
    expect(encoded).not.toContain("mesh-body-123");
    expect(test.coordinator.diagnostics().length).toBeLessThanOrEqual(100);
  });

  it("invalidates permits and callbacks on disposal", () => {
    const test = setup();
    expect(test.coordinator.dispose()).toBe(true);
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.registry.snapshot().registryState).toBe("unavailable");
  });
});
