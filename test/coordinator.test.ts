import { describe, expect, it, vi } from "vitest";
import { ContextLifecycleCoordinatorV1, type ManagedCompactionAdapter } from "../src/coordinator.js";
import { registryForHost } from "../src/registry.js";
import type { ReleasePermit } from "../src/types.js";

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

describe("managed lifecycle coordinator", () => {
  it("starts exactly once after settled, observes success, and settles one resume before an empty release", async () => {
    const { registry, coordinator, compact, sendResume, generationId } = setup();
    const accepted = coordinator.requestSelfCompaction("reload HANDOFF.md", "tool-1");
    expect(accepted.disposition).toBe("accepted");
    expect(registry.snapshot().phase).toBe("pending-settle");
    expect(compact).not.toHaveBeenCalled();

    coordinator.onAgentSettled(generationId);
    coordinator.onAgentSettled(generationId);
    expect(compact).toHaveBeenCalledTimes(1);
    expect(registry.snapshot().phase).toBe("compacting");

    coordinator.onCompactionSuccess(generationId);
    coordinator.onCompactionSuccess(generationId);
    expect(sendResume).toHaveBeenCalledTimes(1);
    expect(registry.snapshot().phase).toBe("resuming");

    const resume = sendResume.mock.calls[0]?.[0];
    expect(resume).toContain("reload HANDOFF.md");
    coordinator.onInput(generationId, { source: "extension", text: resume ?? "" });
    coordinator.onAgentStart(generationId);
    coordinator.onAgentSettled(generationId);
    await Promise.resolve();
    expect(registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
  });

  it("joins duplicate requests and rejects stale generations", () => {
    const { coordinator, compact, generationId } = setup();
    const first = coordinator.requestSelfCompaction("", "tool-1");
    const second = coordinator.requestSelfCompaction("ignored later focus", "tool-2");
    expect(second).toMatchObject({ disposition: "joined", operationId: first.disposition === "accepted" ? first.operationId : "" });
    coordinator.onAgentSettled("stale-generation");
    expect(compact).not.toHaveBeenCalled();
    coordinator.onAgentSettled(generationId);
    expect(compact).toHaveBeenCalledTimes(1);
    expect(coordinator.diagnostics().some((entry) => entry.code === "stale-generation-callback-dropped")).toBe(true);
  });

  it("authorizes release by opaque permit identity, not copied fields", async () => {
    const { coordinator, generationId } = setup();
    let permit!: ReleasePermit;
    let finish!: () => void;
    const waiting = new Promise<void>((resolve) => { finish = resolve; });
    coordinator.registerDrainer({
      consumerId: "consumer",
      priority: 1,
      generationId,
      async drain(value) {
        permit = value;
        await waiting;
        return { releaseId: value.releaseId, consumerId: value.consumerId, disposition: "empty", submittedCount: 0 };
      },
    });
    coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId, reason: "remote" });
    coordinator.onAgentSettled(generationId);
    coordinator.onCompactionSuccess(generationId);
    expect(permit).toBeDefined();
    const wake = { consumerId: "consumer", wakeId: "wake", sessionId: "session", generationId };
    expect(coordinator.admitWake(wake, { ...permit }).disposition).toBe("hold");
    expect(coordinator.admitWake(wake, permit)).toMatchObject({ disposition: "deliver", code: "release-permit" });
    finish();
    await waiting;
    await Promise.resolve();
    expect(coordinator.admitWake(wake, permit).code).toBe("idle");
  });

  it("keeps focus and error content out of bounded diagnostics", () => {
    const { coordinator, generationId, compact } = setup();
    const secret = "secret prompt token mesh-body-123";
    coordinator.requestSelfCompaction(secret, "tool");
    coordinator.onAgentSettled(generationId);
    compact.mock.calls[0]?.[0].onError(new Error(secret));
    const encoded = JSON.stringify(coordinator.diagnostics());
    expect(encoded).not.toContain(secret);
    expect(encoded).not.toContain("mesh-body-123");
    expect(coordinator.diagnostics().length).toBeLessThanOrEqual(100);
  });

  it("invalidates permits and callbacks on disposal", () => {
    const { registry, coordinator, generationId, compact } = setup();
    expect(coordinator.dispose()).toBe(true);
    coordinator.onAgentSettled(generationId);
    expect(compact).not.toHaveBeenCalled();
    expect(registry.snapshot().registryState).toBe("unavailable");
  });
});
