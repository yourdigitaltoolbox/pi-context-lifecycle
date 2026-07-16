import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { ContextLifecycleCoordinatorV1, type ManagedCompactionAdapter } from "../src/coordinator.js";
import { registryForHost } from "../src/registry.js";
import { CONTEXT_LIFECYCLE_RELEASE_LANES } from "../src/types.js";
import type { CompactRequest, DrainAck, LifecycleClaim, ReleasePermit, WakeAdmission } from "../src/types.js";

const TEST_LANE = "background-notify" as const;

function setup() {
  const registry = registryForHost({});
  const coordinator = new ContextLifecycleCoordinatorV1("owner");
  const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
  const sendResume = vi.fn<ManagedCompactionAdapter["sendResume"]>();
  const appendLifecycleEntry = vi.fn<(claim: LifecycleClaim) => void>();
  const isIdle = vi.fn(() => true);
  const hasPendingMessages = vi.fn(() => false);
  const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
  coordinator.attachPublication(publication);
  const generationId = coordinator.bindSession("session", { compact, sendResume, isIdle, hasPendingMessages, appendLifecycleEntry });
  return { registry, coordinator, compact, sendResume, appendLifecycleEntry, isIdle, hasPendingMessages, generationId };
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
    expect(test.coordinator.requestSelfCompaction("too late to replace the admitted message", "late-tool").disposition).toBe("joined");

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

  it("starts an attested Remote request synchronously from the current session-start settlement proof", () => {
    const test = setup();
    const disposition = test.coordinator.requestCompaction({
      requestId: "remote-idle",
      sessionId: "session",
      generationId: test.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });

    expect(disposition).toMatchObject({ disposition: "accepted" });
    expect(test.appendLifecycleEntry.mock.calls.map(([claim]) => claim.state)).toEqual(["requested", "compacting"]);
    expect(test.compact).toHaveBeenCalledTimes(1);
    expect(test.registry.snapshot().phase).toBe("compacting");
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "remote-idle-started" }));
  });

  it("remembers a no-operation settled boundary but rejects a missing proof despite public idle", () => {
    const test = setup();
    test.coordinator.onActivity(test.generationId);
    const rejected = test.coordinator.requestCompaction({
      requestId: "no-proof",
      sessionId: "session",
      generationId: test.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });
    expect(rejected).toMatchObject({ disposition: "rejected", code: "settlement-proof-unavailable" });
    expect(test.compact).not.toHaveBeenCalled();

    test.coordinator.onAgentSettled(test.generationId);
    const accepted = test.coordinator.requestCompaction({
      requestId: "remembered-proof",
      sessionId: "session",
      generationId: test.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });
    expect(accepted).toMatchObject({ disposition: "accepted" });
    expect(test.compact).toHaveBeenCalledTimes(1);
  });

  it("waits for a genuine settled boundary when public work is active or queued", () => {
    const test = setup();
    test.isIdle.mockReturnValue(false);
    const active = test.coordinator.requestCompaction({
      requestId: "remote-active",
      sessionId: "session",
      generationId: test.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });
    expect(active).toMatchObject({ disposition: "accepted" });
    expect(test.registry.snapshot().phase).toBe("pending-settle");
    expect(test.compact).not.toHaveBeenCalled();
    test.isIdle.mockReturnValue(true);
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.compact).toHaveBeenCalledTimes(1);

    const queued = setup();
    queued.hasPendingMessages.mockReturnValue(true);
    queued.coordinator.requestCompaction({
      requestId: "remote-queued",
      sessionId: "session",
      generationId: queued.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });
    expect(queued.compact).not.toHaveBeenCalled();
    queued.hasPendingMessages.mockReturnValue(false);
    queued.coordinator.onAgentSettled(queued.generationId);
    expect(queued.compact).toHaveBeenCalledTimes(1);
  });

  it("invalidates proof for a delivered managed wake and never lets a join force-start a tool operation", () => {
    const test = setup();
    expect(test.coordinator.admitWake({ consumerId: "consumer", laneId: TEST_LANE, wakeId: "wake", sessionId: "session", generationId: test.generationId })).toMatchObject({ disposition: "deliver" });
    const rejected = test.coordinator.requestCompaction({
      requestId: "after-wake",
      sessionId: "session",
      generationId: test.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });
    expect(rejected).toMatchObject({ disposition: "rejected", code: "settlement-proof-unavailable" });

    const joined = setup();
    const tool = joined.coordinator.requestSelfCompaction("", "tool");
    const remote = joined.coordinator.requestCompaction({
      requestId: "join-tool",
      sessionId: "session",
      generationId: joined.generationId,
      reason: "remote",
      source: "remote-pi-action",
      actor: "operator",
      channel: "remote",
      settlementPolicy: "current-or-next-settled-boundary",
    });
    expect(tool).toMatchObject({ disposition: "accepted" });
    expect(remote).toMatchObject({ disposition: "joined" });
    expect(joined.compact).not.toHaveBeenCalled();
  });

  it("rejects untrusted current-boundary assertions and preserves default tool/legacy waiting semantics", () => {
    const test = setup();
    for (const request of [
      { reason: "self", source: "remote-pi-action", actor: "operator", channel: "remote" },
      { reason: "remote", source: "other", actor: "operator", channel: "remote" },
      { reason: "remote", source: "remote-pi-action", actor: "operator" },
      { reason: "remote", source: "remote-pi-action", channel: "remote" },
    ]) {
      expect(test.coordinator.requestCompaction({
        requestId: randomUUID(),
        sessionId: "session",
        generationId: test.generationId,
        settlementPolicy: "current-or-next-settled-boundary",
        ...request,
      } as CompactRequest)).toMatchObject({ disposition: "rejected", code: "invalid-start-authority" });
    }
    expect(test.coordinator.requestSelfCompaction("", "tool")).toMatchObject({ disposition: "accepted" });
    expect(test.compact).not.toHaveBeenCalled();
  });

  it("best-effort adopts native manual compaction only after its visible preflight hook", async () => {
    const test = setup();
    test.coordinator.onSessionBeforeCompact(test.generationId, "manual");
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "settlement-invalidated" }));
    expect(test.registry.snapshot()).toMatchObject({ phase: "observed-preflight", reason: "builtin" });
    expect(test.coordinator.admitWake({ consumerId: "consumer", laneId: TEST_LANE, wakeId: "wake", sessionId: "session", generationId: test.generationId }).disposition).toBe("hold");

    test.coordinator.onSessionCompact(test.generationId, "manual");
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.sendResume).not.toHaveBeenCalled();
  });

  it("adopts automatic threshold compaction, holds wakes, and releases on durable success", async () => {
    const test = setup();

    test.coordinator.onSessionBeforeCompact(test.generationId, "threshold");
    expect(test.registry.snapshot()).toMatchObject({ phase: "observed-preflight", reason: "threshold" });
    expect(test.coordinator.admitWake({ consumerId: "consumer", laneId: TEST_LANE, wakeId: "wake", sessionId: "session", generationId: test.generationId })).toMatchObject({ disposition: "hold", phase: "observed-preflight" });
    expect(test.compact).not.toHaveBeenCalled();

    test.coordinator.onSessionCompact(test.generationId, "threshold");
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(test.sendResume).not.toHaveBeenCalled();
  });

  it("adopts native automatic compaction while a managed request is pending, resumes, and releases held work", async () => {
    const test = setup();
    const drained: string[] = [];
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: 1, heldCount: 1 }),
      drain(permit) {
        drained.push("held-message");
        return { releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition: "submitted", submittedCount: 1, handledCount: 1, handledThrough: permit.cut.watermark };
      },
    });
    const accepted = test.coordinator.requestSelfCompaction("", "tool");
    const operationId = accepted.disposition === "accepted" ? accepted.operationId : "missing";
    expect(test.registry.snapshot().phase).toBe("pending-settle");

    expect(test.coordinator.onSessionBeforeCompact(test.generationId, "threshold")).toBe(operationId);
    expect(test.registry.snapshot()).toMatchObject({ phase: "observed-preflight", reason: "self" });
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.appendLifecycleEntry.mock.calls.map(([claim]) => claim.state)).toEqual(["requested", "compacting"]);

    test.coordinator.onSessionCompact(test.generationId, "threshold");
    expect(test.registry.snapshot()).toMatchObject({ phase: "pending-settle", lastOutcome: "completed" });
    expect(test.sendResume).not.toHaveBeenCalled();
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.registry.snapshot()).toMatchObject({ phase: "resuming", lastOutcome: "completed" });
    expect(test.sendResume).toHaveBeenCalledTimes(1);
    const resume = test.sendResume.mock.calls[0]?.[0] ?? "";
    test.coordinator.onMessageStart(test.generationId, { role: "user", content: resume });
    test.coordinator.onAgentSettled(test.generationId);
    // The submitted held wake starts a new Pi turn, which must settle before idle.
    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" }));
    expect(drained).toEqual(["held-message"]);
    expect(test.appendLifecycleEntry.mock.calls.map(([claim]) => claim.state)).toEqual([
      "requested", "compacting", "compacted", "resume-pending", "resume-admitting", "resume-admitted", "resume-settled", "released",
    ]);
  });

  it("terminalizes an adopted automatic compaction that settles without success", async () => {
    const test = setup();
    const drained: string[] = [];
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: 1, heldCount: 1 }),
      drain(permit) {
        drained.push("held-message");
        return { releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition: "submitted", submittedCount: 1, handledCount: 1, handledThrough: permit.cut.watermark };
      },
    });
    test.coordinator.requestSelfCompaction("", "tool");
    test.coordinator.onSessionBeforeCompact(test.generationId, "overflow");

    test.coordinator.onAgentSettled(test.generationId);
    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "failed" }));
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.sendResume).not.toHaveBeenCalled();
    expect(drained).toEqual(["held-message"]);
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "adopted-automatic-compaction-failed", outcome: "failed" }));
  });

  it("waits for settlement before releasing an adopted automatic cancellation", async () => {
    const test = setup();
    const drained: string[] = [];
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: 1, heldCount: 1 }),
      drain(permit) {
        drained.push("held-message");
        return { releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition: "submitted", submittedCount: 1, handledCount: 1, handledThrough: permit.cut.watermark };
      },
    });
    test.coordinator.requestSelfCompaction("", "tool");
    const operationId = test.coordinator.onSessionBeforeCompact(test.generationId, "threshold") ?? "missing";

    test.coordinator.onCompactionCancelled(test.generationId, operationId);
    expect(test.registry.snapshot()).toMatchObject({ phase: "pending-settle", lastOutcome: "cancelled" });
    expect(drained).toEqual([]);
    test.coordinator.onAgentSettled(test.generationId);
    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "cancelled" }));
    expect(test.sendResume).not.toHaveBeenCalled();
    expect(drained).toEqual(["held-message"]);
  });

  it("lets late durable automatic success resolve the current compaction block", async () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      test.coordinator.onSessionBeforeCompact(test.generationId, "threshold");
      vi.advanceTimersByTime(600_000);
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");

      test.coordinator.onSessionCompact(test.generationId, "threshold");
      await Promise.resolve();

      expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
      expect(test.sendResume).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("releases automatic compaction as failed when the enclosing generation settles without success", async () => {
    const test = setup();
    test.coordinator.onSessionBeforeCompact(test.generationId, "overflow");

    test.coordinator.onAgentSettled(test.generationId);
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "failed" });
    expect(test.sendResume).not.toHaveBeenCalled();
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "automatic-compaction-failed", outcome: "failed" }));
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

  it("warns at two minutes and blocks at ten without unlocking managed compaction", () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      test.coordinator.requestSelfCompaction("", "tool");
      test.coordinator.onAgentSettled(test.generationId);

      vi.advanceTimersByTime(119_999);
      expect(test.coordinator.diagnostics().some((entry) => entry.code === "compaction-attention-warning")).toBe(false);
      vi.advanceTimersByTime(1);
      expect(test.registry.snapshot().phase).toBe("compacting");
      expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "compaction-attention-warning", phase: "compacting" }));

      vi.advanceTimersByTime(479_999);
      expect(test.registry.snapshot().phase).toBe("compacting");
      vi.advanceTimersByTime(1);
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");
      expect(test.sendResume).not.toHaveBeenCalled();
      expect(test.coordinator.admitWake({ consumerId: "consumer", laneId: TEST_LANE, wakeId: "wake", sessionId: "session", generationId: test.generationId }).disposition).toBe("hold");
    } finally {
      vi.useRealTimers();
    }
  });

  it("lets late durable managed success plus owned completion resolve the current block", () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      test.coordinator.requestSelfCompaction("", "tool");
      test.coordinator.onAgentSettled(test.generationId);
      vi.advanceTimersByTime(600_000);
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");

      test.coordinator.onSessionCompact(test.generationId, "manual");
      test.compact.mock.calls[0]?.[0].onComplete();

      expect(test.registry.snapshot()).toMatchObject({ phase: "resuming", lastOutcome: "completed" });
      expect(test.sendResume).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("lets late authoritative managed failure resolve the current compaction block", async () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      test.coordinator.requestSelfCompaction("", "tool");
      test.coordinator.onAgentSettled(test.generationId);
      vi.advanceTimersByTime(600_000);
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");

      test.compact.mock.calls[0]?.[0].onError(new Error("late provider rejection"));
      await Promise.resolve();

      expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "failed" });
      expect(test.sendResume).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("releases the unchanged session after authoritative managed onError without resuming", async () => {
    const test = setup();
    test.coordinator.requestSelfCompaction("", "tool");
    test.coordinator.onAgentSettled(test.generationId);

    test.compact.mock.calls[0]?.[0].onError(new Error("provider rejected"));
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "failed" });
    expect(test.sendResume).not.toHaveBeenCalled();
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "managed-compaction-failed", outcome: "failed" }));
  });

  it("classifies authoritative managed cancellation without logging its error", async () => {
    const test = setup();
    test.coordinator.requestSelfCompaction("", "tool");
    test.coordinator.onAgentSettled(test.generationId);

    test.compact.mock.calls[0]?.[0].onError(new Error("Compaction cancelled"));
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "cancelled" });
    expect(test.sendResume).not.toHaveBeenCalled();
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "managed-compaction-cancelled", outcome: "cancelled" }));
    expect(JSON.stringify(test.coordinator.diagnostics())).not.toContain("Compaction cancelled");
  });

  it("classifies an observed automatic abort signal as cancellation", async () => {
    const test = setup();
    const operationId = test.coordinator.onSessionBeforeCompact(test.generationId, "threshold");
    expect(operationId).toEqual(expect.any(String));

    test.coordinator.onCompactionCancelled(test.generationId, operationId ?? "missing");
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "cancelled" });
    expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "automatic-compaction-cancelled", outcome: "cancelled" }));
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
    expect(test.coordinator.admitWake({ consumerId: "consumer", laneId: TEST_LANE, wakeId: "stale", sessionId: "session", generationId: "stale" })).toMatchObject({ disposition: "reject", code: "generation-mismatch", generationId: test.generationId });
  });

  it("durably merges joined resume intent and safe pre-compaction focus", () => {
    const test = setup();
    const first = test.coordinator.requestCompaction({ requestId: "remote-1", sessionId: "session", generationId: test.generationId, reason: "remote", resume: false });
    const second = test.coordinator.requestSelfCompaction("joined focus from self", "tool-2");
    expect(second).toMatchObject({ disposition: "joined", operationId: first.disposition === "accepted" ? first.operationId : "" });
    expect(test.appendLifecycleEntry).toHaveBeenCalledTimes(2);
    expect(test.appendLifecycleEntry.mock.calls[0]?.[0]).toMatchObject({ state: "requested", reason: "remote", resumeIntent: false });
    expect(test.appendLifecycleEntry.mock.calls[1]?.[0]).toMatchObject({ state: "requested", reason: "remote", resumeIntent: true });
    expect(JSON.stringify(test.appendLifecycleEntry.mock.calls)).not.toContain("joined focus from self");

    test.coordinator.onAgentSettled("stale-generation");
    expect(test.compact).not.toHaveBeenCalled();
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.compact).toHaveBeenCalledTimes(1);
    expect(test.compact.mock.calls[0]?.[0].customInstructions).toContain("joined focus from self");
    expect(test.coordinator.diagnostics().some((entry) => entry.code === "stale-generation-callback-dropped")).toBe(true);

    const restoredRegistry = registryForHost({});
    const restored = new ContextLifecycleCoordinatorV1("replacement-owner");
    const publication = restoredRegistry.publish(restored.ownerInstanceId, restored, {});
    restored.attachPublication(publication);
    restored.bindSession("session", { compact: vi.fn(), sendResume: vi.fn() });
    restored.restoreClaims(test.appendLifecycleEntry.mock.calls.map(([claim]) => claim));
    expect(restoredRegistry.snapshot()).toMatchObject({ phase: "blocked-unknown", operationId: first.disposition === "accepted" ? first.operationId : "", reason: "remote", resumeIntent: true });
  });

  it("expires a drainer permit and blocks release at five seconds without accepting a late ack", async () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      let permit!: ReleasePermit;
      let finish!: () => void;
      const waiting = new Promise<void>((resolve) => { finish = resolve; });
      test.coordinator.registerDrainer({
        consumerId: "consumer",
        laneId: TEST_LANE,
        generationId: test.generationId,
        capture: () => ({ watermark: 0, heldCount: 0 }),
        async drain(value) {
          permit = value;
          await waiting;
          return { releaseId: value.releaseId, consumerId: value.consumerId, laneId: value.laneId, disposition: "empty", submittedCount: 0, handledCount: 0, handledThrough: value.cut.watermark };
        },
      });
      test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
      test.coordinator.onAgentSettled(test.generationId);
      completeManagedCompaction(test);
      expect(test.registry.snapshot().phase).toBe("releasing");

      vi.advanceTimersByTime(4_999);
      await Promise.resolve();
      expect(test.registry.snapshot().phase).toBe("releasing");
      await vi.advanceTimersByTimeAsync(1);
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");

      const wake = { consumerId: "consumer", laneId: TEST_LANE, wakeId: "wake", sessionId: "session", generationId: test.generationId };
      expect(test.coordinator.admitWake(wake, permit).disposition).toBe("hold");
      finish();
      await waiting;
      await Promise.resolve();
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");
    } finally {
      vi.useRealTimers();
    }
  });

  it("exports and enforces the seven-lane cut while allowing one consumer to own multiple lanes", async () => {
    const test = setup();
    const drained: string[] = [];
    for (const laneId of [...CONTEXT_LIFECYCLE_RELEASE_LANES].reverse()) {
      test.coordinator.registerDrainer({
        consumerId: "multi-lane-consumer",
        laneId,
        generationId: test.generationId,
        capture: () => ({ watermark: 0, heldCount: 0 }),
        drain(permit) {
          drained.push(permit.laneId);
          return { releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition: "empty", submittedCount: 0, handledCount: 0, handledThrough: permit.cut.watermark };
        },
      });
    }
    expect(() => test.coordinator.registerDrainer({
      consumerId: "multi-lane-consumer",
      laneId: "mesh-reply",
      generationId: test.generationId,
      capture: () => ({ watermark: 0, heldCount: 0 }),
      drain: () => { throw new Error("duplicate lane must not run"); },
    })).toThrow(/already registered/);

    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("idle"));
    expect(drained).toEqual(CONTEXT_LIFECYCLE_RELEASE_LANES);
  });

  it("serializes three submitted drainers by stable lane order until each submitted turn settles", async () => {
    const test = setup();
    const drained: string[] = [];
    const lanes = ["loop-tick", "mesh-reply", "failure-attention-decision"] as const;
    for (const laneId of lanes) {
      test.coordinator.registerDrainer({
        consumerId: `consumer-${laneId}`,
        laneId,
        generationId: test.generationId,
        capture: () => ({ watermark: 1, heldCount: 1 }),
        drain(permit) {
          drained.push(permit.laneId);
          return { releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition: "submitted", submittedCount: 1, handledCount: 1, handledThrough: permit.cut.watermark };
        },
      });
    }
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    await vi.waitFor(() => expect(drained).toEqual(["failure-attention-decision"]));
    expect(test.registry.snapshot().phase).toBe("releasing");

    test.coordinator.onAgentSettled("stale-generation");
    await Promise.resolve();
    expect(drained).toEqual(["failure-attention-decision"]);
    expect(test.registry.snapshot().phase).toBe("releasing");

    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(drained).toEqual(["failure-attention-decision", "mesh-reply"]));
    expect(test.registry.snapshot().phase).toBe("releasing");
    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(drained).toEqual(["failure-attention-decision", "mesh-reply", "loop-tick"]));
    expect(test.registry.snapshot().phase).toBe("releasing");
    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("idle"));
  });

  it("accepts a same-generation settlement observed after submission but before its async acknowledgement", async () => {
    const test = setup();
    let acknowledge!: () => void;
    const drained: string[] = [];
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: 1, heldCount: 1 }),
      drain(permit) {
        drained.push("submitted");
        return new Promise<DrainAck>((resolve) => {
          acknowledge = () => resolve({ releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition: "submitted", submittedCount: 1, handledCount: 1, handledThrough: permit.cut.watermark });
        });
      },
    });
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    await vi.waitFor(() => expect(drained).toEqual(["submitted"]));

    test.coordinator.onAgentSettled(test.generationId);
    acknowledge();
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("idle"));
  });

  it.each([
    { disposition: "empty", submittedCount: 1, heldCount: 0 },
    { disposition: "submitted", submittedCount: 0, heldCount: 1 },
    { disposition: "blocked", submittedCount: 1, heldCount: 1 },
    { disposition: "unknown" as DrainAck["disposition"], submittedCount: 1, heldCount: 1 },
  ] as const)("blocks malformed $disposition drain acknowledgement counts", async ({ disposition, submittedCount, heldCount }) => {
    const test = setup();
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: heldCount, heldCount }),
      drain(permit) {
        return { releaseId: permit.releaseId, consumerId: permit.consumerId, laneId: permit.laneId, disposition, submittedCount, handledCount: heldCount, handledThrough: permit.cut.watermark };
      },
    });
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("blocked-unknown"));
  });

  it("captures one finite consumer watermark cut and does not absorb a post-cut arrival", async () => {
    const test = setup();
    const held = [{ sequence: 1, id: "A" }];
    let nextSequence = 1;
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture() {
        return { watermark: nextSequence, heldCount: held.length };
      },
      drain(permit) {
        expect(permit.cut).toEqual({ watermark: 1, heldCount: 1 });
        held.push({ sequence: ++nextSequence, id: "B" });
        const captured = held.filter((item) => item.sequence <= permit.cut.watermark);
        for (const item of captured) held.splice(held.indexOf(item), 1);
        return {
          releaseId: permit.releaseId,
          consumerId: permit.consumerId,
          laneId: permit.laneId,
          disposition: "submitted",
          submittedCount: 1,
          handledCount: captured.length,
          handledThrough: permit.cut.watermark,
        };
      },
    });
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    test.coordinator.onAgentSettled(test.generationId);
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("idle"));

    expect(held).toEqual([{ sequence: 2, id: "B" }]);
  });

  it("retries only the blocked drainer in the existing cut with a fresh permit", async () => {
    const test = setup();
    const permits: ReleasePermit[] = [];
    let attempts = 0;
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: 0, heldCount: 0 }),
      drain(permit) {
        permits.push(permit);
        attempts += 1;
        return {
          releaseId: permit.releaseId,
          consumerId: permit.consumerId,
          laneId: permit.laneId,
          disposition: attempts === 1 ? "blocked" : "empty",
          submittedCount: 0,
          handledCount: 0,
          handledThrough: permit.cut.watermark,
        };
      },
    });
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("blocked-unknown"));
    const blocked = test.registry.snapshot();

    expect(test.registry.repair({
      action: "retry-blocked-drainer",
      operationId: blocked.operationId ?? "missing",
      sessionId: "session",
      generationId: test.generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "idempotent-drainer-state",
      actor: "operator",
      channel: "command",
      consumerId: "consumer",
      laneId: TEST_LANE,
    })).toMatchObject({ disposition: "applied", action: "retry-blocked-drainer" });
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("idle"));

    expect(attempts).toBe(2);
    expect(permits[1]).not.toBe(permits[0]);
    expect(permits[1]?.releaseId).not.toBe(permits[0]?.releaseId);
  });

  it("authorizes release by opaque permit identity, not copied fields", async () => {
    const test = setup();
    let permit!: ReleasePermit;
    let finish!: () => void;
    const waiting = new Promise<void>((resolve) => { finish = resolve; });
    test.coordinator.registerDrainer({
      consumerId: "consumer",
      laneId: TEST_LANE,
      generationId: test.generationId,
      capture: () => ({ watermark: 0, heldCount: 0 }),
      async drain(value) {
        permit = value;
        await waiting;
        return { releaseId: value.releaseId, consumerId: value.consumerId, laneId: value.laneId, disposition: "empty", submittedCount: 0, handledCount: 0, handledThrough: value.cut.watermark };
      },
    });
    test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    expect(permit).toBeDefined();
    const wake = { consumerId: "consumer", laneId: TEST_LANE, wakeId: "wake", sessionId: "session", generationId: test.generationId };
    expect(test.coordinator.admitWake(wake, { ...permit }).disposition).toBe("hold");
    expect(test.coordinator.admitWake(wake, permit)).toMatchObject({ disposition: "deliver", code: "release-permit" });
    finish();
    await waiting;
    await vi.waitFor(() => expect(test.registry.snapshot().phase).toBe("idle"));
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

  it("warns at thirty seconds and blocks at sixty without resending an unobserved resume", () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      test.coordinator.requestSelfCompaction("", "tool");
      test.coordinator.onAgentSettled(test.generationId);
      completeManagedCompaction(test);
      expect(test.sendResume).toHaveBeenCalledTimes(1);

      vi.advanceTimersByTime(29_999);
      expect(test.coordinator.diagnostics().some((entry) => entry.code === "resume-admission-attention-warning")).toBe(false);
      vi.advanceTimersByTime(1);
      expect(test.registry.snapshot().phase).toBe("resuming");
      expect(test.coordinator.diagnostics()).toContainEqual(expect.objectContaining({ code: "resume-admission-attention-warning", phase: "resuming" }));

      vi.advanceTimersByTime(29_999);
      expect(test.registry.snapshot().phase).toBe("resuming");
      vi.advanceTimersByTime(1);
      expect(test.registry.snapshot().phase).toBe("blocked-unknown");
      expect(test.sendResume).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects caller-asserted quiescence for an ambiguous resume in the current owner process", async () => {
    const test = setup();
    const accepted = test.coordinator.requestSelfCompaction("", "tool");
    test.coordinator.onAgentSettled(test.generationId);
    completeManagedCompaction(test);
    expect(test.registry.snapshot().phase).toBe("resuming");

    const operationId = accepted.disposition === "accepted" ? accepted.operationId : "missing";
    test.coordinator.onResumeAdmissionDeadline(test.generationId, operationId);
    const blocked = test.registry.snapshot();
    expect(blocked).toMatchObject({ phase: "blocked-unknown", operationId, lastOutcome: "completed" });

    expect(test.registry.repair({
      action: "abandon-ambiguous-resume",
      operationId,
      sessionId: "session",
      generationId: "stale-generation",
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "current-process-quiescent",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "rejected", code: "generation-mismatch", generationId: test.generationId });
    expect(test.registry.snapshot().phase).toBe("blocked-unknown");

    expect(test.registry.repair({
      action: "abandon-ambiguous-resume",
      operationId,
      sessionId: "session",
      generationId: test.generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "current-process-quiescent",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "rejected", code: "repair-not-applicable" });
    await Promise.resolve();

    expect(test.registry.snapshot()).toMatchObject({ phase: "blocked-unknown", lastOutcome: "completed" });
    expect(test.sendResume).toHaveBeenCalledTimes(1);
  });

  it.each(["requested", "compacting", "compacted", "resume-pending", "resume-admitting", "resume-admitted", "resume-settled", "blocked-unknown"] as const)("restores nonterminal %s from an old owner as blocked in a fresh generation", (state) => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    const generationId = coordinator.bindSession("session", { compact: vi.fn(), sendResume: vi.fn() });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state,
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);

    expect(registry.snapshot()).toMatchObject({ phase: "blocked-unknown", operationId: "old-operation", generationId });
    expect(generationId).not.toBe("old-generation");
  });

  it.each(["released", "failed", "cancelled"] as const)("does not restore terminal %s claims", (state) => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    coordinator.bindSession("session", { compact: vi.fn(), sendResume: vi.fn() });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state,
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);

    expect(registry.snapshot()).toMatchObject({ phase: "idle" });
    expect(registry.snapshot().operationId).toBeUndefined();
  });

  it("restores an ambiguous old-owner resume claim and abandons it without resend by exact CAS repair", async () => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
    const sendResume = vi.fn<ManagedCompactionAdapter["sendResume"]>();
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    const generationId = coordinator.bindSession("session", { compact, sendResume });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state: "resume-admitting",
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);
    const blocked = registry.snapshot();
    expect(blocked).toMatchObject({ phase: "blocked-unknown", operationId: "old-operation", generationId });

    expect(registry.repair({
      action: "abandon-ambiguous-resume",
      operationId: "old-operation",
      sessionId: "session",
      generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "owner-process-replaced",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "applied", action: "abandon-ambiguous-resume" });
    await Promise.resolve();

    expect(registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(sendResume).not.toHaveBeenCalled();
    expect(coordinator.diagnostics()).toContainEqual(expect.objectContaining({
      code: "repair-applied",
      evidenceClass: "owner-process-replaced",
      priorPhase: "blocked-unknown",
      newPhase: "releasing",
    }));
  });

  it("recognizes a persisted settled resume run and releases without another admission", async () => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
    const sendResume = vi.fn<ManagedCompactionAdapter["sendResume"]>();
    const claims: string[] = [];
    const verifyRepairEvidence = vi.fn(() => true);
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    const generationId = coordinator.bindSession("session", { compact, sendResume, appendLifecycleEntry: (claim) => claims.push(claim.state), verifyRepairEvidence });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state: "resume-admitting",
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);
    const blocked = registry.snapshot();

    expect(registry.repair({
      action: "recognize-resume-admitted",
      operationId: "old-operation",
      sessionId: "session",
      generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "persisted-resume-run-settled",
      evidenceEntryId: "persisted-resume-entry",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "applied", action: "recognize-resume-admitted" });
    await Promise.resolve();

    expect(registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "completed" });
    expect(sendResume).not.toHaveBeenCalled();
    expect(verifyRepairEvidence).toHaveBeenCalledTimes(1);
    expect(claims).toEqual(["resume-admitted", "resume-settled", "released"]);
  });

  it("retries a restored resume-pending claim exactly once before any admission attempt", () => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
    const sendResume = vi.fn<ManagedCompactionAdapter["sendResume"]>();
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    const generationId = coordinator.bindSession("session", { compact, sendResume });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state: "resume-pending",
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);
    const blocked = registry.snapshot();

    expect(registry.repair({
      action: "retry-resume-pending",
      operationId: "old-operation",
      sessionId: "session",
      generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "no-admission-attempt",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "applied", action: "retry-resume-pending" });

    expect(registry.snapshot().phase).toBe("resuming");
    expect(sendResume).toHaveBeenCalledTimes(1);
    expect(sendResume.mock.calls[0]?.[0]).toContain("old-operation");
    expect(sendResume.mock.calls[0]?.[0]).toContain(generationId);
    expect(registry.repair({
      action: "retry-resume-pending",
      operationId: "old-operation",
      sessionId: "session",
      generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "no-admission-attempt",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "rejected" });
    expect(sendResume).toHaveBeenCalledTimes(1);
  });

  it("abandons a restored never-started operation after branch-validated owner replacement", async () => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const compact = vi.fn<ManagedCompactionAdapter["compact"]>();
    const sendResume = vi.fn<ManagedCompactionAdapter["sendResume"]>();
    const claims: string[] = [];
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    const generationId = coordinator.bindSession("session", { compact, sendResume, appendLifecycleEntry: (claim) => claims.push(claim.state) });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state: "requested",
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);
    const blocked = registry.snapshot();

    expect(registry.repair({
      action: "abandon-interrupted-operation",
      operationId: "old-operation",
      sessionId: "session",
      generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "branch-validated-owner-replaced",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "applied", action: "abandon-interrupted-operation" });
    await Promise.resolve();

    expect(registry.snapshot()).toMatchObject({ phase: "idle", lastOutcome: "cancelled" });
    expect(compact).not.toHaveBeenCalled();
    expect(sendResume).not.toHaveBeenCalled();
    expect(claims).toEqual(["cancelled", "released"]);
  });

  it("requires fresh-session recovery instead of abandoning a restored operation that had started", () => {
    const registry = registryForHost({});
    const coordinator = new ContextLifecycleCoordinatorV1("replacement-owner");
    const publication = registry.publish(coordinator.ownerInstanceId, coordinator, {});
    coordinator.attachPublication(publication);
    const generationId = coordinator.bindSession("session", { compact: vi.fn(), sendResume: vi.fn() });
    coordinator.restoreClaims([{
      schemaVersion: 1,
      ownerInstanceId: "old-owner",
      originOwnerInstanceId: "old-owner",
      operationId: "old-operation",
      sessionId: "session",
      generationId: "old-generation",
      state: "compacting",
      reason: "self",
      resumeIntent: true,
      timestamp: 1,
    }]);
    const blocked = registry.snapshot();

    expect(registry.repair({
      action: "abandon-interrupted-operation",
      operationId: "old-operation",
      sessionId: "session",
      generationId,
      expectedPhase: "blocked-unknown",
      expectedSequence: blocked.sequence,
      evidenceClass: "branch-validated-owner-replaced",
      actor: "operator",
      channel: "command",
    })).toMatchObject({ disposition: "rejected", code: "fresh-session-required" });
    expect(registry.snapshot()).toMatchObject({ phase: "blocked-unknown", operationId: "old-operation" });
  });

  it("keeps a disposed generation inert when an old drainer deadline or ack arrives", async () => {
    vi.useFakeTimers();
    try {
      const test = setup();
      let finish!: () => void;
      const waiting = new Promise<void>((resolve) => { finish = resolve; });
      test.coordinator.registerDrainer({
        consumerId: "consumer",
        laneId: TEST_LANE,
        generationId: test.generationId,
        capture: () => ({ watermark: 0, heldCount: 0 }),
        async drain(value) {
          await waiting;
          return { releaseId: value.releaseId, consumerId: value.consumerId, laneId: value.laneId, disposition: "empty", submittedCount: 0, handledCount: 0, handledThrough: value.cut.watermark };
        },
      });
      test.coordinator.requestCompaction({ requestId: "remote", sessionId: "session", generationId: test.generationId, reason: "remote" });
      test.coordinator.onAgentSettled(test.generationId);
      completeManagedCompaction(test);
      expect(test.registry.snapshot().phase).toBe("releasing");

      expect(test.coordinator.dispose()).toBe(true);
      await vi.advanceTimersByTimeAsync(5_000);
      finish();
      await waiting;
      await Promise.resolve();

      expect(test.registry.snapshot().registryState).toBe("unavailable");
      expect(test.coordinator.diagnostics().some((entry) => entry.code === "drainer-deadline" || entry.code === "release-completed")).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("invalidates permits and callbacks on disposal", () => {
    const test = setup();
    expect(test.coordinator.dispose()).toBe(true);
    test.coordinator.onAgentSettled(test.generationId);
    expect(test.compact).not.toHaveBeenCalled();
    expect(test.registry.snapshot().registryState).toBe("unavailable");
  });
});
