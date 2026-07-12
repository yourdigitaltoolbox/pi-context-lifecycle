import { access, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createDisposableHarnessRoots, createStructuredTimeline, runBoundedSoak, runPackagedImportSmoke, runScenario, validateCandidateManifest, withDisposableHarnessEnvironment } from "../src/testing/index.js";

describe("candidate manifest", () => {
  it("accepts only immutable relative artifact identities and a complete deterministic order", () => {
    const manifest = validateCandidateManifest({
      schemaVersion: 1,
      pi: { packageName: "@earendil-works/pi-coding-agent", version: "0.80.6", integrity: `sha512-${"a".repeat(64)}` },
      scenario: { version: "1", seed: 42 },
      packageOrder: ["lifecycle"],
      artifacts: [{
        id: "lifecycle",
        packageName: "@yourdigitaltoolbox/pi-context-lifecycle",
        repository: "yourdigitaltoolbox/pi-context-lifecycle",
        commit: "a".repeat(40),
        tree: "b".repeat(40),
        lockfileSha256: "c".repeat(64),
        archive: "pi-context-lifecycle-0.1.0.tgz",
        archiveSha256: "d".repeat(64),
      }],
    });
    expect(manifest.packageOrder).toEqual(["lifecycle"]);
    expect(Object.isFrozen(manifest)).toBe(true);
    expect(Object.isFrozen(manifest.artifacts)).toBe(true);

    expect(() => validateCandidateManifest({ ...manifest, packageOrder: ["lifecycle", "lifecycle"] })).toThrow(/package order/i);
    expect(() => validateCandidateManifest({ ...manifest, artifacts: [{ ...manifest.artifacts[0], commit: "main" }] })).toThrow(/commit/i);
    expect(() => validateCandidateManifest({ ...manifest, artifacts: [{ ...manifest.artifacts[0], archive: "/tmp/private.tgz" }] })).toThrow(/relative/i);
  });
});

describe("structured timeline", () => {
  it("records only bounded lifecycle metadata and rejects content-bearing fields", () => {
    const timeline = createStructuredTimeline({ scenarioId: "managed-failure", seed: 42, maxEvents: 2, now: () => 123 });
    timeline.record({ type: "operation-started", operationId: "operation", generationId: "generation", count: 1 });
    timeline.record({ type: "operation-failed", operationId: "operation", outcome: "failed" });
    expect(timeline.events()).toEqual([
      expect.objectContaining({ index: 0, timestamp: 123, scenarioId: "managed-failure", type: "operation-started" }),
      expect.objectContaining({ index: 1, timestamp: 123, scenarioId: "managed-failure", type: "operation-failed" }),
    ]);
    expect(() => timeline.record({ type: "overflow" })).toThrow(/capacity/i);
    expect(() => createStructuredTimeline({ scenarioId: "redaction", seed: "fixed" }).record({ type: "unsafe", prompt: "secret" } as never)).toThrow(/unsupported timeline field/i);
    expect(JSON.stringify(timeline.events())).not.toContain("secret");
  });
});

describe("scenario and soak drivers", () => {
  it("returns redacted deterministic receipts and stops a soak at the first failure", async () => {
    let now = 10;
    const passed = await runScenario({
      scenarioId: "managed-success",
      seed: 7,
      now: () => now++,
      execute({ timeline }) {
        timeline.record({ type: "operation-completed", outcome: "completed", count: 1 });
      },
    });
    expect(passed).toMatchObject({ status: "passed", eventCount: 1, seed: 7 });

    const failed = await runScenario({
      scenarioId: "managed-failure",
      seed: 8,
      execute() { throw new Error("secret provider body"); },
    });
    expect(failed).toMatchObject({ status: "failed", failureCode: "scenario-failed", eventCount: 0 });
    expect(JSON.stringify(failed)).not.toContain("secret provider body");

    const cycles: number[] = [];
    const soak = await runBoundedSoak({
      cycles: 5,
      seed: "fixed",
      maxDurationMs: 1_000,
      runCycle(cycle) {
        cycles.push(cycle);
        if (cycle === 2) throw new Error("stop");
      },
    });
    expect(soak).toMatchObject({ status: "failed", completedCycles: 2, failureCycle: 2 });
    expect(cycles).toEqual([0, 1, 2]);
  });
});

describe("packaged smoke driver", () => {
  it("hashes and installs only an archive inside disposable roots before importing it", async () => {
    const roots = await createDisposableHarnessRoots();
    try {
      const archive = join(roots.artifacts, "candidate.tgz");
      await writeFile(archive, "immutable archive fixture");
      const calls: Array<{ command: string; args: readonly string[]; cwd: string }> = [];
      const receipt = await runPackagedImportSmoke({
        roots,
        archive,
        packageName: "@yourdigitaltoolbox/pi-context-lifecycle",
        runner: { run(command, args, cwd) { calls.push({ command, args, cwd }); return Promise.resolve(); } },
      });
      expect(receipt).toMatchObject({ status: "passed", packageName: "@yourdigitaltoolbox/pi-context-lifecycle" });
      expect(receipt.archiveSha256).toMatch(/^[0-9a-f]{64}$/);
      expect(calls).toHaveLength(2);
      expect(calls[0]?.command).toBe("npm");
      expect(calls[0]?.args).toContain(archive);
      expect(calls[1]?.command).toBe(process.execPath);
      await expect(runPackagedImportSmoke({ roots, archive: "/tmp/outside.tgz", packageName: "valid", runner: { run: () => Promise.resolve() } })).rejects.toThrow(/disposable artifact root/i);
    } finally {
      await roots.cleanup();
    }
  });
});

describe("public SDK harness roots", () => {
  it("isolates and restores every environment root", async () => {
    const roots = await createDisposableHarnessRoots();
    const previous = {
      HOME: process.env.HOME,
      XDG_CACHE_HOME: process.env.XDG_CACHE_HOME,
      XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
      PI_CODING_AGENT_DIR: process.env.PI_CODING_AGENT_DIR,
    };
    try {
      await withDisposableHarnessEnvironment(roots, () => {
        expect(process.env.HOME).toBe(roots.home);
        expect(process.env.XDG_CACHE_HOME).toBe(roots.cache);
        expect(process.env.XDG_RUNTIME_DIR).toBe(roots.sockets);
        expect(process.env.PI_CODING_AGENT_DIR).toBe(roots.agentDir);
        return Promise.resolve();
      });
      expect({
        HOME: process.env.HOME,
        XDG_CACHE_HOME: process.env.XDG_CACHE_HOME,
        XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
        PI_CODING_AGENT_DIR: process.env.PI_CODING_AGENT_DIR,
      }).toEqual(previous);
    } finally {
      await roots.cleanup();
    }
    await expect(access(roots.root)).rejects.toThrow();
  });
});
