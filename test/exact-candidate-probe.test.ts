import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { loadExactCandidateProbe } from "../src/testing/index.js";

async function createConsumerPackage(root: string, exportsTarget = "./testing.js"): Promise<string> {
  const packageDirectory = join(root, "node_modules", "pi-subagents");
  await mkdir(packageDirectory, { recursive: true });
  await writeFile(join(packageDirectory, "package.json"), JSON.stringify({
    name: "pi-subagents",
    type: "module",
    exports: { "./testing": exportsTarget },
  }));
  await writeFile(join(packageDirectory, "testing.js"), `
    export function createExactCandidateProbe() {
      return {
        consumer: "pi-subagents",
        async inject(input) { return { consumer: "pi-subagents", id: input.id, outcome: "held", notificationCount: 0 }; },
        async observations() { return []; },
        async dispose() {},
      };
    }
  `);
  return packageDirectory;
}

describe("exact candidate consumer testing subpath", () => {
  it("loads only the archive-derived package testing export and returns opaque receipts", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-context-lifecycle-probe-"));
    try {
      const packageDirectory = await createConsumerPackage(root);
      const probe = await loadExactCandidateProbe({
        packageName: "pi-subagents",
        packageDirectory,
        session: {} as AgentSession,
        seed: 6607,
      });
      await expect(probe.inject({ consumer: "pi-subagents", kind: "completion", id: "completion-1", outcome: "success" })).resolves.toEqual({
        consumer: "pi-subagents",
        id: "completion-1",
        outcome: "held",
        notificationCount: 0,
      });
      await expect(probe.observations()).resolves.toEqual([]);
      await probe.dispose();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("fails closed when a package testing export resolves outside its archive directory", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-context-lifecycle-probe-"));
    try {
      const packageDirectory = await createConsumerPackage(root, "../outside.js");
      await writeFile(join(root, "node_modules", "outside.js"), "export const createExactCandidateProbe = () => ({});");
      await expect(loadExactCandidateProbe({
        packageName: "pi-subagents",
        packageDirectory,
        session: {} as AgentSession,
        seed: 6607,
      })).rejects.toThrow(/escaped|invalid "exports" target/i);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
