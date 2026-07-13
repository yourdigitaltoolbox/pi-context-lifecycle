import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { EXACT_CANDIDATE_SCENARIOS, runExactCandidate, type ExactCandidateCommandRunner } from "../src/testing/index.js";

const packages = [
  ["lifecycle", "@yourdigitaltoolbox/pi-context-lifecycle"],
  ["subagents", "pi-subagents"],
  ["remote", "remote-pi"],
  ["background", "pi-background-tasks"],
] as const;

async function candidateRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "pi-context-lifecycle-exact-candidate-test-"));
  await mkdir(join(root, "artifacts"));
  const artifacts = await Promise.all(packages.map(async ([id, packageName]) => {
    const archive = `artifacts/${id}.tgz`;
    const content = `archive:${packageName}`;
    await writeFile(join(root, archive), content);
    return {
      id,
      packageName,
      repository: "yourdigitaltoolbox/test",
      commit: "a".repeat(40),
      tree: "b".repeat(40),
      lockfileSha256: "c".repeat(64),
      archive,
      archiveSha256: createHash("sha256").update(content).digest("hex"),
    };
  }));
  await writeFile(join(root, "candidate-manifest.json"), JSON.stringify({
    schemaVersion: 1,
    pi: { packageName: "@earendil-works/pi-coding-agent", version: "0.80.6", integrity: `sha512-${"a".repeat(64)}` },
    scenario: { version: "slice-7", seed: 6607 },
    packageOrder: packages.map(([id]) => id),
    artifacts,
  }));
  return root;
}

function archiveInstaller(): ExactCandidateCommandRunner {
  return {
    async run(command, args, cwd) {
      if (command !== "npm") return;
      const archiveArguments = args.filter((arg) => arg.endsWith(".tgz"));
      for (const archive of archiveArguments) {
        const id = archive.split("/").at(-1)?.replace(".tgz", "");
        const packageName = packages.find(([candidateId]) => candidateId === id)?.[1];
        if (packageName === undefined) throw new Error("unexpected archive");
        const packageDirectory = join(cwd, "node_modules", ...packageName.split("/"));
        await mkdir(packageDirectory, { recursive: true });
        await writeFile(join(packageDirectory, "package.json"), JSON.stringify({ name: packageName }));
      }
    },
  };
}

describe("exact candidate runner", () => {
  it("installs only the verified ordered four archives, executes the complete matrix and bounded soak, and writes receipts outside source", async () => {
    const root = await candidateRoot();
    const seenScenarios: string[] = [];
    const seenCycles: number[] = [];
    try {
      const receipt = await runExactCandidate({
        candidateRoot: root,
        manifestPath: join(root, "candidate-manifest.json"),
        piCommand: "pi",
        cycles: 3,
        maxDurationMs: 10_000,
        writeReceipts: join(root, "receipts"),
        runner: archiveInstaller(),
        executeScenario(context) {
          seenScenarios.push(context.scenarioId);
          context.timeline.record({ type: "scenario-observed", count: 1 });
        },
        runSoakCycle(_context, cycle) { seenCycles.push(cycle); },
      });
      expect(seenScenarios).toEqual(EXACT_CANDIDATE_SCENARIOS);
      expect(seenCycles).toEqual([0, 1, 2]);
      expect(receipt.status).toBe("passed");
      expect(receipt.commands.map((entry) => [entry.command, entry.args[0], entry.args[1]])).toEqual([
        ["npm", "install", "--prefix"],
        ["pi", "install", "-l"],
        ["pi", "install", "-l"],
        ["pi", "install", "-l"],
        ["pi", "install", "-l"],
      ]);
      expect(receipt.scenarios).toHaveLength(16);
      expect(receipt.scenarios.find((entry) => entry.scenarioId === "manual-compact-characterization")?.status).toBe("passed");
      expect(JSON.parse(await readFile(join(root, "receipts", "exact-candidate-receipt.json"), "utf8"))).toMatchObject({ status: "passed", soak: { completedCycles: 3 } });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("rejects an archive outside the candidate root before invoking npm", async () => {
    const root = await candidateRoot();
    let runs = 0;
    try {
      const manifestPath = join(root, "candidate-manifest.json");
      const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as { artifacts: Array<{ archive: string }> };
      const firstArtifact = manifest.artifacts.at(0);
      if (firstArtifact === undefined) throw new Error("test manifest has no artifacts");
      firstArtifact.archive = "../outside.tgz";
      await writeFile(manifestPath, JSON.stringify(manifest));
      await expect(runExactCandidate({
        candidateRoot: root,
        manifestPath,
        piCommand: "pi",
        runner: { run() { runs += 1; return Promise.resolve(); } },
        executeScenario: () => undefined,
        runSoakCycle: () => undefined,
      })).rejects.toThrow(/relative|candidate root/i);
      expect(runs).toBe(0);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("uses documented Pi removal and removes only candidate-created project settings to restore an absent snapshot", async () => {
    const root = await candidateRoot();
    try {
      const receipt = await runExactCandidate({
        candidateRoot: root,
        manifestPath: join(root, "candidate-manifest.json"),
        piCommand: "pi",
        rollbackRehearsal: true,
        runner: {
          async run(command, args, cwd) {
            await archiveInstaller().run(command, args, cwd);
            if (command === "pi" && args[0] === "install") {
              await mkdir(join(cwd, ".pi"), { recursive: true });
              await writeFile(join(cwd, ".pi", "settings.json"), "candidate registration");
            }
          },
        },
        executeScenario: () => undefined,
        runSoakCycle: () => undefined,
      });
      expect(receipt.rollback).toMatchObject({ status: "restored", settingsExistedBeforeInstall: false, settingsExistedAfterRollback: false });
      expect(receipt.commands.filter((entry) => entry.command === "pi" && entry.args[0] === "remove")).toHaveLength(4);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
