import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { access, mkdir, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import { join, relative, resolve, sep } from "node:path";
import { promisify } from "node:util";
import { validateCandidateManifest, type CandidateArtifact, type CandidateManifest } from "./candidate-manifest.js";
import { createDisposableHarnessRoots, withDisposableHarnessEnvironment, type DisposableHarnessRoots } from "./disposable-roots.js";
import { runBoundedSoak, runScenario, type ScenarioContext, type ScenarioReceipt, type SoakReceipt } from "./scenario-driver.js";
import { createStructuredTimeline } from "./timeline.js";

const execFileAsync = promisify(execFile);

const EXACT_PACKAGES = [
  "@yourdigitaltoolbox/pi-context-lifecycle",
  "pi-subagents",
  "remote-pi",
  "pi-background-tasks",
] as const;

export const EXACT_CANDIDATE_SCENARIOS = [
  "tool-multi-tool",
  "tool-alias-join",
  "remote-owner-join",
  "subagent-before-during-after",
  "mesh-during-compaction",
  "background-due-during-compaction",
  "all-producers-concurrent",
  "slow-provider-success",
  "provider-failure",
  "compaction-cancelled",
  "reload-replacement-repair",
  "automatic-threshold-overflow",
  "manual-compact-characterization",
  "automatic-prehook-race",
  "resume-admission-barrier",
  "large-context-reduction",
] as const;

export type ExactCandidateScenarioId = typeof EXACT_CANDIDATE_SCENARIOS[number];

export interface ExactCandidateCommand {
  command: string;
  args: readonly string[];
  cwd: string;
}

export interface ExactCandidateCommandRunner {
  run(command: string, args: readonly string[], cwd: string): Promise<void>;
}

const defaultRunner: ExactCandidateCommandRunner = {
  async run(command, args, cwd) {
    await execFileAsync(command, [...args], {
      cwd,
      env: {
        ...process.env,
        npm_config_audit: "false",
        npm_config_fund: "false",
        npm_config_update_notifier: "false",
        PI_OFFLINE: "1",
        PI_TELEMETRY: "0",
      },
      maxBuffer: 10 * 1024 * 1024,
    });
  },
};

export interface ExactCandidateScenarioContext extends ScenarioContext {
  candidateRoot: string;
  runtimeRoot: string;
  packageDirectories: Readonly<Record<string, string>>;
  roots: DisposableHarnessRoots;
  manualCharacterization: boolean;
}

export interface ExactCandidateReceipt {
  schemaVersion: 1;
  manifestSha256: string;
  candidateRoot: string;
  runtimeRoot: string;
  packageOrder: readonly string[];
  scenarioVersion: string;
  archiveSha256: Readonly<Record<string, string>>;
  packageDirectories: Readonly<Record<string, string>>;
  commands: readonly Readonly<ExactCandidateCommand>[];
  scenarios: readonly Readonly<ScenarioReceipt>[];
  soak: Readonly<SoakReceipt>;
  rollback?: Readonly<ExactCandidateRollbackReceipt>;
  status: "passed";
}

export interface ExactCandidateRollbackReceipt {
  settingsExistedBeforeInstall: boolean;
  settingsSha256BeforeInstall?: string;
  settingsExistedAfterRollback: boolean;
  settingsSha256AfterRollback?: string;
  status: "restored";
}

function containedPath(root: string, path: string, label: string): string {
  const resolvedRoot = resolve(root);
  const resolvedPath = resolve(path);
  const relativePath = relative(resolvedRoot, resolvedPath);
  if (relativePath === "" || relativePath === ".." || relativePath.startsWith(`..${sep}`) || relativePath.includes(`${sep}..${sep}`)) throw new Error(`${label} must be inside candidate root`);
  return resolvedPath;
}

async function fileSha256(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

function packageDirectory(runtimeRoot: string, packageName: string): string {
  return join(runtimeRoot, "node_modules", ...packageName.split("/"));
}

function assertExactFourPackageManifest(manifest: Readonly<CandidateManifest>): void {
  if (manifest.artifacts.length !== EXACT_PACKAGES.length) throw new Error("exact candidate manifest must contain exactly four artifacts");
  const names = manifest.artifacts.map((artifact) => artifact.packageName);
  if (new Set(names).size !== EXACT_PACKAGES.length || EXACT_PACKAGES.some((packageName) => !names.includes(packageName))) {
    throw new Error("exact candidate manifest must contain the lifecycle, pi-subagents, remote-pi, and background archives");
  }
  const orderedNames = manifest.packageOrder.map((id) => manifest.artifacts.find((artifact) => artifact.id === id)?.packageName);
  if (orderedNames.some((name, index) => name !== EXACT_PACKAGES[index])) throw new Error("exact candidate package order must install lifecycle, pi-subagents, remote-pi, then background");
}

async function readManifest(candidateRoot: string, manifestPath: string): Promise<{ manifest: Readonly<CandidateManifest>; manifestSha256: string }> {
  const resolvedManifest = containedPath(candidateRoot, manifestPath, "candidate manifest");
  const parsed: unknown = JSON.parse(await readFile(resolvedManifest, "utf8"));
  const manifest = validateCandidateManifest(parsed);
  assertExactFourPackageManifest(manifest);
  return { manifest, manifestSha256: await fileSha256(resolvedManifest) };
}

async function verifyArchives(candidateRoot: string, manifest: Readonly<CandidateManifest>): Promise<Readonly<Record<string, string>>>
{
  const archiveSha256: Record<string, string> = {};
  const resolvedCandidateRoot = await realpath(candidateRoot);
  for (const artifact of manifest.artifacts) {
    const archive = containedPath(candidateRoot, join(candidateRoot, artifact.archive), `archive ${artifact.id}`);
    const resolvedArchive = await realpath(archive);
    containedPath(resolvedCandidateRoot, resolvedArchive, `archive ${artifact.id}`);
    const archiveStat = await stat(resolvedArchive);
    if (!archiveStat.isFile()) throw new Error(`archive ${artifact.id} must be a file`);
    const digest = await fileSha256(resolvedArchive);
    if (digest !== artifact.archiveSha256) throw new Error(`archive ${artifact.id} digest does not match candidate manifest`);
    archiveSha256[artifact.id] = digest;
  }
  return Object.freeze(archiveSha256);
}

async function settingsSnapshot(settingsPath: string): Promise<{ exists: boolean; sha256?: string }> {
  try {
    await access(settingsPath);
    return { exists: true, sha256: await fileSha256(settingsPath) };
  } catch (error: unknown) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") return { exists: false };
    throw error;
  }
}

function freezeCommands(commands: readonly ExactCandidateCommand[]): readonly Readonly<ExactCandidateCommand>[] {
  return Object.freeze(commands.map((entry) => Object.freeze({ command: entry.command, args: Object.freeze([...entry.args]), cwd: entry.cwd })));
}

export async function runExactCandidate(options: {
  candidateRoot: string;
  manifestPath: string;
  piCommand: string;
  seed?: string | number;
  cycles?: number;
  maxDurationMs?: number;
  rollbackRehearsal?: boolean;
  writeReceipts?: string;
  runner?: ExactCandidateCommandRunner;
  executeScenario(context: ExactCandidateScenarioContext): Promise<void> | void;
  runSoakCycle(context: ExactCandidateScenarioContext, cycle: number): Promise<void> | void;
}): Promise<Readonly<ExactCandidateReceipt>> {
  const candidateRoot = resolve(options.candidateRoot);
  const { manifest, manifestSha256 } = await readManifest(candidateRoot, options.manifestPath);
  const seed = options.seed ?? manifest.scenario.seed;
  const cycles = options.cycles ?? 100;
  const maxDurationMs = options.maxDurationMs ?? 60 * 60 * 1000;
  const receiptsRoot = options.writeReceipts === undefined ? undefined : containedPath(candidateRoot, options.writeReceipts, "receipt directory");
  const runtimeRoot = join(candidateRoot, "runtime");
  const settingsPath = join(runtimeRoot, ".pi", "settings.json");
  const runner = options.runner ?? defaultRunner;
  const commands: ExactCandidateCommand[] = [];
  const run = async (command: string, args: readonly string[]): Promise<void> => {
    commands.push({ command, args: [...args], cwd: runtimeRoot });
    await runner.run(command, args, runtimeRoot);
  };

  await mkdir(candidateRoot, { recursive: true });
  if (receiptsRoot !== undefined) await mkdir(receiptsRoot, { recursive: true });
  try {
    await access(runtimeRoot);
    throw new Error("candidate runtime already exists; use a fresh external candidate root");
  } catch (error: unknown) {
    if (!(typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT")) throw error;
  }
  await mkdir(runtimeRoot, { recursive: true });
  const settingsBeforeInstall = await settingsSnapshot(settingsPath);
  const archiveSha256 = await verifyArchives(candidateRoot, manifest);
  const orderedArtifacts = manifest.packageOrder.map((id) => manifest.artifacts.find((artifact) => artifact.id === id) as CandidateArtifact);
  const archivePaths = orderedArtifacts.map((artifact) => containedPath(candidateRoot, join(candidateRoot, artifact.archive), `archive ${artifact.id}`));
  const roots = await createDisposableHarnessRoots("pi-context-lifecycle-exact-candidate-");
  let rollback: ExactCandidateRollbackReceipt | undefined;
  let completedReceipt: Readonly<ExactCandidateReceipt> | undefined;
  try {
    await withDisposableHarnessEnvironment(roots, async () => {
      await run("npm", ["install", "--prefix", runtimeRoot, "--ignore-scripts", "--omit=dev", "--no-audit", "--no-fund", ...archivePaths]);
      const packageDirectories: Record<string, string> = {};
      for (const artifact of orderedArtifacts) {
        const directory = packageDirectory(runtimeRoot, artifact.packageName);
        const resolvedDirectory = await (async () => {
          const directoryStat = await stat(directory);
          if (!directoryStat.isDirectory()) throw new Error(`installed package ${artifact.packageName} is not a directory`);
          return resolve(directory);
        })();
        const actualDirectory = await realpath(resolvedDirectory);
        containedPath(await realpath(candidateRoot), actualDirectory, `installed package ${artifact.packageName}`);
        const installedPackage = JSON.parse(await readFile(join(actualDirectory, "package.json"), "utf8")) as { name?: unknown };
        if (installedPackage.name !== artifact.packageName) throw new Error(`installed package identity does not match ${artifact.packageName}`);
        packageDirectories[artifact.id] = actualDirectory;
        await run(options.piCommand, ["install", "-l", "--approve", actualDirectory]);
      }
      const frozenPackageDirectories = Object.freeze({ ...packageDirectories });
      const createContext = (scenarioId: string, context: ScenarioContext): ExactCandidateScenarioContext => ({
        ...context,
        candidateRoot,
        runtimeRoot,
        packageDirectories: frozenPackageDirectories,
        roots,
        manualCharacterization: scenarioId === "manual-compact-characterization",
      });
      const scenarios: ScenarioReceipt[] = [];
      for (const scenarioId of EXACT_CANDIDATE_SCENARIOS) {
        const receipt = await runScenario({
          scenarioId,
          seed,
          async execute(context) { await options.executeScenario(createContext(scenarioId, context)); },
        });
        if (receipt.status !== "passed") throw new Error(`exact candidate scenario failed: ${scenarioId}`);
        scenarios.push(receipt);
      }
      const soak = await runBoundedSoak({
        cycles,
        seed,
        maxDurationMs,
        async runCycle(cycle) {
          const context = createContext("soak", {
            scenarioId: "soak",
            seed,
            timeline: createStructuredTimeline({ scenarioId: "soak", seed }),
          });
          await options.runSoakCycle(context, cycle);
        },
      });
      if (soak.status !== "passed") throw new Error(`exact candidate soak did not pass: ${soak.status}`);
      if (options.rollbackRehearsal === true) {
        for (const artifact of [...orderedArtifacts].reverse()) await run(options.piCommand, ["remove", "-l", "--approve", packageDirectories[artifact.id] as string]);
        // A fresh candidate runtime has no project settings before the rehearsal. Pi
        // intentionally leaves an empty package list after its documented remove
        // command, so remove that candidate-created project directory to restore the
        // byte-identical absent snapshot without editing settings contents.
        if (!settingsBeforeInstall.exists) await rm(join(runtimeRoot, ".pi"), { recursive: true, force: true });
        const afterRollback = await settingsSnapshot(settingsPath);
        if (settingsBeforeInstall.exists !== afterRollback.exists || settingsBeforeInstall.sha256 !== afterRollback.sha256) {
          throw new Error("Pi rollback did not restore the disposable project settings byte-for-byte");
        }
        rollback = Object.freeze({
          settingsExistedBeforeInstall: settingsBeforeInstall.exists,
          ...(settingsBeforeInstall.sha256 === undefined ? {} : { settingsSha256BeforeInstall: settingsBeforeInstall.sha256 }),
          settingsExistedAfterRollback: afterRollback.exists,
          ...(afterRollback.sha256 === undefined ? {} : { settingsSha256AfterRollback: afterRollback.sha256 }),
          status: "restored",
        });
      }
      const receipt: ExactCandidateReceipt = Object.freeze({
        schemaVersion: 1,
        manifestSha256,
        candidateRoot,
        runtimeRoot,
        packageOrder: Object.freeze([...manifest.packageOrder]),
        scenarioVersion: manifest.scenario.version,
        archiveSha256,
        packageDirectories: frozenPackageDirectories,
        commands: freezeCommands(commands),
        scenarios: Object.freeze(scenarios),
        soak,
        ...(rollback === undefined ? {} : { rollback }),
        status: "passed",
      });
      if (receiptsRoot !== undefined) await writeFile(join(receiptsRoot, "exact-candidate-receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
      completedReceipt = receipt;
    });
  } finally {
    await roots.cleanup();
  }
  if (completedReceipt === undefined) throw new Error("exact candidate runner did not return a receipt");
  return completedReceipt;
}

export async function removeExactCandidateRuntime(candidateRoot: string): Promise<void> {
  const runtimeRoot = containedPath(candidateRoot, join(candidateRoot, "runtime"), "candidate runtime");
  await rm(runtimeRoot, { recursive: true, force: true });
}
