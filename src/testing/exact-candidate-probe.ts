import { readFile, realpath } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import type { LifecycleLane } from "../types.js";

export type ExactCandidateConsumer = "pi-subagents" | "remote-pi" | "pi-background-tasks";

export type ExactCandidateProbeInjection =
  | Readonly<{ consumer: "pi-subagents"; kind: "completion"; id: string; outcome: "success" | "failure" }>
  | Readonly<{ consumer: "remote-pi"; kind: "compact-request"; id: string; ownerId: string }>
  | Readonly<{ consumer: "remote-pi"; kind: "mesh-arrival"; id: string; lane: "reply" | "unsolicited" }>
  | Readonly<{ consumer: "pi-background-tasks"; kind: "notify" | "loop" | "cron" | "watchdog"; id: string }>;

export interface ExactCandidateProbeReceipt {
  consumer: ExactCandidateConsumer;
  id: string;
  outcome: "accepted" | "held" | "released" | "coalesced" | "rejected" | "completed" | "failed";
  /** Canonical redacted lifecycle lane on production held/released receipts. */
  laneId?: LifecycleLane;
  /** Remote Pi's current packaged probe spelling, normalized by the matrix. */
  lane?: LifecycleLane;
  /**
   * Redacted monotonic production dispatch ordering. A consumer may attach it
   * when receipt delivery is asynchronous with respect to its real submit.
   */
  dispatchSequence?: number;
  operationId?: string;
  generationId?: string;
  notificationCount?: number;
}

export interface ExactCandidateProbeOptions {
  /** The documented public session API; consumers must not import Pi internals. */
  session: AgentSession;
  seed: string | number;
  packageDirectory: string;
}

/**
 * Public, archive-included bridge to a consumer's real adapter boundary. It is
 * intentionally one-way: injections and observations contain opaque IDs and
 * outcomes only, never prompts, results, mesh bodies, or mutable coordinator state.
 */
export interface ExactCandidateProbe {
  readonly consumer: ExactCandidateConsumer;
  inject(input: ExactCandidateProbeInjection): Promise<Readonly<ExactCandidateProbeReceipt>>;
  observations(): Promise<readonly Readonly<ExactCandidateProbeReceipt>[]>;
  dispose(): Promise<void>;
}

export interface ExactCandidateTestingModule {
  createExactCandidateProbe(options: ExactCandidateProbeOptions): Promise<ExactCandidateProbe> | ExactCandidateProbe;
}

function isContained(root: string, path: string): boolean {
  const relation = relative(root, path);
  return relation === "" || (!relation.startsWith(`..${sep}`) && relation !== "..");
}

function requireTestingModule(value: unknown): ExactCandidateTestingModule {
  if (typeof value !== "object" || value === null || !("createExactCandidateProbe" in value) || typeof value.createExactCandidateProbe !== "function") {
    throw new Error("candidate consumer testing subpath must export createExactCandidateProbe");
  }
  return value as ExactCandidateTestingModule;
}

/**
 * Resolve `<archive-derived package>/testing` from the disposable candidate
 * runtime, not from this lifecycle worktree. Consumers must expose that exact
 * public subpath through package exports (or an equivalent package-local file).
 */
function testingImportTarget(packageJson: unknown): string {
  if (typeof packageJson !== "object" || packageJson === null || !("exports" in packageJson)) throw new Error("candidate consumer package does not export ./testing");
  const exportsField = packageJson.exports;
  if (typeof exportsField !== "object" || exportsField === null || !("./testing" in exportsField)) throw new Error("candidate consumer package does not export ./testing");
  const testing = exportsField["./testing"];
  const target = typeof testing === "string"
    ? testing
    : typeof testing === "object" && testing !== null && "import" in testing ? testing.import : undefined;
  if (typeof target !== "string" || !target.startsWith("./")) throw new Error("candidate consumer ./testing export must provide a relative import target");
  return target;
}

export async function loadExactCandidateProbe(options: ExactCandidateProbeOptions & { packageName: ExactCandidateConsumer }): Promise<ExactCandidateProbe> {
  const packageJson = JSON.parse(await readFile(resolve(options.packageDirectory, "package.json"), "utf8")) as unknown;
  const resolvedSubpath = resolve(options.packageDirectory, testingImportTarget(packageJson));
  const [actualDirectory, actualSubpath] = await Promise.all([realpath(options.packageDirectory), realpath(resolvedSubpath)]);
  if (!isContained(actualDirectory, actualSubpath)) throw new Error(`candidate consumer testing subpath escaped ${options.packageName} archive directory`);
  const testingModule = requireTestingModule(await import(pathToFileURL(actualSubpath).href));
  const probe = await testingModule.createExactCandidateProbe(options);
  if (probe.consumer !== options.packageName || typeof probe.inject !== "function" || typeof probe.observations !== "function" || typeof probe.dispose !== "function") {
    throw new Error(`candidate consumer testing subpath returned an invalid ${options.packageName} probe`);
  }
  return probe;
}
