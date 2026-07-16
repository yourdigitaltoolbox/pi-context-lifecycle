import { createStructuredTimeline, type StructuredTimeline, type StructuredTimelineEvent } from "./timeline.js";

export interface ScenarioContext {
  scenarioId: string;
  seed: string | number;
  timeline: StructuredTimeline;
}

export class ScenarioBlockedError extends Error {
  constructor(readonly limitation: string) {
    super(limitation);
    this.name = "ScenarioBlockedError";
  }
}

export interface ScenarioReceipt {
  schemaVersion: 1;
  scenarioId: string;
  seed: string | number;
  /** Blocked is a fail-closed public-SDK limitation, never a passing verdict. */
  status: "passed" | "failed" | "blocked";
  startedAt: number;
  completedAt: number;
  eventCount: number;
  failureCode?: "scenario-failed" | "public-sdk-limitation";
  timeline: readonly Readonly<StructuredTimelineEvent>[];
}

export async function runScenario(options: {
  scenarioId: string;
  seed: string | number;
  execute(context: ScenarioContext): Promise<void> | void;
  now?: () => number;
  maxEvents?: number;
}): Promise<Readonly<ScenarioReceipt>> {
  const now = options.now ?? Date.now;
  const startedAt = now();
  const timeline = createStructuredTimeline({ scenarioId: options.scenarioId, seed: options.seed, ...(options.maxEvents === undefined ? {} : { maxEvents: options.maxEvents }), now });
  let failureCode: ScenarioReceipt["failureCode"];
  try {
    await options.execute({ scenarioId: options.scenarioId, seed: options.seed, timeline });
  } catch (error) {
    failureCode = error instanceof ScenarioBlockedError ? "public-sdk-limitation" : "scenario-failed";
  }
  const events = timeline.events();
  return Object.freeze({
    schemaVersion: 1,
    scenarioId: options.scenarioId,
    seed: options.seed,
    status: failureCode === undefined ? "passed" : failureCode === "public-sdk-limitation" ? "blocked" : "failed",
    startedAt,
    completedAt: now(),
    eventCount: events.length,
    ...(failureCode === undefined ? {} : { failureCode }),
    timeline: events,
  });
}

export interface SoakReceipt {
  schemaVersion: 1;
  seed: string | number;
  requestedCycles: number;
  completedCycles: number;
  status: "passed" | "failed" | "deadline-exceeded" | "skipped";
  failureCycle?: number;
  startedAt: number;
  completedAt: number;
}

export async function runBoundedSoak(options: {
  cycles: number;
  seed: string | number;
  maxDurationMs: number;
  runCycle(cycle: number, seed: string | number): Promise<void> | void;
  now?: () => number;
}): Promise<Readonly<SoakReceipt>> {
  if (!Number.isSafeInteger(options.cycles) || options.cycles < 1 || options.cycles > 10_000) throw new Error("soak cycles must be between 1 and 10000");
  if (!Number.isSafeInteger(options.maxDurationMs) || options.maxDurationMs < 1 || options.maxDurationMs > 24 * 60 * 60 * 1000) throw new Error("soak maxDurationMs must be between 1 and 86400000");
  const now = options.now ?? Date.now;
  const startedAt = now();
  let completedCycles = 0;
  let failureCycle: number | undefined;
  let deadlineExceeded = false;
  for (let cycle = 0; cycle < options.cycles; cycle += 1) {
    if (now() - startedAt >= options.maxDurationMs) {
      deadlineExceeded = true;
      break;
    }
    try {
      await options.runCycle(cycle, options.seed);
      completedCycles += 1;
    } catch {
      failureCycle = cycle;
      break;
    }
  }
  const status = failureCycle !== undefined ? "failed" : deadlineExceeded ? "deadline-exceeded" : "passed";
  return Object.freeze({
    schemaVersion: 1,
    seed: options.seed,
    requestedCycles: options.cycles,
    completedCycles,
    status,
    ...(failureCycle === undefined ? {} : { failureCycle }),
    startedAt,
    completedAt: now(),
  });
}
