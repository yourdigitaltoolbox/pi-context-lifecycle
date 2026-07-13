#!/usr/bin/env node
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type TestingModule = typeof import("../src/testing/index.js");
type ExactCandidateScenarioId = import("../src/testing/index.js").ExactCandidateScenarioId;

interface Arguments {
  candidateRoot: string;
  manifest: string;
  seed?: string;
  cycles?: number;
  maxDurationMs?: number;
  rollbackRehearsal: boolean;
  writeReceipts?: string;
  scenario?: string;
}

function usage(): never {
  throw new Error("usage: npm run test:exact-candidate -- --manifest <candidate-manifest.json> --candidate-root <external-root> [--seed <seed>] [--cycles <1-10000>] [--max-duration-ms <ms>] [--write-receipts <external-dir>] [--scenario <layer-3-scenario-id>] [--rollback-rehearsal]");
}

function parseArguments(argv: readonly string[]): Arguments {
  const values = new Map<string, string>();
  let rollbackRehearsal = false;
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--rollback-rehearsal") { rollbackRehearsal = true; continue; }
    if (argument === undefined || !["--manifest", "--candidate-root", "--seed", "--cycles", "--max-duration-ms", "--write-receipts", "--scenario"].includes(argument)) usage();
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--") || values.has(argument)) usage();
    values.set(argument, value);
    index += 1;
  }
  const candidateRoot = values.get("--candidate-root");
  const manifest = values.get("--manifest");
  if (candidateRoot === undefined || manifest === undefined) usage();
  const parseBoundedInteger = (flag: string): number | undefined => {
    const value = values.get(flag);
    if (value === undefined) return undefined;
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed < 1) usage();
    return parsed;
  };
  const seed = values.get("--seed");
  const cycles = parseBoundedInteger("--cycles");
  const maxDurationMs = parseBoundedInteger("--max-duration-ms");
  const writeReceipts = values.get("--write-receipts");
  const scenario = values.get("--scenario");
  return {
    candidateRoot,
    manifest,
    ...(seed === undefined ? {} : { seed }),
    ...(cycles === undefined ? {} : { cycles }),
    ...(maxDurationMs === undefined ? {} : { maxDurationMs }),
    ...(writeReceipts === undefined ? {} : { writeReceipts }),
    ...(scenario === undefined ? {} : { scenario }),
    rollbackRehearsal,
  };
}

const args = parseArguments(process.argv.slice(2));
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const testing = await import(new URL("../dist/testing/index.js", import.meta.url).href) as TestingModule;
const receipt = await testing.runExactCandidate({
  candidateRoot: args.candidateRoot,
  manifestPath: args.manifest,
  piCommand: resolve(root, "node_modules/.bin/pi"),
  ...(args.seed === undefined ? {} : { seed: args.seed }),
  ...(args.cycles === undefined ? {} : { cycles: args.cycles }),
  ...(args.maxDurationMs === undefined ? {} : { maxDurationMs: args.maxDurationMs }),
  ...(args.writeReceipts === undefined ? {} : { writeReceipts: args.writeReceipts }),
  ...(args.scenario === undefined ? {} : { scenarioIds: [args.scenario as ExactCandidateScenarioId] }),
  rollbackRehearsal: args.rollbackRehearsal,
});
process.stdout.write(`${JSON.stringify({ status: receipt.status, scenarios: receipt.scenarios.length, completedCycles: receipt.soak.completedCycles, rollback: receipt.rollback?.status ?? "not-requested" })}\n`);
