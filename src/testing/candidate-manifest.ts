import { isAbsolute, normalize } from "node:path";

export interface CandidateArtifact {
  id: string;
  packageName: string;
  repository: string;
  commit: string;
  tree: string;
  lockfileSha256: string;
  archive: string;
  archiveSha256: string;
}

export interface CandidateManifest {
  schemaVersion: 1;
  pi: {
    packageName: string;
    version: string;
    integrity: string;
  };
  scenario: {
    version: string;
    seed: string | number;
  };
  packageOrder: readonly string[];
  artifacts: readonly Readonly<CandidateArtifact>[];
}

const FULL_GIT_ID = /^[0-9a-f]{40}$/;
const SHA256 = /^[0-9a-f]{64}$/;
const REPOSITORY = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const EXACT_VERSION = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;
const PACKAGE_NAME = /^(?:@[a-z0-9_.-]+\/)?[a-z0-9_.-]+$/i;

function objectRecord(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as Record<string, unknown>;
}

function stringField(record: Record<string, unknown>, field: string, label: string): string {
  const value = record[field];
  if (typeof value !== "string" || value.length === 0) throw new Error(`${label}.${field} must be a non-empty string`);
  return value;
}

function immutableArchivePath(value: string): boolean {
  if (isAbsolute(value) || value.includes("\\")) return false;
  const normalized = normalize(value).replaceAll("\\", "/");
  return normalized === value && !normalized.startsWith("../") && normalized !== ".." && !normalized.includes("/../");
}

export function validateCandidateManifest(value: unknown): Readonly<CandidateManifest> {
  const root = objectRecord(value, "candidate manifest");
  if (root.schemaVersion !== 1) throw new Error("candidate manifest schemaVersion must be 1");

  const piInput = objectRecord(root.pi, "candidate manifest pi");
  const pi = {
    packageName: stringField(piInput, "packageName", "pi"),
    version: stringField(piInput, "version", "pi"),
    integrity: stringField(piInput, "integrity", "pi"),
  };
  if (!PACKAGE_NAME.test(pi.packageName)) throw new Error("pi packageName is invalid");
  if (!EXACT_VERSION.test(pi.version)) throw new Error("pi version must be exact");
  if (!/^sha512-[A-Za-z0-9+/=]+$/.test(pi.integrity)) throw new Error("pi integrity must be an sha512 value");

  const scenarioInput = objectRecord(root.scenario, "candidate manifest scenario");
  const scenarioVersion = stringField(scenarioInput, "version", "scenario");
  const seed = scenarioInput.seed;
  if ((typeof seed !== "string" || seed.length === 0) && (typeof seed !== "number" || !Number.isSafeInteger(seed))) throw new Error("scenario seed must be a non-empty string or safe integer");

  if (!Array.isArray(root.artifacts) || root.artifacts.length === 0 || root.artifacts.length > 16) throw new Error("candidate artifacts must contain 1-16 entries");
  const artifacts = root.artifacts.map((entry, index): Readonly<CandidateArtifact> => {
    const input = objectRecord(entry, `artifact ${index}`);
    const artifact: CandidateArtifact = {
      id: stringField(input, "id", `artifact ${index}`),
      packageName: stringField(input, "packageName", `artifact ${index}`),
      repository: stringField(input, "repository", `artifact ${index}`),
      commit: stringField(input, "commit", `artifact ${index}`),
      tree: stringField(input, "tree", `artifact ${index}`),
      lockfileSha256: stringField(input, "lockfileSha256", `artifact ${index}`),
      archive: stringField(input, "archive", `artifact ${index}`),
      archiveSha256: stringField(input, "archiveSha256", `artifact ${index}`),
    };
    if (!PACKAGE_NAME.test(artifact.packageName)) throw new Error(`artifact ${index} packageName is invalid`);
    if (!REPOSITORY.test(artifact.repository)) throw new Error(`artifact ${index} repository is invalid`);
    if (!FULL_GIT_ID.test(artifact.commit)) throw new Error(`artifact ${index} commit must be a full 40-character Git id`);
    if (!FULL_GIT_ID.test(artifact.tree)) throw new Error(`artifact ${index} tree must be a full 40-character Git id`);
    if (!SHA256.test(artifact.lockfileSha256)) throw new Error(`artifact ${index} lockfileSha256 is invalid`);
    if (!SHA256.test(artifact.archiveSha256)) throw new Error(`artifact ${index} archiveSha256 is invalid`);
    if (!immutableArchivePath(artifact.archive)) throw new Error(`artifact ${index} archive must be a normalized relative path`);
    return Object.freeze(artifact);
  });

  if (!Array.isArray(root.packageOrder)) throw new Error("candidate package order must be a string array");
  const packageOrder = root.packageOrder.map((entry) => {
    if (typeof entry !== "string" || entry.length === 0) throw new Error("candidate package order must be a string array");
    return entry;
  });
  const artifactIds = artifacts.map((artifact) => artifact.id);
  if (new Set(packageOrder).size !== packageOrder.length || packageOrder.length !== artifactIds.length || packageOrder.some((id) => !artifactIds.includes(id))) throw new Error("candidate package order must contain every artifact id exactly once");
  if (new Set(artifactIds).size !== artifactIds.length) throw new Error("candidate artifact ids must be unique");

  return Object.freeze({
    schemaVersion: 1,
    pi: Object.freeze(pi),
    scenario: Object.freeze({ version: scenarioVersion, seed }),
    packageOrder: Object.freeze(packageOrder),
    artifacts: Object.freeze(artifacts),
  });
}
