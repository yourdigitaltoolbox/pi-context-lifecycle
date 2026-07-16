import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import type { DisposableHarnessRoots } from "./disposable-roots.js";

const execFileAsync = promisify(execFile);

export interface PackagedSmokeCommand {
  command: string;
  args: readonly string[];
  cwd: string;
}

export interface PackagedSmokeReceipt {
  schemaVersion: 1;
  packageName: string;
  archive: string;
  archiveSha256: string;
  installRoot: string;
  imports: readonly string[];
  commands: readonly Readonly<PackagedSmokeCommand>[];
  status: "passed";
}

export interface PackagedSmokeRunner {
  run(command: string, args: readonly string[], cwd: string): Promise<void>;
}

const defaultRunner: PackagedSmokeRunner = {
  async run(command, args, cwd) {
    await execFileAsync(command, [...args], {
      cwd,
      env: { ...process.env, npm_config_audit: "false", npm_config_fund: "false", npm_config_update_notifier: "false" },
      maxBuffer: 10 * 1024 * 1024,
    });
  },
};

export async function runPackagedImportSmoke(options: {
  roots: DisposableHarnessRoots;
  archive: string;
  packageName: string;
  imports?: readonly string[];
  runner?: PackagedSmokeRunner;
}): Promise<Readonly<PackagedSmokeReceipt>> {
  if (!/^(?:@[a-z0-9_.-]+\/)?[a-z0-9_.-]+$/i.test(options.packageName)) throw new Error("packaged smoke packageName is invalid");
  const imports = options.imports ?? [options.packageName, `${options.packageName}/extension`];
  if (imports.length === 0 || imports.some((specifier) => typeof specifier !== "string" || (specifier !== options.packageName && !specifier.startsWith(`${options.packageName}/`)))) throw new Error("packaged smoke imports must be package-owned specifiers");
  const archive = resolve(options.archive);
  const artifactRoot = `${resolve(options.roots.artifacts)}/`;
  if (!`${archive}/`.startsWith(artifactRoot)) throw new Error("packaged smoke archive must be inside the disposable artifact root");
  const archiveSha256 = createHash("sha256").update(await readFile(archive)).digest("hex");
  const installRoot = join(options.roots.root, "packaged-smoke-install");
  await mkdir(installRoot, { recursive: true });
  const runner = options.runner ?? defaultRunner;
  const commands: PackagedSmokeCommand[] = [];
  const run = async (command: string, args: readonly string[]): Promise<void> => {
    commands.push({ command, args: [...args], cwd: installRoot });
    await runner.run(command, args, installRoot);
  };

  await run("npm", ["install", "--prefix", installRoot, "--ignore-scripts", "--omit=peer", "--no-audit", "--no-fund", archive]);
  await run(process.execPath, ["--input-type=module", "--eval", `await Promise.all(${JSON.stringify(imports)}.map((specifier) => import(specifier)))`]);

  return Object.freeze({
    schemaVersion: 1,
    packageName: options.packageName,
    archive,
    archiveSha256,
    installRoot,
    imports: Object.freeze([...imports]),
    commands: Object.freeze(commands.map((command) => Object.freeze({ ...command, args: Object.freeze([...command.args]) }))),
    status: "passed",
  });
}
