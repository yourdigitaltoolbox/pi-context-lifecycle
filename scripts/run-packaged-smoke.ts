#!/usr/bin/env node
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createDisposableHarnessRoots, runPackagedImportSmoke, withDisposableHarnessEnvironment } from "../dist/testing/index.js";

const execFileAsync = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const roots = await createDisposableHarnessRoots("pi-context-lifecycle-packaged-");
try {
  await withDisposableHarnessEnvironment(roots, async () => {
    const packed = await execFileAsync("npm", ["pack", "--json", "--pack-destination", roots.artifacts], { cwd: root, maxBuffer: 10 * 1024 * 1024 });
    const result = JSON.parse(packed.stdout) as unknown;
    const first: unknown = Array.isArray(result) ? result[0] : undefined;
    const filename = typeof first === "object" && first !== null && "filename" in first && typeof first.filename === "string" ? first.filename : undefined;
    if (filename === undefined) throw new Error("npm pack did not return an archive filename");
    const receipt = await runPackagedImportSmoke({
      roots,
      archive: `${roots.artifacts}/${filename}`,
      packageName: "@yourdigitaltoolbox/pi-context-lifecycle",
    });
    process.stdout.write(`${JSON.stringify({ status: receipt.status, packageName: receipt.packageName, archiveSha256: receipt.archiveSha256, commandCount: receipt.commands.length })}\n`);
  });
} finally {
  await roots.cleanup();
}
