import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import {
  AuthStorage,
  createAgentSessionFromServices,
  createAgentSessionRuntime,
  createAgentSessionServices,
  ModelRegistry,
  SessionManager,
  SettingsManager,
  type CreateAgentSessionRuntimeFactory,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);

async function createArchivePackage(root: string): Promise<string> {
  const packageRoot = join(root, "archive-source");
  const archives = join(root, "archives");
  await mkdir(packageRoot, { recursive: true });
  await mkdir(archives, { recursive: true });
  const extension = (name: "loop" | "cron") => `
import { appendFile } from "node:fs/promises";
import { join } from "node:path";

export default function (pi) {
  let timer;
  const record = (ctx, event) => appendFile(join(ctx.cwd, ".archive-runtime-cleanup.jsonl"), \`${name}:\${event}\\n\`);
  pi.on("session_start", async (_event, ctx) => {
    timer = setInterval(() => undefined, 60_000);
    await record(ctx, "start");
  });
  pi.on("session_shutdown", async (_event, ctx) => {
    if (timer !== undefined) clearInterval(timer);
    timer = undefined;
    await record(ctx, "shutdown");
  });
}
`;
  await writeFile(join(packageRoot, "package.json"), JSON.stringify({
    name: "archive-runtime-cleanup-fixture",
    version: "1.0.0",
    type: "module",
    files: ["loop.mjs", "cron.mjs"],
    pi: { extensions: ["./loop.mjs", "./cron.mjs"] },
  }));
  await writeFile(join(packageRoot, "loop.mjs"), extension("loop"));
  await writeFile(join(packageRoot, "cron.mjs"), extension("cron"));
  await execFileAsync("npm", ["pack", "--pack-destination", archives], { cwd: packageRoot });
  return join(archives, "archive-runtime-cleanup-fixture-1.0.0.tgz");
}

describe("exact candidate managed runtime", () => {
  it("emits session_shutdown and awaits archive-loaded loop and cron cleanup before session invalidation", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-context-lifecycle-runtime-"));
    try {
      const archive = await createArchivePackage(root);
      const runtimeRoot = join(root, "runtime");
      await execFileAsync("npm", ["install", "--prefix", runtimeRoot, "--ignore-scripts", "--omit=dev", "--no-audit", "--no-fund", archive]);
      const archivePackage = join(runtimeRoot, "node_modules", "archive-runtime-cleanup-fixture");
      const agentDir = join(root, "agent");
      const sessionManager = SessionManager.create(runtimeRoot, join(root, "sessions"));
      const authStorage = AuthStorage.inMemory();
      const modelRegistry = ModelRegistry.inMemory(authStorage);
      const settingsManager = SettingsManager.inMemory({ packages: [archivePackage] }, { projectTrusted: true });
      const createRuntime: CreateAgentSessionRuntimeFactory = async ({ cwd, agentDir: runtimeAgentDir, sessionManager: targetSessionManager, sessionStartEvent }) => {
        const services = await createAgentSessionServices({
          cwd,
          agentDir: runtimeAgentDir,
          authStorage,
          modelRegistry,
          settingsManager,
          resourceLoaderReloadOptions: { resolveProjectTrust: () => Promise.resolve(true) },
        });
        return {
          ...(await createAgentSessionFromServices({
            services,
            sessionManager: targetSessionManager,
            ...(sessionStartEvent === undefined ? {} : { sessionStartEvent }),
            noTools: "all",
          })),
          services,
          diagnostics: services.diagnostics,
        };
      };
      const runtime = await createAgentSessionRuntime(createRuntime, { cwd: runtimeRoot, agentDir, sessionManager });
      expect(runtime.services.resourceLoader.getExtensions().extensions.filter((extension) => extension.path.startsWith(archivePackage))).toHaveLength(2);
      await runtime.session.bindExtensions({ mode: "print" });
      const receiptPath = join(runtimeRoot, ".archive-runtime-cleanup.jsonl");
      expect(await readFile(receiptPath, "utf8")).toBe("loop:start\ncron:start\n");

      let eventsBeforeInvalidation = "";
      runtime.setBeforeSessionInvalidate(() => {
        eventsBeforeInvalidation = readFileSync(receiptPath, "utf8");
      });
      await runtime.dispose();

      expect(eventsBeforeInvalidation).toBe("loop:start\ncron:start\nloop:shutdown\ncron:shutdown\n");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
