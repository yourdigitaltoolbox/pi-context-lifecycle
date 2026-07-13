import { execFile } from "node:child_process";
import { access, copyFile, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function disposableNpmEnvironment(root: string): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: join(root, "home"),
    XDG_CACHE_HOME: join(root, "cache"),
    npm_config_cache: join(root, "npm-cache"),
    npm_config_userconfig: join(root, "home", ".npmrc"),
    npm_config_audit: "false",
    npm_config_fund: "false",
    npm_config_update_notifier: "false",
  };
  delete environment.npm_config_ignore_scripts;
  return environment;
}

async function run(command: string, args: readonly string[], cwd: string, env?: NodeJS.ProcessEnv): Promise<string> {
  const result = await execFileAsync(command, [...args], { cwd, env, maxBuffer: 10 * 1024 * 1024 });
  return result.stdout;
}

async function createExactSourceRepository(root: string): Promise<{ source: string; commit: string }> {
  const source = join(root, "source");
  await mkdir(source, { recursive: true });
  const tracked = (await run("git", ["ls-files", "-z"], projectRoot)).split("\0").filter((path) => path.length > 0);
  for (const path of tracked) {
    const destination = join(source, path);
    await mkdir(dirname(destination), { recursive: true });
    await copyFile(join(projectRoot, path), destination);
  }
  await run("git", ["init", "--quiet"], source);
  await run("git", ["config", "user.email", "git-install-smoke@example.invalid"], source);
  await run("git", ["config", "user.name", "Git install smoke"], source);
  await run("git", ["add", "."], source);
  await run("git", ["commit", "--quiet", "-m", "fixture"], source);
  return { source, commit: (await run("git", ["rev-parse", "HEAD"], source)).trim() };
}

describe("Git package install with dev dependencies omitted", () => {
  it("runs prepare from an exact Git revision using only production build dependencies", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-context-lifecycle-git-omit-dev-"));
    const environment = disposableNpmEnvironment(root);
    try {
      const { source, commit } = await createExactSourceRepository(root);
      const consumer = join(root, "consumer");
      await mkdir(consumer, { recursive: true });
      const packageSpecifier = `git+${pathToFileURL(source).href}#${commit}`;
      await run("npm", ["install", "--prefix", consumer, "--omit=dev", "--no-audit", "--no-fund", packageSpecifier], consumer, environment);

      const packageDirectory = join(consumer, "node_modules", "@yourdigitaltoolbox", "pi-context-lifecycle");
      await access(join(packageDirectory, "dist", "extension.js"));
      const manifest = JSON.parse(await readFile(join(packageDirectory, "package.json"), "utf8")) as { dependencies?: Record<string, string>; devDependencies?: Record<string, string> };
      expect(manifest.dependencies).toMatchObject({
        "@earendil-works/pi-ai": "0.80.6",
        "@earendil-works/pi-coding-agent": "0.80.6",
        "@types/node": "^22.17.0",
        typescript: "^5.9.2",
      });
      expect(manifest.devDependencies?.typescript).toBeUndefined();
      expect(manifest.devDependencies?.["@types/node"]).toBeUndefined();
      expect(manifest.devDependencies?.["@earendil-works/pi-ai"]).toBeUndefined();
      expect(manifest.devDependencies?.["@earendil-works/pi-coding-agent"]).toBeUndefined();
      await access(join(consumer, "node_modules", "typescript", "bin", "tsc"));
      await access(join(consumer, "node_modules", "@types", "node", "package.json"));
      await expect(access(join(consumer, "node_modules", "vitest"))).rejects.toThrow();
      await run(process.execPath, ["--input-type=module", "--eval", 'await Promise.all([import("@yourdigitaltoolbox/pi-context-lifecycle"), import("@yourdigitaltoolbox/pi-context-lifecycle/extension")])'], consumer, environment);

      const piCheckout = join(root, "pi-checkout");
      await run("git", ["clone", "--quiet", "--no-local", source, piCheckout], root);
      await run("git", ["checkout", "--quiet", commit], piCheckout);
      await run("npm", ["install", "--omit=dev", "--no-audit", "--no-fund"], piCheckout, environment);
      await access(join(piCheckout, "dist", "extension.js"));
      await access(join(piCheckout, "node_modules", "@earendil-works", "pi-coding-agent", "package.json"));
      await expect(access(join(piCheckout, "node_modules", "vitest"))).rejects.toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 180_000);
});
