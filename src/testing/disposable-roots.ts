import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

export interface DisposableHarnessRoots {
  root: string;
  home: string;
  agentDir: string;
  cwd: string;
  sessions: string;
  cache: string;
  sockets: string;
  artifacts: string;
  cleanup(): Promise<void>;
}

export async function createDisposableHarnessRoots(prefix = "pi-context-lifecycle-"): Promise<DisposableHarnessRoots> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const paths = {
    home: join(root, "home"),
    agentDir: join(root, "home", ".pi", "agent"),
    cwd: join(root, "cwd"),
    sessions: join(root, "sessions"),
    cache: join(root, "cache"),
    sockets: join(root, "sockets"),
    artifacts: join(root, "artifacts"),
  };
  await Promise.all(Object.values(paths).map((path) => mkdir(path, { recursive: true })));
  const resolvedTemp = `${resolve(tmpdir())}/`;
  if (!`${resolve(root)}/`.startsWith(resolvedTemp)) throw new Error("Disposable harness root escaped the OS temporary directory");
  return {
    root,
    ...paths,
    async cleanup() { await rm(root, { recursive: true, force: true }); },
  };
}

const DISPOSABLE_ENVIRONMENT_KEYS = ["HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR", "PI_CODING_AGENT_DIR"] as const;

export async function withDisposableHarnessEnvironment<T>(roots: DisposableHarnessRoots, run: () => Promise<T>): Promise<T> {
  const previous = new Map<string, string | undefined>(DISPOSABLE_ENVIRONMENT_KEYS.map((key) => [key, process.env[key]]));
  process.env.HOME = roots.home;
  process.env.XDG_CACHE_HOME = roots.cache;
  process.env.XDG_RUNTIME_DIR = roots.sockets;
  process.env.PI_CODING_AGENT_DIR = roots.agentDir;
  try {
    return await run();
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) Reflect.deleteProperty(process.env, key);
      else process.env[key] = value;
    }
  }
}
