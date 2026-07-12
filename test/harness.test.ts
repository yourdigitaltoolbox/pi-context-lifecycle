import { access } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { createDisposableHarnessRoots, withDisposableHarnessEnvironment } from "../src/testing/index.js";

describe("public SDK harness roots", () => {
  it("isolates and restores every environment root", async () => {
    const roots = await createDisposableHarnessRoots();
    const previous = {
      HOME: process.env.HOME,
      XDG_CACHE_HOME: process.env.XDG_CACHE_HOME,
      XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
      PI_CODING_AGENT_DIR: process.env.PI_CODING_AGENT_DIR,
    };
    try {
      await withDisposableHarnessEnvironment(roots, () => {
        expect(process.env.HOME).toBe(roots.home);
        expect(process.env.XDG_CACHE_HOME).toBe(roots.cache);
        expect(process.env.XDG_RUNTIME_DIR).toBe(roots.sockets);
        expect(process.env.PI_CODING_AGENT_DIR).toBe(roots.agentDir);
        return Promise.resolve();
      });
      expect({
        HOME: process.env.HOME,
        XDG_CACHE_HOME: process.env.XDG_CACHE_HOME,
        XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
        PI_CODING_AGENT_DIR: process.env.PI_CODING_AGENT_DIR,
      }).toEqual(previous);
    } finally {
      await roots.cleanup();
    }
    await expect(access(roots.root)).rejects.toThrow();
  });
});
