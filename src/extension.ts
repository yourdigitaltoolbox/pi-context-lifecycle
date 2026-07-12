import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { ContextLifecycleCoordinatorV1, type ManagedCompactionAdapter } from "./coordinator.js";
import { getContextLifecycleDiagnosticsV1, getContextLifecycleSnapshotV1, publishContextLifecycleV1, repairContextLifecycleV1 } from "./registry.js";
import type { CompactionReason, LifecycleClaim, LifecycleClaimState, RepairAction, RepairEvidenceClass, RepairRequest } from "./types.js";

const DEFAULT_HANDOFF_THRESHOLD = 0.94;
const DEFAULT_HANDOFF_REARM_THRESHOLD = 0.65;

function ratioFromEnv(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  const trimmed = value.trim();
  const parsed = Number(trimmed.endsWith("%") ? Number(trimmed.slice(0, -1)) / 100 : trimmed);
  return Number.isFinite(parsed) && parsed > 0 && parsed < 1 ? parsed : fallback;
}

function handoffInstruction(percent: number): string {
  return [
    `Context usage crossed the managed handoff threshold (${Math.round(percent * 1000) / 10}%).`,
    "Finish the current safe work unit, write or update a durable handoff with repository state, validation, risks, and the exact next step, then call self_compact.",
    "On resume, reload project instructions, the durable handoff, and active plan/status files before continuing.",
    "Do not stop in the middle of a fragile operation, and do not use native manual /compact for this managed workflow.",
  ].join("\n\n");
}

function textToolResult(text: string) {
  return { content: [{ type: "text" as const, text }], details: { disposition: text } };
}

function parseHandoffArgs(args: string): { handoffPath: string; nextStep: string } {
  const fallback = { handoffPath: "HANDOFF.md", nextStep: "Continue from the next step recorded in the handoff." };
  const trimmed = args.trim();
  if (trimmed.length === 0) return fallback;
  try {
    const parsed = JSON.parse(trimmed) as { handoffPath?: unknown; nextStep?: unknown };
    return {
      handoffPath: typeof parsed.handoffPath === "string" && parsed.handoffPath.trim().length > 0 ? parsed.handoffPath.trim() : fallback.handoffPath,
      nextStep: typeof parsed.nextStep === "string" && parsed.nextStep.trim().length > 0 ? parsed.nextStep.trim() : fallback.nextStep,
    };
  } catch {
    return { ...fallback, handoffPath: trimmed };
  }
}

const CLAIM_STATES = new Set<LifecycleClaimState>(["requested", "compacting", "compacted", "resume-pending", "resume-admitting", "resume-admitted", "resume-settled", "released", "failed", "cancelled", "blocked-unknown"]);
const COMPACTION_REASONS = new Set<CompactionReason>(["self", "remote", "builtin", "threshold", "overflow"]);

function isLifecycleClaim(value: unknown): value is LifecycleClaim {
  if (typeof value !== "object" || value === null) return false;
  const claim = value as Partial<LifecycleClaim>;
  return claim.schemaVersion === 1
    && typeof claim.ownerInstanceId === "string" && claim.ownerInstanceId.length > 0
    && typeof claim.originOwnerInstanceId === "string" && claim.originOwnerInstanceId.length > 0
    && typeof claim.operationId === "string" && claim.operationId.length > 0
    && typeof claim.sessionId === "string" && claim.sessionId.length > 0
    && typeof claim.generationId === "string" && claim.generationId.length > 0
    && typeof claim.state === "string" && CLAIM_STATES.has(claim.state)
    && typeof claim.reason === "string" && COMPACTION_REASONS.has(claim.reason)
    && typeof claim.timestamp === "number" && Number.isFinite(claim.timestamp);
}

function lifecycleClaims(ctx: ExtensionContext): LifecycleClaim[] {
  const claims: LifecycleClaim[] = [];
  for (const entry of ctx.sessionManager.getEntries()) {
    if (entry.type === "custom" && entry.customType === "pi-context-lifecycle" && isLifecycleClaim(entry.data)) claims.push(entry.data);
  }
  return claims;
}

const REPAIR_ACTIONS = new Set<RepairAction>(["recognize-resume-admitted", "retry-resume-pending", "abandon-ambiguous-resume", "retry-blocked-drainer", "abandon-interrupted-operation"]);
const REPAIR_EVIDENCE_CLASSES = new Set<RepairEvidenceClass>(["persisted-resume-message", "persisted-resume-run-settled", "no-admission-attempt", "current-process-quiescent", "owner-process-replaced", "idempotent-drainer-state", "branch-validated-owner-replaced"]);

function parseRepairRequest(value: string): RepairRequest | undefined {
  try {
    const request = JSON.parse(value) as Record<string, unknown>;
    if (typeof request.action !== "string" || !REPAIR_ACTIONS.has(request.action as RepairAction)
      || request.expectedPhase !== "blocked-unknown"
      || typeof request.evidenceClass !== "string" || !REPAIR_EVIDENCE_CLASSES.has(request.evidenceClass as RepairEvidenceClass)
      || request.actor !== "operator"
      || (request.channel !== "command" && request.channel !== "remote")
      || typeof request.operationId !== "string" || request.operationId.length === 0
      || typeof request.sessionId !== "string" || request.sessionId.length === 0
      || typeof request.generationId !== "string" || request.generationId.length === 0
      || typeof request.expectedSequence !== "number" || !Number.isSafeInteger(request.expectedSequence) || request.expectedSequence < 0
      || (request.consumerId !== undefined && (typeof request.consumerId !== "string" || request.consumerId.length === 0))) return undefined;
    return request as unknown as RepairRequest;
  } catch {
    return undefined;
  }
}

function handoffKickoff(handoffPath: string, nextStep: string): string {
  return [
    "This is a fresh continuation session created from a durable handoff.",
    "Rebuild working context before doing more work:",
    "1. Read AGENTS.md/project instructions for this cwd.",
    `2. Read ${handoffPath}.`,
    "3. Read any active plan/status file referenced by the handoff.",
    `4. Continue with this next step: ${nextStep}`,
    "Treat durable project files as source of truth rather than previous chat history.",
  ].join("\n");
}

export default function contextLifecycleExtension(pi: ExtensionAPI): void {
  interface SessionBinding {
    sessionId: string;
    generationId: string;
    context: ExtensionContext;
  }

  let coordinator: ContextLifecycleCoordinatorV1 | undefined;
  let activeBinding: SessionBinding | undefined;
  let commandRequestSequence = 0;
  let handoffWatcherArmed = true;
  const handoffThreshold = ratioFromEnv(process.env.PI_CONTEXT_HANDOFF_THRESHOLD, DEFAULT_HANDOFF_THRESHOLD);
  const handoffRearmThreshold = ratioFromEnv(process.env.PI_CONTEXT_HANDOFF_REARM_THRESHOLD, DEFAULT_HANDOFF_REARM_THRESHOLD);

  const bindingFor = (ctx: ExtensionContext): SessionBinding | undefined => {
    const binding = activeBinding;
    return binding !== undefined && ctx.sessionManager.getSessionId() === binding.sessionId ? binding : undefined;
  };

  pi.on("session_start", (_event, ctx) => {
    coordinator?.dispose();
    const next = new ContextLifecycleCoordinatorV1();
    const publication = publishContextLifecycleV1(next.ownerInstanceId, next, {});
    next.attachPublication(publication);
    const binding = { sessionId: ctx.sessionManager.getSessionId(), generationId: "", context: ctx };
    const adapter: ManagedCompactionAdapter = {
      compact(options) {
        binding.context.compact(options);
      },
      sendResume(message) {
        pi.sendUserMessage(message, { deliverAs: "steer" });
      },
      appendLifecycleEntry(claim) {
        pi.appendEntry("pi-context-lifecycle", claim);
      },
    };
    binding.generationId = next.bindSession(binding.sessionId, adapter);
    next.restoreClaims(lifecycleClaims(ctx));
    coordinator = next;
    activeBinding = binding;
  });

  pi.on("agent_settled", (_event, ctx) => {
    const binding = bindingFor(ctx);
    if (binding === undefined) return;
    binding.context = ctx;
    coordinator?.onAgentSettled(binding.generationId);
  });

  pi.on("turn_end", (_event, ctx) => {
    const binding = bindingFor(ctx);
    if (binding === undefined) return;
    binding.context = ctx;
    const usage = ctx.getContextUsage();
    if (usage === undefined || usage.tokens === null || usage.contextWindow <= 0) return;
    const percent = usage.tokens / usage.contextWindow;
    if (!handoffWatcherArmed) {
      if (percent < handoffRearmThreshold) handoffWatcherArmed = true;
      return;
    }
    if (percent < handoffThreshold || getContextLifecycleSnapshotV1().phase !== "idle") return;
    try {
      pi.sendUserMessage(handoffInstruction(percent), { deliverAs: "steer" });
      handoffWatcherArmed = false;
    } catch {
      // Remain armed: a later settled turn may safely retry the advisory instruction.
    }
  });

  pi.on("session_before_compact", (event, ctx) => {
    const binding = bindingFor(ctx);
    if (binding === undefined) return;
    binding.context = ctx;
    coordinator?.onSessionBeforeCompact(binding.generationId, event.reason);
  });

  pi.on("session_compact", (event, ctx) => {
    const binding = bindingFor(ctx);
    if (binding === undefined) return;
    binding.context = ctx;
    coordinator?.onSessionCompact(binding.generationId, event.reason);
  });

  pi.on("message_start", (event, ctx) => {
    const binding = bindingFor(ctx);
    if (binding === undefined) return;
    binding.context = ctx;
    coordinator?.onMessageStart(binding.generationId, event.message);
  });

  pi.on("session_shutdown", (_event, ctx) => {
    const binding = bindingFor(ctx);
    if (binding === undefined) return;
    coordinator?.dispose();
    coordinator = undefined;
    activeBinding = undefined;
  });

  pi.registerCommand("context-lifecycle", {
    description: "Show redacted lifecycle status or apply an exact compare-and-swap repair.",
    handler: (args, ctx) => {
      const trimmed = args.trim();
      if (trimmed === "status" || trimmed.length === 0) {
        const status = {
          snapshot: getContextLifecycleSnapshotV1(),
          diagnostics: getContextLifecycleDiagnosticsV1().slice(-10),
        };
        if (ctx.hasUI) ctx.ui.notify(`Context lifecycle status: ${JSON.stringify(status)}`, "info");
        return Promise.resolve();
      }
      if (!trimmed.startsWith("repair ")) {
        if (ctx.hasUI) ctx.ui.notify("Usage: /context-lifecycle status | repair <exact-json-request>", "warning");
        return Promise.resolve();
      }
      const request = parseRepairRequest(trimmed.slice("repair ".length));
      if (request === undefined) {
        if (ctx.hasUI) ctx.ui.notify("Context lifecycle repair rejected: invalid or incomplete request.", "error");
        return Promise.resolve();
      }
      const disposition = repairContextLifecycleV1(request);
      if (ctx.hasUI) ctx.ui.notify(`Context lifecycle repair ${disposition.disposition}${disposition.disposition === "rejected" ? ` (${disposition.code})` : ` (${disposition.action})`}.`, disposition.disposition === "applied" ? "info" : "warning");
      return Promise.resolve();
    },
  });

  for (const commandName of ["self_compact", "self-compact"] as const) {
    pi.registerCommand(commandName, {
      description: "Request managed context compaction and one correlated resume turn.",
      handler: (args, ctx) => {
        const binding = bindingFor(ctx);
        if (binding !== undefined) binding.context = ctx;
        const disposition = binding === undefined
          ? { disposition: "rejected" as const, code: "session-unavailable" }
          : coordinator?.requestSelfCompactionFromCommand(args, `command:${commandName}:${++commandRequestSequence}`) ?? { disposition: "rejected" as const, code: "session-unavailable" };
        if (ctx.hasUI) {
          const level = disposition.disposition === "rejected" ? "warning" : "info";
          ctx.ui.notify(`Self compact ${disposition.disposition}${disposition.disposition === "rejected" ? ` (${disposition.code})` : ` as ${disposition.operationId}`}.`, level);
        }
        return Promise.resolve();
      },
    });
  }

  pi.registerTool({
    name: "handoff_new_session",
    label: "Handoff New Session",
    description: "Validate fresh-session handoff intent and return the command that must be invoked from user command context.",
    parameters: Type.Object({
      handoffPath: Type.Optional(Type.String({ maxLength: 4096, description: "Path to the durable handoff file." })),
      nextStep: Type.Optional(Type.String({ maxLength: 2000, description: "Exact next step for the replacement session." })),
    }),
    execute(_toolCallId, params) {
      const handoff = parseHandoffArgs(JSON.stringify(params));
      const command = `/handoff-new-session ${JSON.stringify(handoff)}`;
      return Promise.resolve(textToolResult(`Pi 0.80.6 permits session replacement only from command context. Invoke this command after the current turn settles; no slash-command text was queued automatically:\n${command}`));
    },
  });

  pi.registerCommand("handoff-new-session", {
    description: "Start one fresh session and continue from a durable handoff file.",
    handler: async (args, ctx) => {
      const handoff = parseHandoffArgs(args);
      const parentSession = ctx.sessionManager.getSessionFile();
      const result = await ctx.newSession({
        ...(parentSession === undefined ? {} : { parentSession }),
        withSession: async (newCtx) => {
          await newCtx.sendUserMessage(handoffKickoff(handoff.handoffPath, handoff.nextStep));
        },
      });
      if (result.cancelled && ctx.hasUI) ctx.ui.notify("Fresh handoff session was cancelled; the current session remains active.", "warning");
    },
  });

  pi.registerTool({
    name: "self_compact",
    label: "Self Compact",
    description: "Request managed context compaction after this agent run settles, then queue one resume turn.",
    promptSnippet: "Use self_compact when the operator asks you to compact before continuing. The lifecycle coordinator waits for this tool result and the settled run before compacting.",
    parameters: Type.Object({
      instructions: Type.Optional(Type.String({ description: "Optional focus for what the compaction summary and resumed turn should preserve and reload." })),
    }),
    execute(toolCallId, params) {
      const disposition = coordinator?.requestSelfCompaction(params.instructions ?? "", toolCallId) ?? { disposition: "rejected" as const, code: "session-unavailable" };
      if (disposition.disposition === "rejected") return Promise.resolve(textToolResult(`Self compact rejected (${disposition.code}); no compaction was started.`));
      if (disposition.disposition === "joined") return Promise.resolve(textToolResult(`Self compact request joined managed operation ${disposition.operationId}; compaction will start after this run settles.`));
      return Promise.resolve(textToolResult(`Self compact accepted as managed operation ${disposition.operationId}; compaction will start after this tool result and agent run settle.`));
    },
  });
}
