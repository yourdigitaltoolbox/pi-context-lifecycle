# Pi Context Lifecycle

Extension-owned coordination for safe Pi context compaction, resume admission, and fresh-session handoff.

This repository is the lifecycle authority for [pi-context-lifecycle#1](https://github.com/yourdigitaltoolbox/pi-context-lifecycle/issues/1) and [ydtb-control-plane#66](https://github.com/yourdigitaltoolbox/ydtb-control-plane/issues/66). It targets Node 22.19.0 or newer and unmodified `@earendil-works/pi-coding-agent` 0.80.6 through documented extension and public SDK APIs only.

The current `feat/context-lifecycle-v1` branch and draft PR are Build candidates, not a published or globally installed release.

## Supported boundary

The managed profile supports:

- the `self_compact` tool;
- `/self_compact` and `/self-compact` command aliases;
- requests joined through the public lifecycle registry, including future Remote Pi callers;
- Pi's native automatic threshold and overflow compaction;
- command-context `/handoff-new-session`.

Native manual `/compact` is an unmanaged escape hatch. The coordinator adopts it best-effort only after Pi emits `session_before_compact`; it cannot protect the earlier extension-invisible TUI interval and does not present that path as a safety guarantee.

## Lifecycle behavior

1. A tool request appends a bounded metadata-only claim and returns a normal tool result.
2. The coordinator closes managed wake admission and waits for the requesting agent run to settle.
3. It invokes `ctx.compact()` once. Successful managed completion requires exactly one matching durable manual `session_compact` event plus the operation-owned `onComplete` callback.
4. A successful self operation persists `resume-pending` and `resume-admitting`, sends one operation/generation-marked resume, and waits for the exact user `message_start` plus its `agent_settled` barrier.
5. Release captures one finite per-consumer/per-lane watermark/count cut. Drainers run under opaque, generation-bound permits in the exported order `failure-attention-decision`, `mesh-reply`, `mesh-unsolicited`, `subagent-success`, `background-notify`, `loop-tick`, then `cron-tick`. A consumer may own multiple lanes, and each acknowledgement must match the exact consumer, lane, handled watermark, and count within five seconds.
6. The operation returns to idle only after every captured lane is empty, submitted/durably represented, or an explicit block is recorded.

Managed `onError` releases the unchanged session without a resume and distinguishes cancellation errors from provider failure. An observed compaction abort signal is authoritative cancellation evidence for managed or automatic work. Automatic threshold/overflow success is authoritative only from `session_compact`; same-generation `agent_settled` after an observed automatic preflight and no success/abort event proves the enclosing attempt stopped without success. Public-SDK tests cover automatic threshold success, managed provider-failure ordering, and managed hook cancellation.

Observation deadlines warn after two minutes and block after ten minutes for compaction, warn after 30 seconds and block after 60 seconds for resume admission, and block one drainer after five seconds. Deadlines never unlock, resend, or fabricate success. Late current-operation terminal evidence can still resolve a compaction block.

## Consumer API

```ts
import {
  admitWake,
  CONTEXT_LIFECYCLE_RELEASE_LANES,
  getContextLifecycleSnapshotV1,
  observeContextLifecycleV1,
  registerContextLifecycleDrainerV1,
  repairContextLifecycleV1,
  requestCompaction,
} from "@yourdigitaltoolbox/pi-context-lifecycle";
```

Helpers share one structural registry at `Symbol.for("yourdigitaltoolbox.pi-context-lifecycle.v1")`, even when independently resolved package copies are loaded. Publication uses compare-and-swap ownership; observation atomically subscribes and returns the current sequenced snapshot; compaction and wake admission require the current session generation.

Held bodies never enter the coordinator. A drainer registers one exported `laneId` and captures only `{ watermark, heldCount }`; its permit carries that frozen cut, and its acknowledgement must prove lane, `handledThrough`, and `handledCount`. The registration key is consumer plus lane, so one consumer can own several accepted lanes without magic priorities. Copying permit fields does not copy its object-identity authority.

Managed profiles treat unavailable or incompatible authority as a visible fail-closed configuration error.

## Durable recovery and repair

The extension appends metadata-only custom entries for:

```text
requested → compacting → compacted → resume-pending → resume-admitting
→ resume-admitted → resume-settled → released
```

Terminal alternatives include `failed`, `cancelled`, and `blocked-unknown`. Entries contain owner, session, generation, operation, reason, boolean resume intent, state, and timestamp only—never prompts, focus text, summaries, tool output, credentials, or mesh bodies. An intent-changing join appends a new claim at the current durable state so owner replacement preserves resume intent while discarding sensitive focus text.

A replacement owner restores every nonterminal old-owner claim as blocked under a fresh generation. `/context-lifecycle status` exposes the redacted snapshot and bounded diagnostics. `/context-lifecycle repair <exact-json-request>` applies only an exact operation/session/generation/phase/snapshot-sequence compare-and-swap and supports:

- recognizing verified persisted resume admission/run evidence;
- retrying a restored `resume-pending` claim before any admission attempt;
- abandoning a restored ambiguous `resume-admitting` claim without resend after owner replacement;
- retrying the existing idempotent blocked drainer cut with a fresh opaque permit;
- abandoning a restored never-started `requested` operation after branch-validated owner replacement.

Restored `compacting`, `compacted`, or otherwise interrupted nonterminal states are not silently abandoned by that action; they return `fresh-session-required`. Caller assertion alone does not establish current-process quiescence. Persisted resume recognition additionally passes through the extension's session-entry verifier. Ambiguous current-owner admission never automatically resends.

## Fresh-session handoff

The `handoff_new_session` tool does **not** inject slash-command text. Pi 0.80.6 exposes replacement only in command context, so the tool returns the exact `/handoff-new-session` command for the operator to invoke after the current turn settles.

The command calls `newSession({ parentSession, withSession })` once and uses only the replacement context to send one kickoff naming the durable handoff and next step. Session shutdown/reload/replacement disposes the old registry owner; a later `session_start` publishes a fresh owner/generation, and old contexts, callbacks, timers, and permits remain inert.

## Candidate harness

`@yourdigitaltoolbox/pi-context-lifecycle/testing` exports:

- disposable HOME/Pi/cwd/session/cache/socket/artifact roots;
- a deterministic deferred fake provider, including provider failure;
- an immutable candidate-manifest validator;
- bounded redacted structured timelines;
- deterministic scenario and bounded soak drivers;
- a packaged-archive install/import smoke driver;
- an exact four-archive candidate installer/matrix/soak/rollback driver; and
- the typed `loadExactCandidateProbe` bridge for archive-derived consumer test subpaths.

`runExactCandidate` accepts only an external candidate root and a manifest with the lifecycle, pi-subagents, Remote Pi, and background-task archives in that order. It verifies every archive digest, npm-installs only those archives into a new candidate-root runtime, and uses documented `pi install -l --approve <archive-derived-package-directory>` operations in a disposable profile. Its matrix lists all 16 Layer-3 scenarios, records redacted receipts, bounds the soak, and, when requested, removes candidate registrations with documented Pi CLI operations before deleting only the candidate-created empty project settings directory to restore the initial absent-settings snapshot.

For the integrated scenarios, every consumer package exposes an explicit `./testing` subpath with `createExactCandidateProbe({ session, seed, packageDirectory })`. The lifecycle driver resolves that subpath from the archive-derived package directory only. The probe permits only typed boundary injections and immutable opaque identifier/outcome receipts; it must not expose payloads or mutable adapter/coordinator state.

The manifest validator requires full commit/tree identities, SHA-256 archive/lock digests, exact Pi version/integrity, a deterministic scenario seed, relative archive paths, and a complete unique package order. Candidate-specific values and outputs remain outside lifecycle source so they cannot create a self-referential commit identity.

The real-session suite uses public Pi SDK/session APIs and disposable roots. It currently proves the managed success path, provider failure, managed cancellation, automatic threshold compaction, successful-response overflow compaction, exact resume admission, rejected handled/transformed/unrelated resume paths, and command-context handoff routing. Final four-package consumer races and the 100-cycle exact-candidate soak are completed after consumer adapters are pinned in later Build slices.

## Development

```sh
npm ci
npm run test:unit
npm run test:integration
npm run test:real-session
npm run test:packaged-smoke
npm run test:candidate
npm run check
npm run ci
npm pack --dry-run
```

`npm run test:packaged-smoke` builds and packs the candidate, installs the tarball under disposable roots, and imports its public main and extension exports. No test discovers or mutates the operator's live `~/.pi` profile, uses private Pi imports, monkey-patches core, publishes packages, or performs deployment work.
