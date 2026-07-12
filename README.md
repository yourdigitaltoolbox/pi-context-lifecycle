# Pi Context Lifecycle

Extension-owned coordination for safe Pi context compaction, resume admission, and fresh-session handoff.

This repository is the lifecycle authority for the cross-extension contract tracked by [pi-context-lifecycle#1](https://github.com/yourdigitaltoolbox/pi-context-lifecycle/issues/1) and [ydtb-control-plane#66](https://github.com/yourdigitaltoolbox/ydtb-control-plane/issues/66). It targets Node 22.19.0 or newer and unmodified `@earendil-works/pi-coding-agent` 0.80.6 using documented extension and public SDK APIs only.

## Slice 1 surface

The current review branch contains the first managed tracer:

1. `self_compact` returns a normal tool result and records a request.
2. The process-global lifecycle authority closes managed wake admission.
3. The coordinator calls `ctx.compact()` exactly once, and only from `agent_settled`.
4. The managed call succeeds only when exactly one `reason: "manual"` durable `session_compact` event is observed before that coordinator-owned call invokes `onComplete`; zero, multiple, or automatic events fail closed.
5. The resume includes its operation/generation marker. Only an exact matching resumed user `message_start` establishes admission; handled, transformed, assistant, or unrelated messages/runs do not.
6. The matched resume run must reach `agent_settled` before an empty finite release cut completes and the authority returns to `idle`.

Failure recovery, automatic/native compaction adoption, aliases, repair commands, handoff, deadlines, and full consumer lanes are deliberately Slice 2 or later. Native manual `/compact` is not a managed entry point.

## Consumer API

```ts
import {
  admitWake,
  getContextLifecycleSnapshotV1,
  observeContextLifecycleV1,
  requestCompaction,
} from "@yourdigitaltoolbox/pi-context-lifecycle";
```

Helpers share one structural registry at `Symbol.for("yourdigitaltoolbox.pi-context-lifecycle.v1")`, even when independently resolved package copies are loaded. Observation atomically subscribes and returns the current sequenced snapshot. Every compaction request and wake admission must carry the current `generationId`; omission or mismatch is rejected before delegation. Both APIs return synchronous structured dispositions. Held payloads never enter the coordinator.

Managed profiles treat `unavailable` and `incompatible` admission as fail-closed configuration errors. A release permit is authorized by short-lived object identity; copying its diagnostic fields does not copy authority.

## Development

```sh
npm ci
npm run test:unit
npm run test:integration
npm run test:real-session
npm run check
npm run ci
npm pack --dry-run
```

The Slice 1 public-SDK harness uses a deterministic deferred fake provider and creates HOME, Pi agent, cwd, session, cache, socket, and artifact roots under the OS temporary directory. It never discovers or mutates the operator's live `~/.pi` profile. This source-level tracer test and `npm pack --dry-run` are chassis/package-shape evidence, not packaged-candidate proof; immutable archive installation and candidate manifests belong to later approved slices. Lifecycle diagnostics contain bounded identity/state metadata only—never prompts, summaries, tool output, credentials, or mesh bodies.
