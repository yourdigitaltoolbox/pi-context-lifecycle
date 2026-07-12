# Pi Context Lifecycle

Extension-owned coordination for safe Pi context compaction, resume admission, and fresh-session handoff.

This repository is the lifecycle authority for the cross-extension contract tracked by [pi-context-lifecycle#1](https://github.com/yourdigitaltoolbox/pi-context-lifecycle/issues/1) and [ydtb-control-plane#66](https://github.com/yourdigitaltoolbox/ydtb-control-plane/issues/66). It targets Node 22 and unmodified `@earendil-works/pi-coding-agent` 0.80.6 using documented extension and public SDK APIs only.

## Slice 1 surface

The current review branch contains the first managed tracer:

1. `self_compact` returns a normal tool result and records a request.
2. The process-global lifecycle authority closes managed wake admission.
3. The coordinator calls `ctx.compact()` exactly once, and only from `agent_settled`.
4. Durable `session_compact` success queues exactly one resume message.
5. Matching extension input and `agent_start` establish admission; `agent_settled` establishes the resume barrier.
6. An empty finite release cut completes and the authority returns to `idle`.

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

Helpers share one structural registry at `Symbol.for("yourdigitaltoolbox.pi-context-lifecycle.v1")`, even when independently resolved package copies are loaded. Observation atomically subscribes and returns the current sequenced snapshot. Compaction requests and wake admission return synchronous structured dispositions. Held payloads never enter the coordinator.

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

The public-SDK harness uses a deterministic deferred fake provider and creates HOME, Pi agent, cwd, session, cache, socket, and artifact roots under the OS temporary directory. It never discovers or mutates the operator's live `~/.pi` profile. Lifecycle diagnostics contain bounded identity/state metadata only—never prompts, summaries, tool output, credentials, or mesh bodies.
