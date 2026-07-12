# AGENTS.md — Pi Context Lifecycle

This repository owns only:

- the versioned process-local context lifecycle coordinator and helper API;
- managed `self_compact` tools/commands and resume behavior;
- command-context fresh-session handoff;
- lifecycle diagnostics/repair;
- the authoritative public-SDK packaged-candidate harness, tests, and docs.

It does not own subagent execution, Remote Pi transport, background-process supervision, YDTB application infrastructure, or Pi core.

Canonical plan and issue:

- `/Users/john/projects/ydtb/.epic/plans/2026-07-11-make-self-compaction-an-extension-coordinated-safe-lifecycle/`
- https://github.com/yourdigitaltoolbox/ydtb-control-plane/issues/66

Development rules:

- Keep Pi core unmodified; no private Pi imports or monkey patches.
- Use documented public Pi extension/SDK APIs.
- Never test against or mutate the operator's live `~/.pi` profile; use disposable roots.
- Do not log prompts, summaries, tool output, credentials, or mesh bodies in lifecycle diagnostics.
- Work on isolated review branches; do not commit implementation directly to `main`.
- Run focused tests while iterating and the repository's full `npm run check` gate before review-ready claims.
- Do not publish, tag, globally install, or disable prior extensions during Build.
