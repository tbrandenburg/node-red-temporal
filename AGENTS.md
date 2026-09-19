# AGENTS.md

## Mission

We are building a Temporal-backed execution engine for Node-RED workflows.

- Preserve Node-RED editor, flow format, node ABI, registry, and reusable node logic wherever practical.
- Temporal must own orchestration, scheduling, durable progress, retries, timers, and recovery.
- Keep upstream Node-RED changes minimal and concentrated behind clear execution seams.
- Prefer adapters and scheduler abstractions over rewriting Node-RED components.
- Maintain compatibility with upstream `@node-red/*` packages whenever possible.
- The first milestone is a simple Node-RED flow executed durably as a Temporal Workflow.
- Validate behavior against the standard Node-RED runtime with compatibility tests.
- Keep the fork easy to merge with upstream and avoid unnecessary architectural divergence.

## Repository context

This is a **divergent fork** of `node-red/node-red`, forked at tag `5.0.7` (`1e85f1efb`).

| Remote | Target |
|---|---|
| `origin` | `tbrandenburg/node-red-temporal` |
| `upstream` | `node-red/node-red` (read-only reference) |

> [!CAUTION]
> **Never open a pull request towards `node-red/node-red`.**
> `gh pr create` on a fork defaults to the **parent** repository. Always pass
> `--repo tbrandenburg/node-red-temporal` and verify `--base` before creating a PR.
> `gh repo set-default tbrandenburg/node-red-temporal` is configured as a guard — do not change it.

> [!CAUTION]
> **PRs may only be raised against this fork (`tbrandenburg/node-red-temporal`), never against `upstream`.**
> Merging is only allowed into this fork's `main`. `upstream` is read-only reference — never propose,
> push, or merge anything there, regardless of CI status or how low-risk a change appears.

## The execution seam

Node-RED already exposes the routing seam as a **public hook API**. `Flow.send()` routes through
`onSend` → `preRoute` → `preDeliver` → `postDeliver`, and each stage aborts cleanly when a hook
calls `done(false)` (`packages/node_modules/@node-red/runtime/lib/flows/Flow.js:806-815`;
`packages/node_modules/@node-red/util/lib/hooks.js:171`).

The entire scheduler seam is therefore:

```js
RED.hooks.add("preRoute.temporal", (sendEvent, done) => {
    capture(sendEvent);   // { source.id, source.port, destination.id, msg }
    done(false);          // suppress local delivery; Temporal decides the next hop
});
```

Completion and error capture are likewise free: `onComplete` already fires with `{msg, error, node}`
(`packages/node_modules/@node-red/runtime/lib/nodes/Node.js:134`).

### Hard constraint

**0 files changed under `packages/node_modules/@node-red/`.**

```bash
git diff --stat upstream/main -- packages/node_modules/@node-red/   # must be empty
```

If this is ever non-empty, the wrong seam was chosen. **Stop and re-evaluate** rather than
accepting the diff. All new code lives in a separate package that depends on upstream
`@node-red/runtime`, `@node-red/registry`, `@node-red/util`, `@node-red/nodes`.

## Architecture rules

- **Nodes are long-lived.** Start the flow once per worker through the standard runtime and leave
  all nodes alive and wired. Do **not** instantiate a node per Activity call: node creation requires
  a live `Flow` with context, env and credentials (`flows/Flow.js:203-290`), and config nodes resolve
  at construction via `flow.getNode` (`flows/Flow.js:426-453`).
- **Activities are thin.** An Activity delivers one message to one live node and returns what that
  node sent. It never routes.
- **Workflows are deterministic.** No `Date.now`, `Math.random`, filesystem or node-config reads in
  workflow code.
- **Pass the wire graph, not node configs**, into workflow input. The worker already has the configs.
- **`flowVersion` (content hash) is mandatory**, not deferred. Redeploying a flow otherwise
  invalidates in-flight workflow replays.
- **Rewrite `_msgid` per Activity invocation.** `_msgid` is preserved across `send()`
  (`nodes/Node.js:397`), so a naive msgid-keyed correlation map collides on multi-hop.

## Known limitations (state these explicitly; never paper over them)

- Temporal Activities are **at-least-once**. A worker kill mid-Activity re-executes side-effecting
  nodes such as `http request`. Recovery demos must kill **between** Activities and say so.
- Hooks are a process-global singleton, not per-flow.
- Node and flow context remain worker-local and are lost on worker restart.

## Working agreements

- Evidence first: never claim a milestone is done without pasted command output or a screenshot.
- Prove the risky part standalone before integrating. The capture layer is the only real risk;
  build and test it with **no Temporal involved**.
- No mocks in E2E tests: real flow, real Temporal server.
- Respect the LOC budget in the milestone plan. If edge cases blow the budget, **cut scope**, do not
  grow the diff.
- Keep a baseline: root `npm test` must stay green and match the recorded pre-change counts.
- Record any upstream file you are ever forced to copy or adapt in `UPSTREAM.md`, with the source tag.

## Commands

```bash
npm install
npm test              # build + verify-deps + lint + coverage
npm run mocha:core    # runtime/unit tests only, faster
npm start             # stock Node-RED
```

### Temporal demo (`make`)

Convenience targets wrapping the Temporal-backed demo's dev-server + worker
lifecycle (`packages/node_modules/@tbrandenburg/node-red-temporal-runtime`),
so it doesn't need 3 manually-managed terminals. PID/log files live under
`/tmp/node-red-temporal-demo/`, not in the repo.

| Target | Description |
|---|---|
| `make demo-run` | Start (or reuse) a Temporal dev server, then start the demo worker — both detached. Prints the Web UI URL and worker log path. |
| `make demo-start` | Trigger a new workflow execution against the running demo (`Inject → HTTP Request → Change → Debug`). |
| `make demo-status` | Check whether the Temporal server and the demo worker are up. |
| `make demo-stop` | Stop the demo worker; stops the Temporal dev server too, but only if `demo-run` started it (a reused, externally-started server is left running). |

## Current work

Milestone plan and acceptance criteria: issue
[#1 — MVP: Temporal-backed Node-RED flow execution (WSJF #1)](https://github.com/tbrandenburg/node-red-temporal/issues/1).

## Lessons Learned

- 2026-09-19: Pitfall: M2's capture layer patched the shared `Node.prototype.error` for the whole install()/uninstall() lifetime; a full-suite run (all specs in one mocha process) showed this leaking across unrelated node test files, causing ~80 cascading unrelated timeouts. Prevention: never patch a shared prototype/class-wide method as a global toggle — scope any such patch to a single instance and a single invocation, always restored on every exit path (resolve/reject/timeout), and verify with a full-suite run (not just the new spec file in isolation) before accepting a milestone that touches Node-RED's shared runtime classes.
