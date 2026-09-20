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

## Folder structure and do-not-touch zones

To maximize mergability with upstream, **all new work lives under `packages/node_modules/@tbrandenburg/`
and its matching test tree**. Everything else under `packages/` and root-level Node-RED scaffolding is
upstream-owned and must stay byte-for-byte identical to `upstream/main`.

| Folder / file | Contents | Touch? |
|---|---|---|
| `packages/node_modules/@node-red/` | runtime, registry, util, editor-api, editor-client, nodes | ❌ DO NOT TOUCH |
| `packages/node_modules/node-red/` | CLI/settings entrypoint (`bin/`, `lib/`, `red.js`) | ❌ DO NOT TOUCH |
| `packages/node_modules/@tbrandenburg/node-red-temporal-runtime/` | all new runtime code (`bin/`, `lib/`, `demo/`, `package.json`) | ✅ ALL new code goes here |
| `test/unit/@node-red/` | upstream unit tests | ❌ DO NOT TOUCH |
| `test/unit/node-red/` | upstream unit tests | ❌ DO NOT TOUCH |
| `test/unit/@tbrandenburg/node-red-temporal-runtime/` | our specs + fixtures | ✅ our tests go here |
| `test/nodes/`, `test/editor/`, `test/resources/` | upstream E2E/editor/node test suites | ❌ DO NOT TOUCH |
| `scripts/` | upstream release/build tooling | ❌ DO NOT TOUCH |
| `eslint.config.js`, `.mocharc.json`, `.nycrc.json`, `nodemon.json`, `jsdoc.json` | upstream lint/test/build config | ❌ DO NOT TOUCH |
| `package.json`, `package-lock.json` | dependency manifests | ⚠️ ADD-ONLY — append new `@tbrandenburg/*` deps only, never remove/reorder/version-bump existing upstream deps |
| `README.md` | root project landing page | ✅ project-owned exception — intentionally diverged from upstream for fork identity (see `UPSTREAM.md`); update freely |
| `CHANGELOG.md`, `API.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `LICENSE`, `CITATION.cff` | upstream project docs/metadata | ❌ DO NOT TOUCH |
| `Makefile` | demo lifecycle targets | ⚠️ ADD-ONLY — `demo-run`/`demo-start`/`demo-status`/`demo-stop` targets only |
| `AGENTS.md`, `UPSTREAM.md` | our own docs, don't exist upstream | ✅ update freely |
| `.agents/`, `.playwright-mcp/`, `.worktrees/` (gitignored) | local tooling/scratch | ✅ ours, never upstream-relevant |

**Rule of thumb:** if a path doesn't start with `@tbrandenburg/`, isn't `AGENTS.md`/`UPSTREAM.md`/root
`README.md`, and isn't a Makefile-only addition, don't edit it. Before every commit:

```bash
git diff --stat upstream/main -- packages/node_modules/@node-red/ packages/node_modules/node-red/ \
  test/unit/@node-red/ test/unit/node-red/ test/nodes/ test/editor/ test/resources/ scripts/   # must be empty
```

## Keeping `UPSTREAM.md` current

`UPSTREAM.md` is the fork's audit trail. Update it (append to its "Sync history" section) on
every one of these events — never silently:

| Event | Update `UPSTREAM.md` with |
|---|---|
| Merging `upstream/main` into `main` | New upstream tag/commit synced to, date, and a re-confirmation the zero-diff check still passes |
| Bumping the pinned `@node-red/*` dependency versions | Old → new version, and which upstream tag they now match |
| Any file ever copied/vendored/adapted from upstream (should be rare/never) | The source file, its upstream path, and the tag it was taken from |
| Security patch merged out-of-cycle from upstream | Same as a regular merge, flagged as security-driven |

Routine for every upstream sync:

```bash
git fetch upstream --tags
git merge upstream/main
git diff --stat upstream/main -- packages/node_modules/@node-red/   # must be empty
npm test                                                            # must stay green
```

Then append one line to `UPSTREAM.md`'s "Sync history" table: date, upstream tag/commit merged to,
zero-diff result, test result. Do this even when the merge was trivial — the log's value is in being
complete, not just covering the hard cases. `UPSTREAM.md` is a **human-readable log only** — nothing
reads it programmatically; it is never parsed by CI or tooling.

### CI enforcement: `upstream-sync-status.yml`

Every PR is blocked (not just warned) if the latest `node-red/node-red` release tag is not yet an
ancestor of the PR's branch (`.github/workflows/upstream-sync-status.yml`, additive — does not
modify the do-not-touch `tests.yml`/`release.yml`). The source of truth is **git ancestry itself**
(`git merge-base --is-ancestor <latest-upstream-tag> HEAD`) — deterministic, requires no file to stay
in sync with reality, and cannot drift from `UPSTREAM.md` because it never reads it.

**Corrective action when blocked:** merge upstream into your own PR branch, then push again — the
check re-runs automatically:

```bash
git fetch upstream --tags
git merge upstream/<latest-tag>
git diff --stat upstream/main -- packages/node_modules/@node-red/   # must be empty
npm test                                                            # must stay green
```

Then append the new sync to `UPSTREAM.md`'s Sync history table as part of the same PR before
pushing — this both satisfies the check and keeps the audit trail complete in one step.

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
- **Node-RED's own resolved routing is authoritative (issue #13).** Each `executeNode` Activity
  result carries the `destinationId` Node-RED's `preRoute` hook already resolved for a `send()`; the
  Workflow enqueues that captured `destinationId` directly instead of re-deriving it from the raw
  wire graph. The minimal `nodeId -> wires` graph (`lib/wireGraph.js`) remains only as a fallback for
  callers without a captured `destinationId` — pass it into workflow input (never full node configs),
  but do not treat it as the primary routing source.
- **`flowVersion` (content hash) is mandatory**, not deferred. Redeploying a flow otherwise
  invalidates in-flight workflow replays.
- **Correlate Activity invocations by `invocationId`, never by `_msgid` (issue #12/#31).** `_msgid` is
  Node-RED message identity, not Temporal Activity-invocation identity, and is preserved across
  `send()` (`nodes/Node.js:397`) — a naive msgid-keyed correlation map collides whenever two
  concurrent invocations hit the same node with the same `_msgid`. `Capture.around()` instead runs
  each invocation inside a dedicated `AsyncLocalStorage`-carried `invocationId`, with a `msg`-identity
  fallback for deferred/batched `done()` calls (e.g. Node-RED's real `join` node) — see
  `lib/capture.js`'s docblock for the full correlation algorithm.
- **Both combined and split worker topologies are supported (issue #16).** `lib/worker.js` exposes
  `createActivityWorker` (boots Node-RED, installs `Capture`, owns source ingress, polls only the
  Activity Task Queue), `createWorkflowWorker` (registers only `executeFlow`, never boots Node-RED or
  installs `Capture`), and `createCombinedWorker` (both roles in one process — today's default). Only
  the Activity role may boot Node-RED/install `Capture`/originate source ingress.
- **Persistent Node-RED context stores work normally (issue #14).** `bootstrap.js` defaults to
  Node-RED's own real storage module (real credentials file, real settings/sessions/library storage)
  and forwards `options.settings.contextStorage` through to the real runtime unmodified — a caller
  configuring a persistent store (e.g. the built-in `localfilesystem` store) gets context that
  survives a worker restart. Context/config state is still worker-local process memory by default
  (the in-memory context store), and Temporal itself never makes context durable — only an explicitly
  configured persistent store does.

## Known limitations (state these explicitly; never paper over them)

- Temporal Activities are **at-least-once**. A worker kill mid-Activity re-executes side-effecting
  nodes such as `http request`. Recovery demos must kill **between** Activities and say so.
- Hooks are a process-global singleton, not per-flow.
- Context/config state is worker-local process memory by default; it survives a worker restart only
  if a persistent Node-RED context store (e.g. the built-in `localfilesystem` store) is explicitly
  configured (issue #14) — Temporal itself never makes context durable.
- `flowVersion` is pinned with no migration path: a redeploy that changes the content hash fails any
  in-flight Workflow still referencing the old `flowVersion` (non-retryable `FLOW_VERSION_MISMATCH`).
- Fan-in (two branches converging on the same downstream node) and finite loops now work (issues #24,
  #31, #34) — an unbounded/misconfigured loop still fails fast via the `maxNodeExecutions` guard
  rather than draining forever.

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
- 2026-09-19: Pitfall: issue #16 made `Worker.create()` open a real, eager `NativeConnection.connect()`; a pre-existing unit test (`deploy_spec.js`) only stubbed `Worker.create`, not the new connection call, so it passed locally (a real Temporal dev server happened to be reachable) but hung indefinitely in CI (no server reachable) — the identical shared-dev-server environment that made local validation convenient also masked the bug. Prevention: whenever adding a new real network/connection call to already-tested production code, grep every existing test exercising that code path and add the matching stub — do not rely on "it passed locally" when a long-lived local service is incidentally reachable.
- 2026-09-19: Pitfall: fixing #24's queue-drain scheduling deadlock (sequential await -> wave-based `Promise.all`) surfaced a second, independent `capture.js` ALS-attribution bug in the real Join node only once both fan-in branches were actually dispatched concurrently — the original bug had masked it entirely. Prevention: when a concurrency-related scheduling fix makes previously-unreachable code paths reachable, re-run the real end-to-end fixture (not just new mock-based unit tests) before declaring the issue fully fixed, and file any newly-exposed bug as its own issue rather than silently expanding scope.
- 2026-09-20: Pitfall: a docs-refresh subagent (#37) assigned to edit root `AGENTS.md` from an isolated worktree briefly edited the coordinator's root-worktree copy of the same file by mistake (both worktrees share the identical relative path `AGENTS.md`), caught only via a manual `git status` check. Prevention: when any subagent's assigned scope includes a file that also exists at the same relative path in the coordinator's own worktree, tell it explicitly to `pwd`/verify its cwd is the worktree path before every edit, and the coordinator must always `git status`/`git diff` its own tree immediately after every subagent with root-file access reports done — before trusting its handoff.
