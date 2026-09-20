# UPSTREAM.md

This repository is a **divergent fork** of [`node-red/node-red`](https://github.com/node-red/node-red).

## Fork point

- Upstream repository: `node-red/node-red`
- Diverged at tag: `5.0.7`
- Diverged at commit: `1e85f1efb`

## New code placement

All new functionality added by this fork (the Temporal-backed execution
engine for Node-RED flows) lives entirely in a new, separate package:

```
packages/node_modules/@tbrandenburg/node-red-temporal-runtime/
```

This package depends on the upstream `@node-red/runtime`,
`@node-red/registry`, `@node-red/util`, and `@node-red/nodes` packages via
normal Node.js `require()` (see its `package.json` `dependencies`). No file
under `packages/node_modules/@node-red/` has ever been copied, vendored,
adapted, or otherwise modified as part of building this new package - the
execution seam it relies on (`RED.hooks`'s `preRoute`/`onComplete` events)
is a public API already exposed by the unmodified upstream runtime.

## Zero-diff verification

The following command must always return empty output, confirming no
upstream `@node-red/*` source was ever touched:

```bash
git diff --stat upstream/main -- packages/node_modules/@node-red/
```

This has been verified empty as of every milestone (M1 through M10) of the
Temporal runtime work tracked in issue #1.

## Merge/PR policy

- `upstream` (`node-red/node-red`) is read-only reference. Never propose,
  push, or merge anything there.
- Pull requests for this fork's own work are only ever raised against
  `tbrandenburg/node-red-temporal`'s `main` branch, using
  `gh pr create --repo tbrandenburg/node-red-temporal`.

## Sync history

Append one row per upstream merge, `@node-red/*` version bump, out-of-cycle
security patch, or (should it ever happen) copied/adapted upstream file. See
`AGENTS.md`'s "Keeping UPSTREAM.md current" for the routine.

| Date | Event | Upstream tag/commit | Zero-diff check | `npm test` |
|---|---|---|---|---|
| 2026-09-18 | Fork point | `5.0.7` (`1e85f1efb`) | empty | baseline: 2864 passing / 60 pending / 0 failing |
| 2026-09-20 | Intentional root `README.md` divergence (issue #41) | n/a — documentation-only change | empty for `packages/node_modules/@node-red/` and `packages/node_modules/node-red/` | unaffected |

Root `README.md` was rewritten from the inherited upstream Node-RED landing
page (hero logo, upstream badges, upstream community links) into a
project-specific landing page identifying this repository as
`node-red-temporal`, an independent experimental fork exploring
Temporal-backed durable execution — see issue #41. No file under
`packages/node_modules/@node-red/` or `packages/node_modules/node-red/` was
touched. Upstream Node-RED attribution and official links (nodered.org,
node-red/node-red) are retained in the new README. `AGENTS.md`'s
do-not-touch table now lists root `README.md` as a project-owned exception;
`CHANGELOG.md`, `API.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
`SECURITY.md`, `LICENSE`, and `CITATION.cff` remain upstream-owned and
unchanged. Future upstream syncs must not silently overwrite this file with
upstream's `README.md`.
