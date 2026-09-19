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
