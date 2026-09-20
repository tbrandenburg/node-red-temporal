# node-red-temporal

[![CI](https://github.com/tbrandenburg/node-red-temporal/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/tbrandenburg/node-red-temporal/actions/workflows/tests.yml)
![Status](https://img.shields.io/badge/status-early%20alpha-orange)
![Node-RED](https://img.shields.io/badge/Node--RED-5.0.7-8F0000)
![Temporal JS](https://img.shields.io/badge/Temporal%20JS-1.24.0-000000)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Temporal-backed durable execution for Node-RED flows using unmodified Node-RED nodes and routing.**

> [!IMPORTANT]
> **This is not the official Node-RED repository or distribution.**
> `node-red-temporal` is an independent experimental fork of [node-red/node-red](https://github.com/node-red/node-red), built to explore running Node-RED flows with [Temporal](https://temporal.io/) as the durable execution scheduler.
>
> Looking for standard Node-RED? Start at [nodered.org](https://nodered.org/) or [node-red/node-red](https://github.com/node-red/node-red).

> [!WARNING]
> **Early alpha.** The core execution model and a representative compatibility corpus are working, but this project is not yet tuned or recommended for production workloads. See [Known limitations](#known-limitations).

Keep the Node-RED things that already work well:

- the Node-RED flow format;
- real Node-RED node implementations and config nodes;
- credentials and context stores;
- source connections such as Inject and MQTT;
- Node-RED's own route resolution, including Link, Catch, Complete and nested subflows;
- Node-RED's normal flow deployment lifecycle.

Use Temporal for the part that benefits from durable orchestration:

- durable workflow progress and history;
- downstream node scheduling;
- Activity retries;
- branch/fan-in scheduling;
- recovery when Workflow or Activity workers restart.

The design rule is simple:

> **Node-RED decides where a message should go. Temporal decides when that destination runs.**

## Quick start

### Prerequisites

- Node.js **>= 22.9**
- the [Temporal CLI](https://docs.temporal.io/cli) available on your `PATH`
- `make` for the convenience demo commands below

Clone and install:

```bash
git clone https://github.com/tbrandenburg/node-red-temporal.git
cd node-red-temporal
npm install
```

Start the Temporal dev server and the demo worker:

```bash
make demo-run
```

The demo contains a scheduled Inject source, so Workflow Executions start automatically.

Open the Temporal Web UI:

```text
http://localhost:8233
```

Or trigger one immediately:

```bash
make demo-start
```

Check or stop the demo:

```bash
make demo-status
make demo-stop
```

For the full CLI, split-worker deployment, MQTT gate, recovery demo, and serialization behavior, see the [runtime package README](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/README.md).

## How it works

```mermaid
flowchart LR
    S["Node-RED source<br/>Inject / MQTT / ..."]
    W["Temporal Workflow"]
    A["executeNode Activity"]
    N["Live Node-RED node"]
    R["Node-RED resolves destination"]
    C["Capture seam"]

    S --> C
    C -->|"start Workflow"| W
    W -->|"schedule"| A
    A --> N
    N --> R
    R --> C
    C -->|"destinationId"| W
```

The integration uses Node-RED's public runtime hooks. When a live node sends a message, Node-RED performs its normal routing first. The Capture layer records the resolved destination and suppresses that external local hop. Temporal then schedules the destination as the next Activity.

There is no parallel routing engine trying to reinterpret Node-RED flow JSON.

### Ownership boundary

| Node-RED owns | Temporal owns |
| --- | --- |
| Node and config-node lifecycle | Durable downstream scheduling |
| Node registry and node implementations | Workflow progress/history |
| Credentials | Activity retries |
| Context/storage | Worker-restart recovery |
| Source connections and timers | Branch scheduling |
| Route resolution | Execution ordering across captured hops |
| Flow deploy/redeploy lifecycle | Durable orchestration state |

## What works today

The current alpha supports, among other things:

- autonomous source ingress;
- fan-out and multiple outputs;
- repeated sends on the same port;
- Link routing;
- Catch and Complete routing;
- Join/fan-in;
- nested subflows;
- finite loops with a runaway execution guard;
- credentials and shared config nodes;
- node, flow and global context;
- configurable persistent context stores;
- flow redeploy without process restart;
- explicit `flowVersion` protection for in-flight Workflows;
- combined or split Workflow/Activity worker processes;
- configurable Temporal address, namespace and task queues;
- worker restart/recovery.

See [COMPATIBILITY.md](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/COMPATIBILITY.md) for the exact evidence behind these statements.

## Compatibility evidence

The published early-alpha smoke matrix deliberately stays small rather than pretending to cover the entire Node-RED ecosystem.

Current measured corpus:

| Corpus | Result |
| --- | ---: |
| Node/runtime mechanisms | **8/8 PASS** |
| Flow patterns | **10/10 PASS** |
| Manual/external gates | **2/2 PASS** |
| `@tbrandenburg` unit suite recorded with the matrix | **156/156 passing** |

These numbers describe **only the published corpus**. They are not a claim that a corresponding percentage of all Node-RED nodes or community flows is supported.

Read the full matrix and limitations in [COMPATIBILITY.md](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/COMPATIBILITY.md).

## Deployment modes

For local development, one process can host both Temporal roles:

```text
node-red-temporal
├── Node-RED runtime + Activity Worker
└── Workflow Worker
```

For separate deployments:

```text
Workflow Worker
      │
      ▼
   Temporal
      ▲
      │
Node-RED runtime + Activity Worker
```

Only the Activity role boots Node-RED and owns source ingress. The Workflow role is deterministic and does not load Node-RED.

See the [runtime package README](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/README.md) for commands and configuration.

## Existing Node-RED editor

**Today:** the alpha runner is headless and the CLI/demo flow is the supported path.

**Next:** [issue #39](https://github.com/tbrandenburg/node-red-temporal/issues/39) tracks deploying from an existing Node-RED editor to a separate Temporal-backed runtime while keeping the normal Node-RED Deploy button and avoiding a second editor. This issue is currently **open and not yet implemented**.

The intended user experience is:

```text
existing Node-RED editor
        │
        │ normal Deploy
        ▼
remote node-red-temporal runtime
        │
        ▼
     Temporal
```

Until #39 is implemented, do not interpret this section as an available feature.

## Known limitations

This is an early alpha. Important boundaries are explicit:

- **Activities are at-least-once.** Killing a worker mid-Activity can repeat a side effect such as an already-issued HTTP request.
- **In-flight flow migration is not implemented.** Redeploying to a new `flowVersion` causes Workflows pinned to the old version to fail explicitly with `FLOW_VERSION_MISMATCH`.
- **Context durability is a Node-RED storage concern.** The default in-memory context is still lost on restart; configure a persistent Node-RED context store when required.
- **Not every JavaScript object is a durable message.** Circular objects, functions and live sockets/streams cannot safely cross the Temporal serialization boundary. Buffer/Date/Error behavior is documented in the runtime package README.
- **HTTP In / HTTP Response bridging is not implemented.** Live `req`/`res` objects are outside the current durable message boundary.
- **Hooks are process-global.** Run one Capture instance per Node-RED Activity-worker process.
- **Performance is not yet characterized.** No production throughput, batching or autoscaling guidance is claimed.

For the detailed serialization table and recovery semantics, see the [runtime package README](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/README.md).

## Repository map

| Path | Purpose |
| --- | --- |
| [`packages/node_modules/@tbrandenburg/node-red-temporal-runtime/`](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/) | Temporal-backed Node-RED runtime integration |
| [runtime README](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/README.md) | Detailed architecture, CLI and operational documentation |
| [COMPATIBILITY.md](packages/node_modules/@tbrandenburg/node-red-temporal-runtime/COMPATIBILITY.md) | Small published compatibility smoke matrix |
| [AGENTS.md](AGENTS.md) | Architecture rules and protected upstream boundaries |
| [UPSTREAM.md](UPSTREAM.md) | Fork point and upstream-sync audit trail |

## Upstream relationship

This repository is a divergent fork of [node-red/node-red](https://github.com/node-red/node-red), originally forked from Node-RED **5.0.7**.

The Temporal integration lives under:

```text
packages/node_modules/@tbrandenburg/node-red-temporal-runtime/
```

The upstream Node-RED runtime, registry, nodes and editor packages remain protected from project-specific modification. The execution seam uses Node-RED's existing public hook API rather than maintaining a private fork of its routing internals.

See [UPSTREAM.md](UPSTREAM.md) for the exact fork/sync policy.

## Development

Install dependencies and run the repository test suite:

```bash
npm install
npm test
```

Run the faster core suite:

```bash
npm run mocha:core
```

Before accepting runtime changes, the protected upstream source paths must remain unchanged:

```bash
git diff --stat upstream/main -- \
  packages/node_modules/@node-red/ \
  packages/node_modules/node-red/
```

That command should produce no output.

Project-specific implementation and tests belong under the `@tbrandenburg` package/test trees. Read [AGENTS.md](AGENTS.md) before contributing.

## Contributing

Issues and pull requests for **node-red-temporal** belong in this repository:

- [Issues](https://github.com/tbrandenburg/node-red-temporal/issues)
- [Pull requests](https://github.com/tbrandenburg/node-red-temporal/pulls)

Do not open node-red-temporal pull requests against the upstream `node-red/node-red` repository.

For standard Node-RED bugs, usage questions or contributions unrelated to this Temporal integration, use the official [Node-RED project](https://github.com/node-red/node-red) and [Node-RED documentation](https://nodered.org/docs/).

## License and acknowledgements

This fork remains licensed under the [Apache License 2.0](LICENSE).

[Node-RED](https://nodered.org/) is an OpenJS Foundation project and provides the runtime, editor, flow format and node ecosystem this work builds on.

[Temporal](https://temporal.io/) provides the durable execution platform used for Workflow and Activity scheduling.

**node-red-temporal is an independent experimental project and is not the official repository for Node-RED or Temporal.**
