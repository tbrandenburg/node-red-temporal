var should = require("should");
var path = require("path");
var { execFileSync } = require("child_process");

var BOOTSTRAP_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var FLOW = path.join(__dirname, "..", "fixtures", "four-node-flow.json");
var FIXTURE_NODES_DIR = path.join(__dirname, "..", "fixtures", "nodes", "runtime-httpnode-test");

/**
 * issue #64 (P4): `bootstrap.js`'s `options.adminApi` path never mounted
 * `runtime.httpNode` (the node-owned "node" HTTP app - the same seam stock
 * `http in` nodes and FlowFuse Dashboard 2.0's `ui-base` config node use to
 * serve their own content) onto the runner's Admin API server, and left
 * `settings.httpNodeRoot` completely unset instead of defaulting it to `/`
 * the way stock `node-red/lib/red.js` does. Nodes that branch on
 * `RED.settings.httpNodeRoot !== false` (e.g. `ui-base`) therefore took the
 * "enabled" code path but found nothing actually listening, causing a
 * runtime crash inside the node's own constructor (unrelated to any
 * node-red-temporal-specific logic).
 *
 * Run in a spawned child process for the same reason as
 * `bootstrap_runtimeAdminMount_spec.js`: `@node-red/editor-api`/
 * `@node-red/registry` hold process-wide singleton state that must stay
 * pristine for upstream's own editor-api unit tests elsewhere in the shared
 * mocha run.
 */
function runInChildProcess(script) {
    return execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 20000 });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - options.adminApi mounts runtime.httpNode (issue #64, spawned child process)", function() {
    this.timeout(20000);

    it("exposes a generic RED.httpNode fixture route, keeps existing Admin routes and disableEditor:true working", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {
                    settings: { nodesDir: [${JSON.stringify(FIXTURE_NODES_DIR)}] },
                    adminApi: { port: 0, host: "127.0.0.1" }
                });
                const port = handle.adminApiAddress.port;
                const base = \`http://127.0.0.1:\${port}\`;

                // 1. a generic RED.httpNode fixture route is reachable.
                const fixtureRes = await fetch(\`\${base}/fixture-runtime-httpnode\`);
                if (fixtureRes.status !== 200) {
                    throw new Error("expected GET /fixture-runtime-httpnode to return 200, got " + fixtureRes.status);
                }
                const fixtureBody = await fixtureRes.json();
                if (fixtureBody.ok !== true) {
                    throw new Error("expected fixture route body {ok:true}, got " + JSON.stringify(fixtureBody));
                }

                // 2. existing Admin routes remain reachable (POST /flows deploy, v2).
                const flows = await handle.getCurrentFlow();
                const deployRes = await fetch(\`\${base}/flows\`, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "Node-RED-API-Version": "v2",
                        "Node-RED-Deployment-Type": "full"
                    },
                    body: JSON.stringify({ flows, credentials: {} })
                });
                if (deployRes.status !== 200) {
                    throw new Error("expected v2 deploy to return 200, got " + deployRes.status);
                }
                const body = await deployRes.json();
                if (!body.rev) {
                    throw new Error("expected deploy response to include a rev");
                }

                // 3. editor UI remains disabled - no editor HTML side effect.
                const editorRes = await fetch(\`\${base}/\`);
                if (editorRes.status === 200) {
                    throw new Error("expected disableEditor:true to NOT serve an editor index page, got 200");
                }

                await handle.stop();
                console.log("OK");
            })().catch((err) => { console.error(err); process.exit(1); });
        `;
        var out = runInChildProcess(script);
        out.should.match(/OK/);
    });

    it("does NOT mount runtime.httpNode when options.adminApi is absent (behavior unchanged)", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {});
                if (handle.adminApiAddress !== null) {
                    throw new Error("expected adminApiAddress to be null without options.adminApi");
                }
                await handle.stop();
                console.log("OK");
            })().catch((err) => { console.error(err); process.exit(1); });
        `;
        var out = runInChildProcess(script);
        out.should.match(/OK/);
    });
});
