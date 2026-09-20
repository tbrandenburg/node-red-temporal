var should = require("should");
var path = require("path");
var { execFileSync } = require("child_process");

var BOOTSTRAP_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var FLOW = path.join(__dirname, "..", "fixtures", "four-node-flow.json");
var FIXTURE_NODES_DIR = path.join(__dirname, "..", "fixtures", "nodes", "runtime-admin-test");

/**
 * issue #60: `bootstrap.js`'s `options.adminApi` path booted the real
 * `@node-red/runtime` and `@node-red/editor-api`, but never performed the
 * final `editorApi.httpAdmin.use(runtime.httpAdmin)` mount stock Node-RED
 * (`node-red/lib/red.js`) does - so node-owned `RED.httpAdmin` routes (e.g.
 * core Inject's `POST /inject/:id`, or any node's own custom route) were
 * registered but never actually reachable through B's Admin API server.
 *
 * Run in a spawned child process for the same reason as
 * `bootstrap_adminApi_spec.js`: `@node-red/editor-api`/`@node-red/registry`
 * hold process-wide singleton state that must stay pristine for upstream's
 * own editor-api unit tests elsewhere in the shared mocha run.
 */
function runInChildProcess(script) {
    return execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 20000 });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - options.adminApi mounts runtime.httpAdmin (issue #60, spawned child process)", function() {
    this.timeout(20000);

    it("exposes node-owned RED.httpAdmin routes (core Inject + a generic fixture node) while keeping disableEditor:true and the stock /flows deploy route working", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {
                    settings: { nodesDir: [${JSON.stringify(FIXTURE_NODES_DIR)}] },
                    adminApi: { port: 0, host: "127.0.0.1" }
                });
                const port = handle.adminApiAddress.port;
                const base = \`http://127.0.0.1:\${port}\`;

                // disableEditor:true must still hold - no editor HTML side effect.
                const editorRes = await fetch(\`\${base}/\`);
                if (editorRes.status === 200) {
                    throw new Error("expected disableEditor:true to NOT serve an editor index page, got 200");
                }

                // Core node-owned route (Inject, "n1" in the fixture flow):
                // must invoke the live node path, not a stock 404.
                const injectRes = await fetch(\`\${base}/inject/n1\`, { method: "POST" });
                if (injectRes.status !== 200) {
                    throw new Error("expected POST /inject/n1 to return 200 (live node), got " + injectRes.status);
                }

                // Same route, unknown node id: stock 404 must still apply.
                const unknownInjectRes = await fetch(\`\${base}/inject/does-not-exist\`, { method: "POST" });
                if (unknownInjectRes.status !== 404) {
                    throw new Error("expected POST /inject/does-not-exist to return 404, got " + unknownInjectRes.status);
                }

                // Generic fixture node's own RED.httpAdmin route: proves the
                // seam is generic, not an Inject special case.
                const fixtureRes = await fetch(\`\${base}/fixture-runtime-admin\`);
                if (fixtureRes.status !== 200) {
                    throw new Error("expected GET /fixture-runtime-admin to return 200, got " + fixtureRes.status);
                }
                const fixtureBody = await fixtureRes.json();
                if (fixtureBody.ok !== true) {
                    throw new Error("expected fixture route body {ok:true}, got " + JSON.stringify(fixtureBody));
                }

                // Existing editor-api Admin route (POST /flows) must be unaffected.
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

                await handle.stop();
                console.log("OK");
            })().catch((err) => { console.error(err); process.exit(1); });
        `;
        var out = runInChildProcess(script);
        out.should.match(/OK/);
    });

    it("does NOT mount runtime.httpAdmin when options.adminApi is absent (behavior unchanged)", function() {
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
