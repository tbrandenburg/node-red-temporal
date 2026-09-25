var should = require("should");
var path = require("path");
var { execFileSync } = require("child_process");

var BOOTSTRAP_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var FLOW = path.join(__dirname, "..", "fixtures", "http-response-flow.json");
var FIXTURE_NODES_DIR = path.join(__dirname, "..", "fixtures", "nodes", "runtime-httpnode-test");

/**
 * issue #102: `options.runtimeHttp` splits `runtime.httpNode`/
 * `runtime.server` (`RED.server`) onto a SECOND, independent HTTP server,
 * away from the Admin API server, so a runtime endpoint can be exposed on a
 * public/Docker-reachable interface without also exposing the Admin API
 * (issue #79's PoC). Run in a spawned child process for the same reason as
 * the sibling `bootstrap_*Mount_spec.js` files: `@node-red/editor-api`/
 * `@node-red/registry` hold process-wide singleton state that must stay
 * pristine for upstream's own editor-api unit tests elsewhere in the shared
 * mocha run.
 */
function runInChildProcess(script) {
    return execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 20000 });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - options.runtimeHttp (issue #102, spawned child process)", function() {
    this.timeout(20000);

    it("splits runtime.httpNode/runtime.server onto a dedicated server: admin-only routes 404 there, runtime routes 404 on the admin server", function() {
        var script = `
            const assert = require("assert");
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            // eslint-disable-next-line global-require
            const runtime = require(${JSON.stringify(require.resolve("../../../../../packages/node_modules/@node-red/runtime"))});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {
                    settings: { nodesDir: [${JSON.stringify(FIXTURE_NODES_DIR)}] },
                    adminApi: { port: 0, host: "127.0.0.1" },
                    runtimeHttp: { port: 0, host: "127.0.0.1" }
                });

                if (!handle.runtimeHttpAddress) {
                    throw new Error("expected runtimeHttpAddress to be set when options.runtimeHttp is given");
                }

                // 1. runtime.server is provably bound to the runtime HTTP
                //    server, not the admin one (issue #79's core finding).
                assert.strictEqual(typeof runtime.server, "object");
                assert.strictEqual(runtime.server.address().port, handle.runtimeHttpAddress.port);
                assert.notStrictEqual(runtime.server.address().port, handle.adminApiAddress.port);

                const adminBase = \`http://127.0.0.1:\${handle.adminApiAddress.port}\`;
                const runtimeBase = \`http://127.0.0.1:\${handle.runtimeHttpAddress.port}\`;

                // 2. admin-only routes reachable on admin, 404 on runtime HTTP.
                const flows = await handle.getCurrentFlow();
                const deployOnAdmin = await fetch(\`\${adminBase}/flows\`, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "Node-RED-API-Version": "v2",
                        "Node-RED-Deployment-Type": "full"
                    },
                    body: JSON.stringify({ flows, credentials: {} })
                });
                if (deployOnAdmin.status !== 200) {
                    throw new Error("expected v2 deploy on admin server to return 200, got " + deployOnAdmin.status);
                }

                const deployOnRuntime = await fetch(\`\${runtimeBase}/flows\`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json", "Node-RED-API-Version": "v2" },
                    body: JSON.stringify({ flows, credentials: {} })
                });
                if (deployOnRuntime.status !== 404) {
                    throw new Error("expected POST /flows on the runtime HTTP server to 404, got " + deployOnRuntime.status);
                }

                // 3. RED.httpNode fixture route reachable on runtime HTTP, 404 on admin.
                const fixtureOnRuntime = await fetch(\`\${runtimeBase}/fixture-runtime-httpnode\`);
                if (fixtureOnRuntime.status !== 200) {
                    throw new Error("expected GET /fixture-runtime-httpnode on runtime HTTP server to return 200, got " + fixtureOnRuntime.status);
                }
                const fixtureBody = await fixtureOnRuntime.json();
                if (fixtureBody.ok !== true) {
                    throw new Error("expected fixture route body {ok:true}, got " + JSON.stringify(fixtureBody));
                }

                const fixtureOnAdmin = await fetch(\`\${adminBase}/fixture-runtime-httpnode\`);
                if (fixtureOnAdmin.status !== 404) {
                    throw new Error("expected GET /fixture-runtime-httpnode on admin server to 404, got " + fixtureOnAdmin.status);
                }

                await handle.stop();
                console.log("OK");
            })().catch((err) => { console.error(err); process.exit(1); });
        `;
        var out = runInChildProcess(script);
        out.should.match(/OK/);
    });

    it("options.runtimeHttp omitted reproduces today's exact single-server behavior (regression guard)", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            // eslint-disable-next-line global-require
            const runtime = require(${JSON.stringify(require.resolve("../../../../../packages/node_modules/@node-red/runtime"))});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {
                    adminApi: { port: 0, host: "127.0.0.1" }
                });

                if (handle.runtimeHttpAddress !== null) {
                    throw new Error("expected runtimeHttpAddress to be null without options.runtimeHttp");
                }
                if (runtime.server.address().port !== handle.adminApiAddress.port) {
                    throw new Error("expected runtime.server to remain bound to the single admin server without options.runtimeHttp");
                }

                const base = \`http://127.0.0.1:\${handle.adminApiAddress.port}\`;
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

                await handle.stop();
                console.log("OK");
            })().catch((err) => { console.error(err); process.exit(1); });
        `;
        var out = runInChildProcess(script);
        out.should.match(/OK/);
    });

    it("rejects options.runtimeHttp when options.adminApi is not also given", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                try {
                    await bootstrap(${JSON.stringify(FLOW)}, { runtimeHttp: { port: 0, host: "127.0.0.1" } });
                    throw new Error("expected bootstrap() to reject options.runtimeHttp without options.adminApi");
                } catch (err) {
                    if (!/requires options.adminApi/.test(err.message)) {
                        throw err;
                    }
                }
                console.log("OK");
            })().catch((err) => { console.error(err); process.exit(1); });
        `;
        var out = runInChildProcess(script);
        out.should.match(/OK/);
    });
});
