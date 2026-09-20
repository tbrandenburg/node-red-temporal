var should = require("should");
var path = require("path");
var { execFileSync } = require("child_process");

var BOOTSTRAP_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var FLOW = path.join(__dirname, "..", "fixtures", "four-node-flow.json");

/**
 * issue #39 (M2): exercising `options.adminApi` requires booting a real
 * `@node-red/editor-api` Admin API server. That module (and
 * `@node-red/registry`) hold PROCESS-WIDE singleton state that upstream's
 * own editor-api unit tests (test/unit/@node-red/editor-api/**) also rely
 * on being pristine - doing this in-process inside the shared mocha run
 * (`npm run mocha:core`) was observed to permanently break ~60 unrelated,
 * later-running tests for the rest of that run (see bootstrap_spec.js's
 * comment and this repo's Lessons Learned on never patching shared
 * singleton state). Running the real boot-and-deploy round trip in a
 * throwaway CHILD PROCESS (like bin_spec.js's spawned CLI tests) verifies
 * the exact same behavior without any risk of cross-file contamination.
 */
function runInChildProcess(script) {
    return execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 20000 });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - options.adminApi (issue #39 M2, spawned child process)", function() {
    this.timeout(20000);

    it("serves the stock v2 Admin API POST /flows route with the editor disabled, and never a second editor UI", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, { adminApi: { port: 0, host: "127.0.0.1" } });
                const port = handle.adminApiAddress.port;

                const editorRes = await fetch(\`http://127.0.0.1:\${port}/\`);
                if (editorRes.status === 200) {
                    throw new Error("expected disableEditor:true to NOT serve an editor index page, got 200");
                }

                const flows = await handle.getCurrentFlow();
                const deployRes = await fetch(\`http://127.0.0.1:\${port}/flows\`, {
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
});
