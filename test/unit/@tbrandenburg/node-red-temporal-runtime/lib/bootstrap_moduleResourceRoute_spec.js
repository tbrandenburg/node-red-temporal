var path = require("path");
var { execFileSync } = require("child_process");

var BOOTSTRAP_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var FLOW = path.join(__dirname, "..", "fixtures", "four-node-flow.json");
var UNSCOPED_FIXTURE_DIR = path.join(__dirname, "..", "fixtures", "nodes", "module-resource-test");
var SCOPED_FIXTURE_DIR = path.join(__dirname, "..", "fixtures", "nodes", "module-resource-scoped-test");

/**
 * issue #74: proves runner B's Admin API server serves the standard
 * Node-RED `/resources/<module>/<file>` route (stock behavior:
 * `@node-red/editor-api/lib/editor/ui.js`'s `moduleResource`, only ever
 * mounted inside the full editor app, which B never boots because
 * `disableEditor: true`) - both for an unscoped and a `@scope/package`
 * module name, resolved through the SAME real `@node-red/registry`
 * `resources/` directory convention, with 404s for unknown modules,
 * missing resources, and path-traversal attempts, and without ever
 * exposing the editor UI itself.
 *
 * Run in a spawned child process for the same process-wide-singleton
 * reason as the other `bootstrap_*` specs in this directory.
 */
function runInChildProcess(script) {
    return execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 20000 });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - module-resource route (issue #74, spawned child process)", function() {
    this.timeout(20000);

    it("serves unscoped and scoped module resources, 404s for unknown/missing/traversal, and keeps the editor UI disabled", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {
                    settings: { nodesDir: [${JSON.stringify(UNSCOPED_FIXTURE_DIR)}, ${JSON.stringify(SCOPED_FIXTURE_DIR)}] },
                    adminApi: { port: 0, host: "127.0.0.1" }
                });
                const port = handle.adminApiAddress.port;
                const base = \`http://127.0.0.1:\${port}\`;

                // 1. unscoped module resource: 200 + exact bytes + correct Content-Type.
                const unscopedRes = await fetch(\`\${base}/resources/node-red-temporal-test-module-resource/test-widget.js\`);
                if (unscopedRes.status !== 200) {
                    throw new Error("expected unscoped module resource to return 200, got " + unscopedRes.status);
                }
                const unscopedContentType = unscopedRes.headers.get("content-type") || "";
                if (!unscopedContentType.includes("javascript")) {
                    throw new Error("expected a javascript Content-Type, got " + unscopedContentType);
                }
                const unscopedBody = await unscopedRes.text();
                if (!unscopedBody.includes("test widget resource for issue #74")) {
                    throw new Error("expected the exact fixture resource bytes, got " + unscopedBody);
                }

                // 2. scoped (@scope/package) module resource: 200.
                const scopedRes = await fetch(\`\${base}/resources/@node-red-temporal-test/module-resource-scoped/test-widget.js\`);
                if (scopedRes.status !== 200) {
                    throw new Error("expected scoped module resource to return 200, got " + scopedRes.status);
                }
                const scopedBody = await scopedRes.text();
                if (!scopedBody.includes("scoped test widget resource for issue #74")) {
                    throw new Error("expected the exact scoped fixture resource bytes, got " + scopedBody);
                }

                // 3. unknown module: 404.
                const unknownModuleRes = await fetch(\`\${base}/resources/totally-unknown-module/test-widget.js\`);
                if (unknownModuleRes.status !== 404) {
                    throw new Error("expected unknown module to 404, got " + unknownModuleRes.status);
                }

                // 4. known module, missing resource: 404.
                const missingResourceRes = await fetch(\`\${base}/resources/node-red-temporal-test-module-resource/does-not-exist.js\`);
                if (missingResourceRes.status !== 404) {
                    throw new Error("expected missing resource to 404, got " + missingResourceRes.status);
                }

                // 5. path-traversal-style input: 404 (registry rejects paths escaping resources/).
                const traversalRes = await fetch(\`\${base}/resources/node-red-temporal-test-module-resource/../../../../etc/passwd\`);
                if (traversalRes.status !== 404) {
                    throw new Error("expected path-traversal-style input to 404, got " + traversalRes.status);
                }

                // 6. editor UI remains inaccessible (disableEditor: true still in force).
                const editorRes = await fetch(\`\${base}/\`);
                if (editorRes.status === 200) {
                    throw new Error("expected disableEditor:true to NOT serve an editor index page, got 200");
                }

                await handle.stop();
                console.log("OK");
            })().catch((err) => {
                console.error(err && err.stack || err);
                process.exit(1);
            });
        `;
        var output = runInChildProcess(script);
        output.should ? output.should.match(/OK/) : require("should")(output).match(/OK/);
    });

    it("does NOT mount the module-resource route when options.adminApi is absent (behavior unchanged)", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {});
                if (handle.adminApiAddress !== null) {
                    throw new Error("expected adminApiAddress to be null without options.adminApi");
                }
                await handle.stop();
                console.log("OK");
            })().catch((err) => { console.error(err && err.stack || err); process.exit(1); });
        `;
        var output = runInChildProcess(script);
        output.should ? output.should.match(/OK/) : require("should")(output).match(/OK/);
    });
});
