var path = require("path");
var { execFileSync } = require("child_process");

var BOOTSTRAP_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var RUNTIME_EVENTS_MODULE = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/remoteRuntimeEvents.js");
var REDUTIL_MODULE = require.resolve("../../../../../packages/node_modules/@node-red/util");
var FLOW = path.join(__dirname, "..", "fixtures", "four-node-flow.json");

/**
 * issue #59: proves the runtime-events streaming route is actually mounted
 * on runner B's real Admin API server (not just unit-tested in isolation),
 * and that a real receiver connecting over real HTTP re-emits the debug
 * comms event on its own events singleton. Spawned in a child process for
 * the same process-wide-singleton reasons as
 * bootstrap_runtimeAdminMount_spec.js.
 */
function runInChildProcess(script) {
    return execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 20000 });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - runtime-events route (issue #59, spawned child process)", function() {
    this.timeout(20000);

    it("streams a real debug comms event from B's Admin API to a real receiver on A's events singleton", function() {
        var script = `
            const { bootstrap } = require(${JSON.stringify(BOOTSTRAP_MODULE)});
            const { createRuntimeEventsReceiver, DEFAULT_PATH } = require(${JSON.stringify(RUNTIME_EVENTS_MODULE)});
            const redUtil = require(${JSON.stringify(REDUTIL_MODULE)});
            (async () => {
                const handle = await bootstrap(${JSON.stringify(FLOW)}, {
                    adminApi: { port: 0, host: "127.0.0.1" }
                });
                const port = handle.adminApiAddress.port;
                const base = \`http://127.0.0.1:\${port}\`;

                const routeRes = await fetch(\`\${base}\${DEFAULT_PATH}\`);
                if (routeRes.status !== 200) {
                    throw new Error("expected the runtime-events route to be reachable with 200, got " + routeRes.status);
                }
                await routeRes.body.cancel();

                const editorEvents = new (require("events").EventEmitter)();
                const receiver = createRuntimeEventsReceiver({
                    events: editorEvents,
                    target: base,
                    log: { warn() {} }
                });

                const received = new Promise((resolve) => {
                    editorEvents.once("comms", resolve);
                });

                receiver.start();
                // give the receiver a moment to connect before the debug event fires
                await new Promise((resolve) => setTimeout(resolve, 200));

                redUtil.events.emit("comms", { topic: "debug", data: { id: "n1", msg: "hello from B" } });

                const event = await received;
                if (event.topic !== "debug" || event.data.msg !== "hello from B") {
                    throw new Error("expected the exact debug comms event to be re-emitted, got " + JSON.stringify(event));
                }

                receiver.stop();
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
});
