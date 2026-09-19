/**
 * issue #15: Node-RED storage/deployment-lifecycle coverage.
 *
 * Proves:
 *  - initial load's active flow/flowVersion come from Node-RED's OWN
 *    `runtime.flows.getFlows()` (bootstrap.js's `getCurrentFlow()`), not a
 *    second independent file read;
 *  - `handle.deploy()` drives a REAL `runtime.flows.setFlows()` full
 *    redeploy - Node-RED performs its normal node close/recreate lifecycle
 *    (a changed node id gets a brand-new live instance) - without
 *    restarting the process;
 *  - `flowVersion` changes after `deploy()` and is derived from the newly
 *    active flow configuration;
 *  - new ingress (via worker.js) picks up the new flowVersion after a
 *    deploy, with no worker/Activity recreation;
 *  - an in-flight Workflow's stale `flowVersion` is rejected with
 *    `FLOW_VERSION_MISMATCH` against the post-deploy Activity, never
 *    silently executed against the new node definitions.
 */
var should = require("should");
var path = require("path");
var fs = require("fs");
var sinon = require("sinon");
var { Worker, NativeConnection } = require("@temporalio/worker");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");
var { createWorker } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/worker.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");
var FLOW_CHANGED = path.join(FIXTURES, "four-node-flow.changed.json");
var SCHEDULED_FLOW = path.join(FIXTURES, "scheduled-flow.json");

var CLIENT_PATH = require.resolve("@temporalio/client");

function stubClientModule(startStub) {
    var original = require.cache[CLIENT_PATH];
    require.cache[CLIENT_PATH] = {
        id: CLIENT_PATH,
        filename: CLIENT_PATH,
        loaded: true,
        exports: {
            Connection: { connect: sinon.stub().resolves({}) },
            Client: function() { return { workflow: { start: startStub } }; }
        }
    };
    return function restore() {
        if (original) {
            require.cache[CLIENT_PATH] = original;
        } else {
            delete require.cache[CLIENT_PATH];
        }
    };
}

function waitUntil(predicate, timeoutMs) {
    var deadline = Date.now() + (timeoutMs || 5000);
    return new Promise(function(resolve, reject) {
        (function poll() {
            if (predicate()) {
                resolve();
                return;
            }
            if (Date.now() > deadline) {
                reject(new Error("waitUntil: timed out waiting for condition"));
                return;
            }
            setTimeout(poll, 20);
        }());
    });
}

describe("@tbrandenburg/node-red-temporal-runtime - issue #15 Node-RED storage/deploy lifecycle", function() {
    this.timeout(20000);

    var handle;

    afterEach(function() {
        if (handle) {
            var h = handle;
            handle = null;
            return h.stop();
        }
    });

    it("initial active flow/flowVersion come from runtime.flows.getFlows(), matching the caller's flow content", function() {
        return bootstrap(FLOW).then(function(h) {
            handle = h;
            return handle.getCurrentFlow();
        }).then(function(flows) {
            var n2 = flows.find(function(n) { return n.id === "n2"; });
            should.exist(n2);
            n2.name.should.equal("double");
            handle.flowVersion.should.be.a.String();
            handle.getFlowVersion().should.equal(handle.flowVersion);
        });
    });

    it("deploy() performs a real runtime.flows.setFlows() redeploy: new flowVersion, node instance recreated, new behavior live", function() {
        var newFlow = JSON.parse(fs.readFileSync(FLOW_CHANGED, "utf8"));
        return bootstrap(FLOW).then(function(h) {
            handle = h;
            var originalVersion = handle.flowVersion;
            var n2Before = handle.getNode("n2");
            should.exist(n2Before);

            return handle.deploy(newFlow).then(function(newVersion) {
                newVersion.should.not.equal(originalVersion);
                handle.flowVersion.should.equal(newVersion);

                // Node-RED's own normal lifecycle: a changed node gets
                // stopped and a brand-new instance created, not mutated
                // in-place.
                var n2After = handle.getNode("n2");
                should.exist(n2After);
                n2After.should.not.equal(n2Before);
                n2After.name.should.equal("triple");

                return handle.getCurrentFlow();
            }).then(function(flows) {
                var n2 = flows.find(function(n) { return n.id === "n2"; });
                n2.name.should.equal("triple");
            });
        });
    });

    it("deploy() actually changes runtime node behavior end-to-end (double -> triple) when driven through the live node", function() {
        var newFlow = JSON.parse(fs.readFileSync(FLOW_CHANGED, "utf8"));
        return bootstrap(FLOW).then(function(h) {
            handle = h;
            return handle.deploy(newFlow);
        }).then(function() {
            return new Promise(function(resolve, reject) {
                var n2 = handle.getNode("n2");
                var n3 = handle.getNode("n3");
                var originalReceive = n3.receive;
                n3.receive = function(msg) {
                    n3.receive = originalReceive;
                    resolve(msg);
                };
                try {
                    n2.receive({ payload: 10, _msgid: "deploy-e2e" });
                } catch (err) {
                    n3.receive = originalReceive;
                    reject(err);
                }
            });
        }).then(function(msg) {
            // FLOW_CHANGED's n2 multiplies by 3 (was by 2 pre-deploy).
            msg.payload.should.equal(30);
        });
    });

    it("createExecuteNode rejects with FLOW_VERSION_MISMATCH against the CURRENT deployment, not a scalar captured at construction time", function() {
        var newFlow = JSON.parse(fs.readFileSync(FLOW_CHANGED, "utf8"));
        var capture;
        return bootstrap(FLOW).then(function(h) {
            handle = h;
            capture = new Capture();
            var RED = require("nr-test-utils").require("node-red/lib/red");
            capture.install(RED);
            var staleVersion = handle.flowVersion;
            var executeNode = createExecuteNode({
                getNode: handle.getNode,
                flowVersion: function() { return handle.flowVersion; },
                capture: capture
            });

            // Pre-deploy: the Workflow's own version still matches - runs fine.
            return executeNode({ flowVersion: staleVersion, nodeId: "n2", msg: { payload: 1, _msgid: "pre-deploy" } }).then(function(result) {
                should.not.exist(result.error);

                return handle.deploy(newFlow);
            }).then(function() {
                // Post-deploy: the SAME stale Workflow flowVersion must now
                // be rejected explicitly - never silently executed against
                // the new node definitions.
                return executeNode({ flowVersion: staleVersion, nodeId: "n2", msg: { payload: 1, _msgid: "post-deploy-stale" } });
            }).then(function(result) {
                result.sends.should.eql([]);
                result.error.code.should.equal("FLOW_VERSION_MISMATCH");
                result.error.nodeId.should.equal("n2");

                // A fresh Workflow using the NEW current flowVersion runs fine.
                return executeNode({ flowVersion: handle.flowVersion, nodeId: "n2", msg: { payload: 1, _msgid: "post-deploy-current" } });
            }).then(function(result) {
                should.not.exist(result.error);
            });
        }).finally(function() {
            if (capture) {
                capture.uninstall();
            }
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/worker - issue #15 redeploy without process restart", function() {
    this.timeout(20000);

    var createStub;
    var nativeConnectionStub;
    var wired;
    var result;
    var restoreClient;

    beforeEach(function() {
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
        // issue #16: createActivityWorker/createWorkflowWorker now open a
        // REAL NativeConnection.connect() before Worker.create() (fixes
        // issue #21 - the Worker's own poll connection now honors the
        // configured address). Stub it here too, alongside Worker.create,
        // so this suite never attempts a real network connection - without
        // this, these tests would hang/fail wherever no real Temporal dev
        // server is reachable on the default address (e.g. CI).
        nativeConnectionStub = sinon.stub(NativeConnection, "connect").resolves({ close: sinon.stub().resolves() });
        result = null;
        restoreClient = null;
    });

    afterEach(function() {
        nativeConnectionStub.restore();
        var stopPromise = result ? result.stop() : Promise.resolve();
        return stopPromise.finally(function() {
            createStub.restore();
            if (restoreClient) {
                restoreClient();
            }
        });
    });

    it("new ingress after handle.deploy() carries the NEW flowVersion, without recreating the Worker/Activity", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-deploy" });
        restoreClient = stubClientModule(startStub);
        var newFlow = JSON.parse(fs.readFileSync(SCHEDULED_FLOW, "utf8"));
        var preDeployVersion;
        // Slow the repeat rate slightly on redeploy content is irrelevant -
        // reuse the exact same scheduled flow shape (still fires) so we
        // only need to observe flowVersion, not content, changing.
        newFlow.find(function(n) { return n.id === "n2"; }).name = "tag-v2";

        return createWorker(SCHEDULED_FLOW).then(function(created) {
            result = created;
            return waitUntil(function() { return startStub.callCount >= 1; }, 10000);
        }).then(function() {
            preDeployVersion = startStub.firstCall.args[1].args[0].flowVersion;
            preDeployVersion.should.equal(result.handle.flowVersion);
            startStub.resetHistory();

            return result.handle.deploy(newFlow);
        }).then(function(newVersion) {
            newVersion.should.not.equal(preDeployVersion);
            return waitUntil(function() { return startStub.callCount >= 1; }, 10000);
        }).then(function() {
            var postDeployVersion = startStub.firstCall.args[1].args[0].flowVersion;
            postDeployVersion.should.equal(result.handle.flowVersion);
            postDeployVersion.should.not.equal(preDeployVersion);
            createStub.calledOnce.should.equal(true); // no new Worker created
        });
    });
});
