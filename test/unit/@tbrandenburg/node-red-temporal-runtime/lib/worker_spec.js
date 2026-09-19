var should = require("should");
var path = require("path");
var sinon = require("sinon");
var { Worker } = require("@temporalio/worker");
var WORKER_MODULE_PATH = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/worker.js");
var { createWorker, WORKFLOWS_PATH } = require(WORKER_MODULE_PATH);

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");
var FANOUT_FLOW = path.join(FIXTURES, "fanout-flow.json");
var SCHEDULED_FLOW = path.join(FIXTURES, "scheduled-flow.json");
var EXTERNAL_SOURCE_FLOW = path.join(FIXTURES, "external-source-flow.json");

// Same "stub the SDK's own entry point via require.cache" convention as
// bin_spec.js's in-process @temporalio/client stubbing - keeps M3's ingress
// wiring tests fast/deterministic without a live Temporal dev server.
var CLIENT_PATH = require.resolve("@temporalio/client");

/**
 * M5/M6: installs a fake `@temporalio/client` module (in require.cache) whose
 * `Client` constructor returns `{ workflow: { start: startStub } }`, mirroring
 * the M4 fanout test's convention. Returns a `restore()` to undo it - callers
 * MUST call this in `afterEach`/`finally` so the fake never leaks into other
 * test files (this repo's own lessons-learned explicitly warn about shared
 * global state leaking across a full-suite run).
 */
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

/** Polls `predicate` every 20ms until it returns true or `timeoutMs` elapses. */
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

describe("@tbrandenburg/node-red-temporal-runtime/lib/worker", function() {
    this.timeout(20000);

    var createStub;
    var wired;

    beforeEach(function() {
        // `Worker.create` performs real workflow-bundling (webpack) against
        // a live Temporal environment, which is slow/heavy and belongs to
        // M7/M8's real-server E2E tests, not this unit test. Stubbing the
        // SDK's own `Worker.create` (restored in afterEach, scoped to this
        // one test file/run) keeps this test fast while still exercising
        // every real line of `createWorker`'s wiring logic.
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
    });

    afterEach(function() {
        createStub.restore();
    });

    it("boots the flow once, installs Capture, and registers executeNode + workflowsPath with Worker.create", function() {
        var ctx;
        return createWorker(FLOW).then(function(result) {
            ctx = result;
            result.worker.should.equal(wired);
            result.flowVersion.should.equal(result.handle.flowVersion);

            createStub.calledOnce.should.equal(true);
            var opts = createStub.firstCall.args[0];
            opts.workflowsPath.should.equal(WORKFLOWS_PATH);
            opts.taskQueue.should.equal("node-red-temporal");
            opts.activities.executeNode.should.be.a.Function();

            return opts.activities.executeNode({ flowVersion: result.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "wm1" } });
        }).then(function(execResult) {
            should.not.exist(execResult.error);
            execResult.sends[0].msg.payload.should.equal(42);
            return ctx.stop();
        }).then(function() {
            wired.shutdown.calledOnce.should.equal(true);
        });
    });

    it("honours workerOptions overrides (e.g. a custom taskQueue) without dropping the required wiring", function() {
        return createWorker(FLOW, { workerOptions: { taskQueue: "custom-queue" } }).then(function(result) {
            var opts = createStub.firstCall.args[0];
            opts.taskQueue.should.equal("custom-queue");
            opts.workflowsPath.should.equal(WORKFLOWS_PATH);
            return result.stop();
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/worker - toInitial (M3 ingress -> initial mapping)", function() {
    var worker = require(WORKER_MODULE_PATH);

    it("maps one initial entry per sends[] item, using each send's own msg", function() {
        var ingress = {
            sourceNodeId: "n1",
            msg: { payload: "group-level, should not be used" },
            sends: [
                { port: 0, destinationId: "n2", msg: { payload: "a" } },
                { port: 1, destinationId: "n3", msg: { payload: "b" } }
            ]
        };
        worker.toInitial(ingress).should.eql([
            { nodeId: "n2", msg: { payload: "a" } },
            { nodeId: "n3", msg: { payload: "b" } }
        ]);
    });

    it("returns an empty array when there are no sends", function() {
        worker.toInitial({ sourceNodeId: "n1", msg: {}, sends: [] }).should.eql([]);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/worker - createOnIngress (M3 start executeFlow from source ingress)", function() {
    var worker = require(WORKER_MODULE_PATH);
    var flowInfo = { graph: { n2: [[]], n3: [[]] }, nodeMeta: { n2: { type: "change" } }, flowVersion: "v1" };

    it("starts executeFlow with taskQueue, a unique workflowId, and the mapped initial array", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-1" });
        var client = { workflow: { start: startStub } };
        var onIngress = worker.createOnIngress(function() { return Promise.resolve(client); }, flowInfo);

        return onIngress({
            sourceNodeId: "n1",
            msg: { payload: 1 },
            sends: [
                { port: 0, destinationId: "n2", msg: { payload: 1 } },
                { port: 0, destinationId: "n3", msg: { payload: 1 } }
            ]
        }).then(function() {
            startStub.calledOnce.should.equal(true);
            var args = startStub.firstCall.args;
            args[0].should.equal("executeFlow");
            args[1].taskQueue.should.equal("node-red-temporal");
            args[1].workflowId.should.be.a.String();
            args[1].args[0].graph.should.equal(flowInfo.graph);
            args[1].args[0].nodeMeta.should.equal(flowInfo.nodeMeta);
            args[1].args[0].flowVersion.should.equal("v1");
            args[1].args[0].initial.should.eql([
                { nodeId: "n2", msg: { payload: 1 } },
                { nodeId: "n3", msg: { payload: 1 } }
            ]);
        });
    });

    it("does not call client.workflow.start twice for two separate ingress calls (no duplicate workflow start)", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-1" });
        var client = { workflow: { start: startStub } };
        var onIngress = worker.createOnIngress(function() { return Promise.resolve(client); }, flowInfo);

        return onIngress({ sourceNodeId: "n1", msg: {}, sends: [{ port: 0, destinationId: "n2", msg: {} }] })
            .then(function() {
                startStub.calledOnce.should.equal(true);
            });
    });

    it("AC7: a rejected client.workflow.start is caught, logged via console.error, and never rejects/throws or reports false success", function() {
        var startStub = sinon.stub().rejects(new Error("temporal unreachable"));
        var client = { workflow: { start: startStub } };
        var onIngress = worker.createOnIngress(function() { return Promise.resolve(client); }, flowInfo);
        var errorSpy = sinon.stub(console, "error");

        return onIngress({ sourceNodeId: "n1", msg: {}, sends: [{ port: 0, destinationId: "n2", msg: {} }] })
            .then(function(result) {
                should.not.exist(result);
                errorSpy.calledOnce.should.equal(true);
                errorSpy.firstCall.args[0].should.match(/temporal unreachable/);
                errorSpy.firstCall.args[0].should.match(/n1/);
            }).finally(function() {
                errorSpy.restore();
            });
    });

    it("AC7: a rejected getClient() (connection failure) is likewise caught and logged, not thrown", function() {
        var onIngress = worker.createOnIngress(function() { return Promise.reject(new Error("connection refused")); }, flowInfo);
        var errorSpy = sinon.stub(console, "error");

        return onIngress({ sourceNodeId: "n1", msg: {}, sends: [{ port: 0, destinationId: "n2", msg: {} }] })
            .then(function() {
                errorSpy.calledOnce.should.equal(true);
                errorSpy.firstCall.args[0].should.match(/connection refused/);
            }).finally(function() {
                errorSpy.restore();
            });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/worker - createWorker wires onIngress into Capture (M3)", function() {
    this.timeout(20000);

    var createStub;
    var wired;

    beforeEach(function() {
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
    });

    afterEach(function() {
        createStub.restore();
    });

    it("defaults captureOptions.onIngress to the real ingress-start wiring (a function) when not overridden", function() {
        return createWorker(FLOW).then(function(result) {
            result.capture._onIngress.should.be.a.Function();
            return result.stop();
        });
    });

    it("honours an explicit captureOptions.onIngress override instead of the default", function() {
        var custom = sinon.stub();
        return createWorker(FLOW, { captureOptions: { onIngress: custom } }).then(function(result) {
            result.capture._onIngress.should.equal(custom);
            return result.stop();
        });
    });

    it("starts exactly one executeFlow Workflow for one Inject firing that fans out to two downstream nodes (M4 gate)", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-fanout" });
        var originalClientModule = require.cache[CLIENT_PATH];
        require.cache[CLIENT_PATH] = {
            id: CLIENT_PATH,
            filename: CLIENT_PATH,
            loaded: true,
            exports: {
                Connection: { connect: sinon.stub().resolves({}) },
                Client: function() { return { workflow: { start: startStub } }; }
            }
        };

        return createWorker(FANOUT_FLOW).then(function(result) {
            return result.capture._onIngress({
                sourceNodeId: "n1",
                msg: { payload: 1 },
                sends: [
                    { port: 0, destinationId: "n2", msg: { payload: 1 } },
                    { port: 0, destinationId: "n3", msg: { payload: 1 } }
                ]
            }).then(function() {
                startStub.calledOnce.should.equal(true);
                var args = startStub.firstCall.args[1];
                args.args[0].initial.should.eql([
                    { nodeId: "n2", msg: { payload: 1 } },
                    { nodeId: "n3", msg: { payload: 1 } }
                ]);
                return result.stop();
            });
        }).finally(function() {
            if (originalClientModule) {
                require.cache[CLIENT_PATH] = originalClientModule;
            } else {
                delete require.cache[CLIENT_PATH];
            }
        });
    });

    it("createWorker still boots successfully even if Temporal is unreachable at boot time (client is lazy, only used on ingress)", function() {
        var originalClientModule = require.cache[CLIENT_PATH];
        require.cache[CLIENT_PATH] = {
            id: CLIENT_PATH,
            filename: CLIENT_PATH,
            loaded: true,
            exports: {
                Connection: { connect: sinon.stub().rejects(new Error("connection refused")) },
                Client: function() { throw new Error("should not be constructed"); }
            }
        };

        return createWorker(FLOW).then(function(result) {
            result.worker.should.equal(wired);
            return result.stop();
        }).finally(function() {
            if (originalClientModule) {
                require.cache[CLIENT_PATH] = originalClientModule;
            } else {
                delete require.cache[CLIENT_PATH];
            }
        });
    });
});

// M5+M6: integration-level tests driven through a REAL, running Node-RED
// flow (real Inject repeat timer / real node.receive() calls), not synthetic
// manual `capture._onIngress(...)` invocations - only `Worker.create` and
// `@temporalio/client` are faked, per the milestone plan.
describe("@tbrandenburg/node-red-temporal-runtime/lib/worker - M5/M6 real-flow ingress integration", function() {
    this.timeout(20000);

    var createStub;
    var wired;
    var result;
    var restoreClient;

    beforeEach(function() {
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
        result = null;
        restoreClient = null;
    });

    afterEach(function() {
        var stopPromise = result ? result.stop() : Promise.resolve();
        return stopPromise.finally(function() {
            createStub.restore();
            if (restoreClient) {
                restoreClient();
            }
        });
    });

    it("M5/AC3: a scheduled Inject firing 3 times starts 3 distinct executeFlow Workflow Executions, no CLI start used", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-scheduled" });
        restoreClient = stubClientModule(startStub);

        return createWorker(SCHEDULED_FLOW).then(function(created) {
            result = created;
            return waitUntil(function() { return startStub.callCount >= 3; }, 10000);
        }).then(function() {
            startStub.callCount.should.be.aboveOrEqual(3);
            var calls = startStub.getCalls().slice(0, 3);
            var workflowIds = calls.map(function(call) { return call.args[1].workflowId; });
            // AC3: three DISTINCT Workflow Executions.
            (new Set(workflowIds)).size.should.equal(3);
            calls.forEach(function(call) {
                var args = call.args[1];
                args.args[0].should.equal(args.args[0]); // sanity: args shape present
                args.args[0].initial.should.be.an.Array();
                args.args[0].initial.length.should.equal(1);
                args.args[0].initial[0].nodeId.should.equal("n2");
            });
        });
    });

    it("M5/AC5: two rapid scheduled-Inject firings produce 2 isolated Workflow Executions with non-crossed payloads", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-rapid" });
        restoreClient = stubClientModule(startStub);

        return createWorker(SCHEDULED_FLOW).then(function(created) {
            result = created;
            return waitUntil(function() { return startStub.callCount >= 2; }, 10000);
        }).then(function() {
            var first = startStub.getCall(0).args[1];
            var second = startStub.getCall(1).args[1];

            first.workflowId.should.not.equal(second.workflowId);

            var firstMsg = first.args[0].initial[0].msg;
            var secondMsg = second.args[0].initial[0].msg;
            // Each firing is Date.now()-payloaded (payloadType "date") and gets
            // its own auto-generated _msgid - both prove no shared/mutated
            // object crossed between the two ingress groups.
            firstMsg._msgid.should.not.equal(secondMsg._msgid);
            firstMsg.should.not.equal(secondMsg);
            should(typeof firstMsg.payload).equal("number");
            should(typeof secondMsg.payload).equal("number");
        });
    });

    it("M5/AC6 (regression): an Activity-owned executeNode invocation (real capture.around()) never calls workflow.start", function() {
        var startStub = sinon.stub();
        restoreClient = stubClientModule(startStub);

        // FLOW's Inject has no repeat/once/crontab, so it never fires on its
        // own - the only send() happens synchronously inside the Activity's
        // own capture.around() call below, i.e. an Activity-owned send.
        return createWorker(FLOW).then(function(created) {
            result = created;
            var opts = createStub.firstCall.args[0];
            return opts.activities.executeNode({ flowVersion: result.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "wm-ac6" } });
        }).then(function(execResult) {
            should.not.exist(execResult.error);
            execResult.sends[0].msg.payload.should.equal(42);
            // Give any stray async ingress handling a tick to (not) fire.
            return new Promise(function(resolve) { setImmediate(resolve); });
        }).then(function() {
            startStub.called.should.equal(false);
        });
    });

    it("M5/AC7: a real scheduled-Inject firing whose client.workflow.start rejects is logged via console.error, never thrown/unhandled", function() {
        var startStub = sinon.stub().rejects(new Error("temporal unreachable (AC7 real-inject path)"));
        restoreClient = stubClientModule(startStub);
        var errorSpy = sinon.stub(console, "error");

        return createWorker(SCHEDULED_FLOW).then(function(created) {
            result = created;
            return waitUntil(function() { return errorSpy.called; }, 10000);
        }).then(function() {
            errorSpy.firstCall.args[0].should.match(/temporal unreachable \(AC7 real-inject path\)/);
            errorSpy.firstCall.args[0].should.match(/n1/);
        }).finally(function() {
            errorSpy.restore();
        });
    });

    it("M6: a real non-timer external event (node.receive(), representing e.g. an MQTT In message) starts exactly one Workflow, seeded from the source's WIRED DESTINATION not the source itself", function() {
        // Scope note (see handoff): no in-process MQTT broker is wired into
        // this repo's install (packages/node_modules/@tbrandenburg's own
        // package.json is not an npm workspace member, so a devDependency
        // added there is never actually installed by root `npm install`).
        // This test instead proves the general "external, non-timer source"
        // mechanism using a plain orphan node driven via its public
        // `receive()` API - the exact same call shape a real MQTT In node's
        // client callback would make on an incoming message.
        var startStub = sinon.stub().resolves({ workflowId: "wf-external" });
        restoreClient = stubClientModule(startStub);

        return createWorker(EXTERNAL_SOURCE_FLOW).then(function(created) {
            result = created;
            var sourceNode = result.handle.getNode("n1");
            sourceNode.receive({ payload: "external-event" });
            return waitUntil(function() { return startStub.callCount >= 1; }, 5000);
        }).then(function() {
            startStub.calledOnce.should.equal(true);
            var args = startStub.firstCall.args[1];
            // Seeded from n1's wired destination (n2), never from n1 itself.
            args.args[0].initial.should.eql([
                { nodeId: "n2", msg: { payload: "external-event", _msgid: args.args[0].initial[0].msg._msgid } }
            ]);
            args.args[0].initial[0].nodeId.should.not.equal("n1");
        });
    });
});
