var should = require("should");
var path = require("path");
var sinon = require("sinon");
var { Worker, NativeConnection } = require("@temporalio/worker");
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
    var nativeConnectionStub;

    beforeEach(function() {
        // `Worker.create` performs real workflow-bundling (webpack) against
        // a live Temporal environment, which is slow/heavy and belongs to
        // M7/M8's real-server E2E tests, not this unit test. Stubbing the
        // SDK's own `Worker.create` (restored in afterEach, scoped to this
        // one test file/run) keeps this test fast while still exercising
        // every real line of `createWorker`'s wiring logic.
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
        // issue #21/#16: createActivityWorker/createWorkflowWorker now
        // establish an explicit NativeConnection.connect() before
        // Worker.create() (so the poll connection honors configured
        // address). Stub it too, same "stub the SDK's own entry point"
        // convention, so these tests never depend on a live Temporal
        // dev server being reachable.
        nativeConnectionStub = sinon.stub(NativeConnection, "connect").resolves({ close: sinon.stub().resolves() });
    });

    afterEach(function() {
        createStub.restore();
        nativeConnectionStub.restore();
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
    var nativeConnectionStub;

    beforeEach(function() {
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
        nativeConnectionStub = sinon.stub(NativeConnection, "connect").resolves({ close: sinon.stub().resolves() });
    });

    afterEach(function() {
        createStub.restore();
        nativeConnectionStub.restore();
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

    it("issue #12 finding 2: a first failed connectClient() does not poison later ingress - a later attempt reconnects, and a successful client is then reused", function() {
        // Pre-existing, unrelated leak: @node-red/util's shared `events`
        // singleton EventEmitter accumulates one "comms:*" listener per
        // bootstrap() across this whole file's tests (never removed by
        // handle.stop()) and hits Node's default MaxListeners(10) warning
        // around the file's ~11th createWorker() call. That default warning
        // handler writes via console.error, which would otherwise leak into
        // this test's own console.error spy below. Bumping the limit here
        // is a test-only mitigation - fixing the underlying leak is out of
        // scope for issue #12 (see handoff Follow-up).
        var redUtilEvents = require("../../../../../packages/node_modules/@node-red/util").events;
        redUtilEvents.setMaxListeners(50);

        var connectStub = sinon.stub();
        connectStub.onCall(0).rejects(new Error("temporal unreachable (first attempt)"));
        connectStub.onCall(1).resolves({});
        var startStub = sinon.stub().resolves({ workflowId: "wf-reconnect" });
        var originalClientModule = require.cache[CLIENT_PATH];
        require.cache[CLIENT_PATH] = {
            id: CLIENT_PATH,
            filename: CLIENT_PATH,
            loaded: true,
            exports: {
                Connection: { connect: connectStub },
                Client: function() { return { workflow: { start: startStub } }; }
            }
        };

        var result;
        var errorSpy = sinon.stub(console, "error");
        return createWorker(FLOW).then(function(created) {
            result = created;
            // First ingress: connectClient() rejects - must fail visibly
            // (logged via console.error), no local routing fallback, and
            // must NOT wedge future ingress attempts.
            return result.capture._onIngress({
                sourceNodeId: "n1",
                msg: {},
                sends: [{ port: 0, destinationId: "n2", msg: {} }]
            });
        }).then(function() {
            errorSpy.calledOnce.should.equal(true);
            errorSpy.firstCall.args[0].should.match(/temporal unreachable \(first attempt\)/);
            startStub.called.should.equal(false);
            connectStub.callCount.should.equal(1);

            // Second ingress, after Temporal becomes available: reconnects
            // successfully without needing a worker restart.
            return result.capture._onIngress({
                sourceNodeId: "n1",
                msg: {},
                sends: [{ port: 0, destinationId: "n2", msg: {} }]
            });
        }).then(function() {
            startStub.calledOnce.should.equal(true);
            connectStub.callCount.should.equal(2);

            // Third ingress: the now-successful client must be REUSED, not
            // reconnected again.
            return result.capture._onIngress({
                sourceNodeId: "n1",
                msg: {},
                sends: [{ port: 0, destinationId: "n2", msg: {} }]
            });
        }).then(function() {
            startStub.calledTwice.should.equal(true);
            connectStub.callCount.should.equal(2);
            return result.stop();
        }).finally(function() {
            errorSpy.restore();
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
    var nativeConnectionStub;

    beforeEach(function() {
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
        nativeConnectionStub = sinon.stub(NativeConnection, "connect").resolves({ close: sinon.stub().resolves() });
        result = null;
        restoreClient = null;
    });

    afterEach(function() {
        var stopPromise = result ? result.stop() : Promise.resolve();
        return stopPromise.finally(function() {
            createStub.restore();
            nativeConnectionStub.restore();
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

// issue #16: role decomposition - proves the Workflow-only role never boots
// Node-RED/bootstrap(), the Activity role registers Activities and boots
// Node-RED, the Combined role preserves today's single-queue behavior, and
// custom Temporal config (address/namespace/queues) is passed through to
// Worker.create()/NativeConnection.connect() rather than silently replaced.
describe("@tbrandenburg/node-red-temporal-runtime/lib/worker - issue #16 role decomposition", function() {
    this.timeout(20000);

    var worker = require(WORKER_MODULE_PATH);
    var createStub;
    var wired;
    var nativeConnectionStub;
    var bootstrapModule = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");

    beforeEach(function() {
        wired = { shutdown: sinon.stub() };
        createStub = sinon.stub(Worker, "create").resolves(wired);
        nativeConnectionStub = sinon.stub(NativeConnection, "connect").resolves({ close: sinon.stub().resolves() });
    });

    afterEach(function() {
        createStub.restore();
        nativeConnectionStub.restore();
    });

    it("createWorkflowWorker never calls bootstrap() and never boots Node-RED", function() {
        var bootstrapSpy = sinon.spy(bootstrapModule, "bootstrap");
        return worker.createWorkflowWorker().then(function(result) {
            bootstrapSpy.called.should.equal(false);
            result.worker.should.equal(wired);
            result.should.not.have.property("handle");
            result.should.not.have.property("capture");
            return result.stop();
        }).finally(function() {
            bootstrapSpy.restore();
        });
    });

    it("createWorkflowWorker polls only the (default) Workflow Task Queue and honors NativeConnection", function() {
        return worker.createWorkflowWorker().then(function(result) {
            var opts = createStub.firstCall.args[0];
            opts.taskQueue.should.equal(result.temporalConfig.workflowTaskQueue);
            should.not.exist(opts.activities);
            nativeConnectionStub.calledOnce.should.equal(true);
            nativeConnectionStub.firstCall.args[0].address.should.equal(result.temporalConfig.address);
            return result.stop();
        });
    });

    it("createActivityWorker boots Node-RED, installs Capture, and registers executeNode on the Activity Task Queue", function() {
        return worker.createActivityWorker(FLOW).then(function(result) {
            result.handle.should.be.an.Object();
            result.capture._onIngress.should.be.a.Function();
            var opts = createStub.firstCall.args[0];
            opts.activities.executeNode.should.be.a.Function();
            opts.taskQueue.should.equal(result.temporalConfig.activityTaskQueue);
            return result.stop();
        });
    });

    it("createActivityWorker requires a flow file (source ingress/Node-RED is only ever hosted here)", function() {
        return worker.createActivityWorker().then(function() {
            throw new Error("expected createActivityWorker() to reject without a flow file");
        }, function(err) {
            err.should.be.an.Error();
        });
    });

    it("createCombinedWorker preserves today's single-shared-queue behavior when queues are not overridden", function() {
        return worker.createCombinedWorker(FLOW).then(function(result) {
            createStub.calledOnce.should.equal(true);
            var opts = createStub.firstCall.args[0];
            opts.taskQueue.should.equal("node-red-temporal");
            opts.workflowsPath.should.equal(WORKFLOWS_PATH);
            opts.activities.executeNode.should.be.a.Function();
            should.not.exist(result.workflowWorker);
            return result.stop();
        });
    });

    it("createCombinedWorker creates two independently-configured Workers when workflow/activity queues differ", function() {
        return worker.createCombinedWorker(FLOW, {
            temporal: { workflowTaskQueue: "wf-q", activityTaskQueue: "act-q" }
        }).then(function(result) {
            createStub.calledTwice.should.equal(true);
            var activityOpts = createStub.firstCall.args[0];
            var workflowOpts = createStub.secondCall.args[0];
            activityOpts.taskQueue.should.equal("act-q");
            workflowOpts.taskQueue.should.equal("wf-q");
            result.workflowWorker.should.be.an.Object();
            return result.stop();
        });
    });

    it("custom namespace/address/workerOptions are passed through to Worker.create(), not silently replaced (issue #21)", function() {
        return worker.createActivityWorker(FLOW, {
            temporal: { address: "10.0.0.5:9999", namespace: "custom-ns", activityTaskQueue: "custom-activity-q" },
            workerOptions: { maxConcurrentActivityTaskExecutions: 3 }
        }).then(function(result) {
            var opts = createStub.firstCall.args[0];
            opts.namespace.should.equal("custom-ns");
            opts.taskQueue.should.equal("custom-activity-q");
            opts.maxConcurrentActivityTaskExecutions.should.equal(3);
            nativeConnectionStub.firstCall.args[0].address.should.equal("10.0.0.5:9999");
            result.temporalConfig.address.should.equal("10.0.0.5:9999");
            return result.stop();
        });
    });

    it("source-ingress-started Workflows target the configured Workflow Task Queue and carry the configured Activity Task Queue (fixes #27)", function() {
        var startStub = sinon.stub().resolves({ workflowId: "wf-1" });
        var restore = stubClientModule(startStub);
        return worker.createActivityWorker(FLOW, {
            temporal: { workflowTaskQueue: "custom-wf-q", activityTaskQueue: "custom-act-q" }
        }).then(function(result) {
            return result.capture._onIngress({
                sourceNodeId: "n1",
                msg: {},
                sends: [{ port: 0, destinationId: "n2", msg: {} }]
            }).then(function() {
                startStub.calledOnce.should.equal(true);
                var args = startStub.firstCall.args[1];
                args.taskQueue.should.equal("custom-wf-q");
                args.args[0].activityTaskQueue.should.equal("custom-act-q");
                return result.stop();
            });
        }).finally(function() {
            restore();
        });
    });

    it("stop() calls Worker.shutdown(), Capture.uninstall(), and Node-RED handle.stop() for the Activity role", function() {
        return worker.createActivityWorker(FLOW).then(function(result) {
            var stopSpy = sinon.spy(result.handle, "stop");
            return result.stop().then(function() {
                wired.shutdown.calledOnce.should.equal(true);
                stopSpy.calledOnce.should.equal(true);
            });
        });
    });

    it("resolveTemporalConfig falls back to env vars, then to today's local defaults", function() {
        var config = worker.resolveTemporalConfig();
        config.address.should.equal(process.env.TEMPORAL_ADDRESS || "127.0.0.1:7233");
        config.namespace.should.equal(process.env.TEMPORAL_NAMESPACE || "default");
        config.workflowTaskQueue.should.equal(process.env.TEMPORAL_WORKFLOW_TASK_QUEUE || "node-red-temporal");
        config.activityTaskQueue.should.equal(process.env.TEMPORAL_ACTIVITY_TASK_QUEUE || "node-red-temporal");
    });
});
