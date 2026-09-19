var should = require("should");
var path = require("path");
var sinon = require("sinon");
var { Worker } = require("@temporalio/worker");
var { createWorker, WORKFLOWS_PATH } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/worker.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");

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
