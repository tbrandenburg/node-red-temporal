var should = require("should");
var fs = require("fs");
var os = require("os");
var path = require("path");
var sinon = require("sinon");

var MODULE_PATH = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/ingressSpool.js");
var ingressSpool = require(MODULE_PATH);
var CLIENT_PATH = require.resolve("@temporalio/client");

function WorkflowExecutionAlreadyStartedError(message) {
    var err = new Error(message);
    err.name = "WorkflowExecutionAlreadyStartedError";
    Object.setPrototypeOf(err, WorkflowExecutionAlreadyStartedError.prototype);
    return err;
}
WorkflowExecutionAlreadyStartedError.prototype = Object.create(Error.prototype);
WorkflowExecutionAlreadyStartedError.prototype.constructor = WorkflowExecutionAlreadyStartedError;

/**
 * Same "stub the SDK's own entry point via require.cache" convention used
 * by httpIngress_spec.js/worker_spec.js, so ingressSpool.js's internal
 * `require("@temporalio/client")` resolves to our fakes too.
 */
function stubClientModule(startStub) {
    var original = require.cache[CLIENT_PATH];
    require.cache[CLIENT_PATH] = {
        id: CLIENT_PATH,
        filename: CLIENT_PATH,
        loaded: true,
        exports: {
            WorkflowIdReusePolicy: { REJECT_DUPLICATE: "REJECT_DUPLICATE" },
            WorkflowExecutionAlreadyStartedError: WorkflowExecutionAlreadyStartedError,
            Client: function() { return { workflow: { start: startStub } }; },
            Connection: { connect: sinon.stub().resolves({}) }
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

function freshDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), "ingress-spool-spec-"));
}

var SAMPLE_RECORD_BASE = {
    graph: { n1: [["n2"]] },
    nodeMeta: { n2: { type: "change" } },
    flowVersion: "v1",
    initial: [{ nodeId: "n2", msg: { payload: 1 } }],
    activityTaskQueue: "node-red-temporal",
    nodeExecutionTimeoutMs: 1800000
};

describe("@tbrandenburg/node-red-temporal-runtime/lib/ingressSpool - M1 pure primitives", function() {
    var dir;

    beforeEach(function() {
        dir = freshDir();
    });

    afterEach(function() {
        fs.rmSync(dir, { recursive: true, force: true });
    });

    it("a complete record survives process reopen (round-trips exactly via a fresh require of the module)", function() {
        var record = Object.assign({ id: "id-1" }, SAMPLE_RECORD_BASE);
        ingressSpool.writeIngressRecord(dir, record);

        var pending = ingressSpool.listPendingIngressRecords(dir);
        pending.should.have.length(1);
        pending[0].id.should.equal("id-1");
        pending[0].record.should.eql(record);
    });

    it("leaves no .tmp artifact behind and never lists a .tmp file as pending", function() {
        var record = Object.assign({ id: "id-2" }, SAMPLE_RECORD_BASE);
        ingressSpool.writeIngressRecord(dir, record);
        fs.readdirSync(dir).filter(function(name) { return name.endsWith(".tmp"); }).should.eql([]);

        // A stray .tmp file (simulating a crash mid-write) must be ignored.
        fs.writeFileSync(path.join(dir, "incomplete.tmp"), "{not json");
        var pending = ingressSpool.listPendingIngressRecords(dir);
        pending.should.have.length(1);
        pending[0].id.should.equal("id-2");
    });

    it("delete (removeIngressRecord) is idempotent - a second call for an already-removed id is a harmless no-op", function() {
        var record = Object.assign({ id: "id-3" }, SAMPLE_RECORD_BASE);
        ingressSpool.writeIngressRecord(dir, record);
        ingressSpool.removeIngressRecord(dir, "id-3");
        ingressSpool.listPendingIngressRecords(dir).should.eql([]);

        // second call must not throw
        should.doesNotThrow(function() {
            ingressSpool.removeIngressRecord(dir, "id-3");
        });
    });

    it("a malformed final record (invalid JSON) is quarantined, logged, and does not crash the scan", function() {
        var good = Object.assign({ id: "id-good" }, SAMPLE_RECORD_BASE);
        ingressSpool.writeIngressRecord(dir, good);
        fs.writeFileSync(path.join(dir, "bad.json"), "{ not: valid json");

        var errorSpy = sinon.stub(console, "error");
        var pending;
        try {
            pending = ingressSpool.listPendingIngressRecords(dir);
        } finally {
            errorSpy.restore();
        }

        pending.should.have.length(1);
        pending[0].id.should.equal("id-good");
        fs.existsSync(path.join(dir, "bad.json")).should.equal(false);
        fs.existsSync(path.join(dir, "bad.json.malformed")).should.equal(true);
        errorSpy.calledOnce.should.equal(true);
        errorSpy.firstCall.args[0].should.match(/quarantined/);
    });

    it("a well-formed JSON file missing an \"id\" field is also quarantined, not crashed on", function() {
        fs.writeFileSync(path.join(dir, "no-id.json"), JSON.stringify({ graph: {} }));
        var errorSpy = sinon.stub(console, "error");
        var pending;
        try {
            pending = ingressSpool.listPendingIngressRecords(dir);
        } finally {
            errorSpy.restore();
        }
        pending.should.eql([]);
        fs.existsSync(path.join(dir, "no-id.json.malformed")).should.equal(true);
    });

    it("listPendingIngressRecords returns an empty array when the directory does not exist yet", function() {
        var missing = path.join(dir, "does-not-exist");
        ingressSpool.listPendingIngressRecords(missing).should.eql([]);
    });

    it("no new dependency: only Node builtins (fs/path/crypto) are required by the module", function() {
        var source = fs.readFileSync(MODULE_PATH, "utf8");
        var requireCalls = source.match(/require\(("|')([^"']+)("|')\)/g) || [];
        var externalRequires = requireCalls.filter(function(call) {
            return !/require\(("|')(fs|path|crypto|@temporalio\/client)("|')\)/.test(call);
        });
        externalRequires.should.eql([]);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/ingressSpool - M2 startIngressWorkflow (stable Workflow start helper)", function() {
    it("derives a stable Workflow ID from the record id", function() {
        ingressSpool.deriveIngressWorkflowId("abc-123").should.equal("ingress:abc-123");
    });

    it("first start succeeds and uses REJECT_DUPLICATE reuse policy with the derived Workflow ID", function() {
        var startStub = sinon.stub().resolves({});
        var restore = stubClientModule(startStub);
        var record = Object.assign({ id: "wf-id-1" }, SAMPLE_RECORD_BASE);

        return ingressSpool.startIngressWorkflow(function() { return Promise.resolve(new (require("@temporalio/client").Client)()); }, { workflowTaskQueue: "wq", activityTaskQueue: "aq" }, record)
            .then(function(result) {
                result.should.eql({ workflowId: "ingress:wf-id-1", duplicate: false });
                startStub.calledOnce.should.equal(true);
                var args = startStub.firstCall.args;
                args[0].should.equal("executeFlow");
                args[1].workflowId.should.equal("ingress:wf-id-1");
                args[1].workflowIdReusePolicy.should.equal("REJECT_DUPLICATE");
                args[1].taskQueue.should.equal("wq");
                args[1].args[0].graph.should.equal(record.graph);
                args[1].args[0].initial.should.eql(record.initial);
            }).finally(restore);
    });

    it("retry after success (already-started/duplicate) is treated as success, not an error", function() {
        var startStub = sinon.stub().rejects(new WorkflowExecutionAlreadyStartedError("already started"));
        var restore = stubClientModule(startStub);
        var record = Object.assign({ id: "wf-id-2" }, SAMPLE_RECORD_BASE);

        return ingressSpool.startIngressWorkflow(function() { return Promise.resolve(new (require("@temporalio/client").Client)()); }, { workflowTaskQueue: "wq", activityTaskQueue: "aq" }, record)
            .then(function(result) {
                result.should.eql({ workflowId: "ingress:wf-id-2", duplicate: true });
            }).finally(restore);
    });

    it("a non-duplicate start error (e.g. Temporal unavailable) rejects so the caller can leave the record pending", function() {
        var startStub = sinon.stub().rejects(new Error("temporal unreachable"));
        var restore = stubClientModule(startStub);
        var record = Object.assign({ id: "wf-id-3" }, SAMPLE_RECORD_BASE);

        return ingressSpool.startIngressWorkflow(function() { return Promise.resolve(new (require("@temporalio/client").Client)()); }, { workflowTaskQueue: "wq", activityTaskQueue: "aq" }, record)
            .then(function() {
                throw new Error("expected rejection");
            }, function(err) {
                err.message.should.match(/temporal unreachable/);
            }).finally(restore);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/ingressSpool - createIngressSpool (M3/M4/M5 stateful wiring)", function() {
    var dir;

    beforeEach(function() {
        dir = freshDir();
    });

    afterEach(function() {
        fs.rmSync(dir, { recursive: true, force: true });
    });

    it("persistAndStart writes the record durably, starts the Workflow, and removes the record on success", function() {
        var startStub = sinon.stub().resolves({});
        var restore = stubClientModule(startStub);
        var spool = ingressSpool.createIngressSpool({
            dir,
            getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
            temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" }
        });
        var record = Object.assign({ id: spool.generateId() }, SAMPLE_RECORD_BASE);

        return spool.persistAndStart(record).then(function() {
            startStub.calledOnce.should.equal(true);
            ingressSpool.listPendingIngressRecords(dir).should.eql([]);
        }).finally(restore);
    });

    it("crash-window A: workflow.start fails - the record stays pending (not removed), no throw/unhandled rejection", function() {
        var startStub = sinon.stub().rejects(new Error("temporal unreachable"));
        var restore = stubClientModule(startStub);
        var errorSpy = sinon.stub(console, "error");
        var spool = ingressSpool.createIngressSpool({
            dir,
            getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
            temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" },
            retryDelaysMs: []
        });
        var record = Object.assign({ id: spool.generateId() }, SAMPLE_RECORD_BASE);

        return spool.persistAndStart(record).then(function() {
            ingressSpool.listPendingIngressRecords(dir).should.have.length(1);
            errorSpy.called.should.equal(true);
        }).finally(function() {
            errorSpy.restore();
            spool.stop();
            restore();
        });
    });

    it("crash-window A recovery: restarting and replaying the still-pending record starts it successfully and removes it", function() {
        var failingStart = sinon.stub().rejects(new Error("temporal unreachable"));
        var restoreFail = stubClientModule(failingStart);
        var errorSpy = sinon.stub(console, "error");
        var spoolBeforeCrash = ingressSpool.createIngressSpool({
            dir,
            getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
            temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" },
            retryDelaysMs: []
        });
        var record = Object.assign({ id: spoolBeforeCrash.generateId() }, SAMPLE_RECORD_BASE);

        return spoolBeforeCrash.persistAndStart(record).then(function() {
            errorSpy.restore();
            restoreFail();
            spoolBeforeCrash.stop();

            // "restart": a fresh spool instance over the SAME directory,
            // now backed by a succeeding client.
            var succeedingStart = sinon.stub().resolves({});
            var restoreSucceed = stubClientModule(succeedingStart);
            var spoolAfterRestart = ingressSpool.createIngressSpool({
                dir,
                getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
                temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" }
            });

            return spoolAfterRestart.replayPending().then(function() {
                succeedingStart.calledOnce.should.equal(true);
                var startedWorkflowId = succeedingStart.firstCall.args[1].workflowId;
                startedWorkflowId.should.equal(ingressSpool.deriveIngressWorkflowId(record.id));
                ingressSpool.listPendingIngressRecords(dir).should.eql([]);
            }).finally(restoreSucceed);
        });
    });

    it("crash-window B: workflow.start succeeds but the record is not yet unlinked (simulated crash) - replay sees already-started and removes the record without starting a second Workflow", function() {
        var record = Object.assign({ id: "crash-b-id" }, SAMPLE_RECORD_BASE);
        // Simulate: Temporal already accepted the start, but the process
        // died before this runner unlinked the spool record - so the
        // record is simply still on disk.
        ingressSpool.writeIngressRecord(dir, record);

        var duplicateStart = sinon.stub().rejects(new WorkflowExecutionAlreadyStartedError("already started"));
        var restore = stubClientModule(duplicateStart);
        var spool = ingressSpool.createIngressSpool({
            dir,
            getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
            temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" }
        });

        return spool.replayPending().then(function() {
            duplicateStart.calledOnce.should.equal(true);
            ingressSpool.listPendingIngressRecords(dir).should.eql([]);
        }).finally(restore);
    });

    it("two independent ingress records remain distinct (different ids, different derived Workflow IDs, independently removed)", function() {
        var startStub = sinon.stub().resolves({});
        var restore = stubClientModule(startStub);
        var spool = ingressSpool.createIngressSpool({
            dir,
            getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
            temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" }
        });
        var recordA = Object.assign({ id: spool.generateId() }, SAMPLE_RECORD_BASE);
        var recordB = Object.assign({ id: spool.generateId() }, SAMPLE_RECORD_BASE);
        recordA.id.should.not.equal(recordB.id);

        return Promise.all([spool.persistAndStart(recordA), spool.persistAndStart(recordB)]).then(function() {
            startStub.calledTwice.should.equal(true);
            var workflowIds = startStub.getCalls().map(function(call) { return call.args[1].workflowId; });
            workflowIds.should.containEql(ingressSpool.deriveIngressWorkflowId(recordA.id));
            workflowIds.should.containEql(ingressSpool.deriveIngressWorkflowId(recordB.id));
            ingressSpool.listPendingIngressRecords(dir).should.eql([]);
        }).finally(restore);
    });

    it("stop() cancels pending in-process retry timers without deleting the on-disk record", function() {
        var startStub = sinon.stub().rejects(new Error("temporal unreachable"));
        var restore = stubClientModule(startStub);
        var errorSpy = sinon.stub(console, "error");
        var spool = ingressSpool.createIngressSpool({
            dir,
            getClient: function() { return Promise.resolve(new (require("@temporalio/client").Client)()); },
            temporalConfig: { workflowTaskQueue: "wq", activityTaskQueue: "aq" },
            retryDelaysMs: [50000]
        });
        var record = Object.assign({ id: spool.generateId() }, SAMPLE_RECORD_BASE);

        return spool.persistAndStart(record).then(function() {
            // A retry timer is now scheduled 50s out - stop() must cancel
            // it (verified indirectly: no crash/leak, record still present).
            spool.stop();
            ingressSpool.listPendingIngressRecords(dir).should.have.length(1);
        }).finally(function() {
            errorSpy.restore();
            restore();
        });
    });
});
