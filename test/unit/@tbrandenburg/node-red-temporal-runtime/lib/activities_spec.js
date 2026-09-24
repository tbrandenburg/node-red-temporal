var should = require("should");
var path = require("path");
var RED = require("nr-test-utils").require("node-red/lib/red");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");
var ERROR_FLOW = path.join(FIXTURES, "error-flow.json");
var CATCH_FLOW = path.join(FIXTURES, "catch-flow.json");

describe("@tbrandenburg/node-red-temporal-runtime/lib/activities", function() {
    this.timeout(20000);

    var handle;
    var capture;

    afterEach(function() {
        if (capture) {
            capture.uninstall();
            capture = null;
        }
        if (handle) {
            var h = handle;
            handle = null;
            return h.stop();
        }
    });

    function bootWith(flowFile) {
        return bootstrap(flowFile).then(function(h) {
            handle = h;
            capture = new Capture();
            capture.install(RED);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return { handle: h, executeNode: executeNode };
        });
    }

    it("delivers a message to a real live node and returns its actual sends as {port, destinationId, msg}", function() {
        return bootWith(FLOW).then(function(ctx) {
            return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "m1" } });
        }).then(function(result) {
            should.not.exist(result.error);
            result.sends.length.should.equal(1);
            result.sends[0].port.should.equal(0);
            result.sends[0].msg.payload.should.equal(42);
            // issue #13: destinationId is Node-RED's own resolved routing
            // decision (from Capture's preRoute hook), preserved through the
            // Activity boundary instead of being dropped.
            result.sends[0].destinationId.should.equal("n3");
        });
    });

    it("rejects with a structured error when flowVersion does not match, without executing the node", function() {
        return bootWith(FLOW).then(function(ctx) {
            return ctx.executeNode({ flowVersion: "stale-version", nodeId: "n2", msg: { payload: 1, _msgid: "m2" } });
        }).then(function(result) {
            result.sends.should.eql([]);
            result.error.code.should.equal("FLOW_VERSION_MISMATCH");
            result.error.nodeId.should.equal("n2");
        });
    });

    it("returns a structured error, not a crash, for an unknown nodeId", function() {
        return bootWith(FLOW).then(function(ctx) {
            return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "does-not-exist", msg: { payload: 1, _msgid: "m3" } });
        }).then(function(result) {
            result.sends.should.eql([]);
            result.error.code.should.equal("NODE_NOT_FOUND");
            result.error.nodeId.should.equal("does-not-exist");
        });
    });

    it("preserves pre-error sends and returns a structured (non-string) error when the node throws", function() {
        return bootWith(ERROR_FLOW).then(function(ctx) {
            return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "e1", msg: { payload: 1, _msgid: "m4" } });
        }).then(function(result) {
            should.exist(result.error);
            result.error.should.be.an.Object();
            result.error.code.should.be.a.String();
            result.error.message.should.containEql("boom");
            result.sends.should.eql([]);
        });
    });

    it("issue #32: a node error routed to a wired Catch node resolves with sends, no error, plus a handledError marker", function() {
        return bootWith(CATCH_FLOW).then(function(ctx) {
            return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 1, _msgid: "m5" } });
        }).then(function(result) {
            should.not.exist(result.error);
            result.sends.length.should.equal(1);
            result.sends[0].destinationId.should.equal("n3");
            result.handledError.code.should.equal("NODE_ERROR");
            result.handledError.nodeId.should.equal("n2");
            result.handledError.message.should.containEql("boom");
        });
    });

    describe("issue #89: optional suspension adapter seam", function() {
        function bootWithAdapter(flowFile, suspensionAdapter) {
            return bootstrap(flowFile).then(function(h) {
                handle = h;
                capture = new Capture();
                capture.install(RED);
                var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture, suspensionAdapter: suspensionAdapter });
                return { handle: h, executeNode: executeNode };
            });
        }

        it("with no adapter installed, a resume input fails loudly (non-retryable) instead of silently executing the node", function() {
            return bootWith(FLOW).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 1, _msgid: "m1" }, resume: { type: "timer", continuation: {} } });
            }).then(function(result) {
                result.sends.should.eql([]);
                result.error.code.should.equal("SUSPENSION_UNSUPPORTED");
                result.error.nodeId.should.equal("n2");
            });
        });

        it("with an adapter whose plan() returns null, the node executes exactly as if no adapter were installed", function() {
            var adapter = { plan: function() { return null; } };
            return bootWithAdapter(FLOW, adapter).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "m2" } });
            }).then(function(result) {
                should.not.exist(result.error);
                should.not.exist(result.suspension);
                result.sends.length.should.equal(1);
                result.sends[0].msg.payload.should.equal(42);
            });
        });

        it("an adapter's plan() may suspend the invocation before the node ever runs, returning {sends: [], suspension}", function() {
            var planCalls = [];
            var adapter = {
                plan: function(node, msg) {
                    planCalls.push(msg.payload);
                    return { type: "timer", durationMs: 60000, continuation: { resumeAt: "later" } };
                }
            };
            return bootWithAdapter(FLOW, adapter).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "m3" } });
            }).then(function(result) {
                should.not.exist(result.error);
                result.sends.should.eql([]);
                result.suspension.should.eql({ type: "timer", durationMs: 60000, continuation: { resumeAt: "later" } });
                planCalls.should.eql([21]);
            });
        });

        it("a malformed suspension from plan() fails loudly (non-retryable SUSPENSION_INVALID), never silently falling back to local execution", function() {
            var adapter = { plan: function() { return { type: "timer", durationMs: -1 }; } };
            return bootWithAdapter(FLOW, adapter).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "m4" } });
            }).then(function(result) {
                result.sends.should.eql([]);
                result.error.code.should.equal("SUSPENSION_INVALID");
            });
        });

        it("resume input routes to adapter.resume(node, msg, resume) instead of the normal invocation path", function() {
            var resumeCalls = [];
            var adapter = {
                plan: function() { return { type: "signal", key: "approval:1" }; },
                resume: function(node, msg, resume) {
                    resumeCalls.push(resume);
                    return { sends: [{ port: 0, destinationId: "n3", msg: Object.assign({}, msg, { approved: true }) }] };
                }
            };
            return bootWithAdapter(FLOW, adapter).then(function(ctx) {
                return ctx.executeNode({
                    flowVersion: ctx.handle.flowVersion,
                    nodeId: "n2",
                    msg: { payload: 21, _msgid: "m5" },
                    resume: { type: "signal", continuation: { foo: "bar" }, signal: { approvedBy: "alice" } }
                });
            }).then(function(result) {
                should.not.exist(result.error);
                result.sends.length.should.equal(1);
                result.sends[0].destinationId.should.equal("n3");
                result.sends[0].msg.approved.should.equal(true);
                resumeCalls.length.should.equal(1);
                resumeCalls[0].should.eql({ type: "signal", continuation: { foo: "bar" }, signal: { approvedBy: "alice" } });
            });
        });

        it("adapter output is normalized into the existing Activity result shape (mapSends strips extra fields)", function() {
            var adapter = {
                resume: function() {
                    return { sends: [{ port: 0, destinationId: "n3", msg: { payload: 1 }, extraneous: "drop-me" }] };
                }
            };
            return bootWithAdapter(FLOW, adapter).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 1, _msgid: "m6" }, resume: { type: "timer" } });
            }).then(function(result) {
                result.sends.should.eql([{ port: 0, destinationId: "n3", msg: { payload: 1 } }]);
            });
        });

        it("a nested suspension returned from adapter.resume() is rejected loudly rather than silently dropped", function() {
            var adapter = {
                resume: function() {
                    return { sends: [], suspension: { type: "timer", durationMs: 1000 } };
                }
            };
            return bootWithAdapter(FLOW, adapter).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 1, _msgid: "m7" }, resume: { type: "timer" } });
            }).then(function(result) {
                result.sends.should.eql([]);
                result.error.code.should.equal("SUSPENSION_INVALID");
            });
        });
    });
});
