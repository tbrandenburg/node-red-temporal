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
});
