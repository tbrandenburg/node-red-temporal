var should = require("should");
var path = require("path");
var RED = require("nr-test-utils").require("node-red/lib/red");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");
var { createDelaySuspensionAdapter } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/delaySuspensionAdapter.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var DELAY_FLOW = path.join(FIXTURES, "delay-flow.json");
var DELAY_RANDOM_FLOW = path.join(FIXTURES, "delay-random-flow.json");
var FOUR_NODE_FLOW = path.join(FIXTURES, "four-node-flow.json");

describe("@tbrandenburg/node-red-temporal-runtime/lib/delaySuspensionAdapter (issue #90)", function() {
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
            var adapter = createDelaySuspensionAdapter({ capture: capture });
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture, suspensionAdapter: adapter });
            return { handle: h, executeNode: executeNode, adapter: adapter };
        });
    }

    describe("plan(): supported fixed-delay mode", function() {
        it("the initial Activity completes quickly with a timer suspension instead of waiting node.timeout ms", function() {
            return bootWith(DELAY_FLOW).then(function(ctx) {
                var start = Date.now();
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "delay1", msg: { payload: 1, _msgid: "m1" } }).then(function(result) {
                    var elapsed = Date.now() - start;
                    // fixture's configured delay is 200ms - a plan-only
                    // Activity must not wait anywhere near that long.
                    elapsed.should.be.below(100);
                    should.not.exist(result.error);
                    result.sends.should.eql([]);
                    result.suspension.type.should.equal("timer");
                    result.suspension.durationMs.should.equal(200);
                });
            });
        });

        it("does not leave a stock Delay setTimeout/buffer pending (idList stays empty)", function() {
            return bootWith(DELAY_FLOW).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "delay1", msg: { payload: 1, _msgid: "m2" } }).then(function() {
                    var node = ctx.handle.getNode("delay1");
                    node.idList.should.eql([]);
                });
            });
        });
    });

    describe("resume(): routes the delayed message through Node-RED's own resolved routing", function() {
        it("produces the same destinationId a normal delivery would, without calling receive()/the stock timer", function() {
            return bootWith(DELAY_FLOW).then(function(ctx) {
                return ctx.executeNode({
                    flowVersion: ctx.handle.flowVersion,
                    nodeId: "delay1",
                    msg: { payload: 42, _msgid: "m3" },
                    resume: { type: "timer", continuation: null }
                }).then(function(result) {
                    should.not.exist(result.error);
                    result.sends.length.should.equal(1);
                    result.sends[0].destinationId.should.equal("n2");
                    result.sends[0].msg.payload.should.equal(42);
                });
            });
        });
    });

    describe("unsupported modes fall through unchanged", function() {
        it("a non-'delay' pauseType (e.g. random) is never suspended - ordinary local execution still applies", function() {
            return bootWith(DELAY_RANDOM_FLOW).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "delay1", msg: { payload: 1, _msgid: "m4" } }).then(function(result) {
                    should.not.exist(result.suspension);
                });
            });
        });

        it("a non-delay node type is never touched by the adapter", function() {
            return bootWith(FOUR_NODE_FLOW).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "m5" } }).then(function(result) {
                    should.not.exist(result.suspension);
                    should.not.exist(result.error);
                    result.sends.length.should.equal(1);
                });
            });
        });

        it("a 'reset' control message on a fixed-delay node is never suspended", function() {
            return bootWith(DELAY_FLOW).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "delay1", msg: { reset: true, _msgid: "m6" } }).then(function(result) {
                    should.not.exist(result.suspension);
                });
            });
        });

        it("a 'flush' control message on a fixed-delay node is never suspended", function() {
            return bootWith(DELAY_FLOW).then(function(ctx) {
                return ctx.executeNode({ flowVersion: ctx.handle.flowVersion, nodeId: "delay1", msg: { flush: true, _msgid: "m7" } }).then(function(result) {
                    should.not.exist(result.suspension);
                });
            });
        });
    });

    describe("plan() directly (no Activity boundary)", function() {
        it("returns undefined for any non-'delay' node type", function() {
            return bootWith(FOUR_NODE_FLOW).then(function(ctx) {
                var node = ctx.handle.getNode("n2");
                should.not.exist(ctx.adapter.plan(node, { payload: 1 }));
            });
        });

        it("returns undefined when the node has no positive numeric timeout", function() {
            var adapter = createDelaySuspensionAdapter({ capture: null });
            should.not.exist(adapter.plan({ type: "delay", pauseType: "delay", timeout: 0 }, {}));
            should.not.exist(adapter.plan({ type: "delay", pauseType: "delay", timeout: "not-a-number" }, {}));
        });

        it("accepts a numeric-string timeout (stock Delay's own 'milliseconds' constructor output)", function() {
            var adapter = createDelaySuspensionAdapter({ capture: null });
            var descriptor = adapter.plan({ type: "delay", pauseType: "delay", timeout: "200" }, {});
            descriptor.type.should.equal("timer");
            descriptor.durationMs.should.equal(200);
        });
    });
});
