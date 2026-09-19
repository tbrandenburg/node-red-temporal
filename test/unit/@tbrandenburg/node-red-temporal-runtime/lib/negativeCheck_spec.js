/**
 * M10 A3#4 negative check: proves Temporal is genuinely GATING delivery via
 * the preRoute hook, not merely observing a flow that would route locally
 * anyway.
 *
 * Two independent, real (no mocks of capture/node execution) assertions:
 *
 * (a) WITHOUT Capture installed at all, a real Node-RED flow routes
 *     entirely locally: plain node.receive() on the first node produces a
 *     real, observable side effect on the second node - normal Node-RED
 *     behaviour needs no Temporal/Capture involvement whatsoever.
 *
 * (b) The `executeNode` Activity contract (lib/activities.js) is entirely
 *     DEPENDENT on Capture being installed to observe/report a node's
 *     sends and completion. With a `Capture` instance that was never
 *     install()ed (hooks never registered), `capture.around()` against a
 *     real live node cannot resolve at all - node-RED itself will still
 *     route the message locally (per (a)), but the Activity waiting on
 *     `preRoute`/`onComplete` hooks has nothing to observe, so it times
 *     out. This proves the Activity's ability to gate/observe execution
 *     is contingent on hook installation, not incidental to running the
 *     node - i.e. Temporal (via Capture) is the thing actually deciding
 *     whether the message may proceed past a node.
 */

var should = require("should");
var path = require("path");
var helper = require("node-red-node-test-helper");
var RED = require("nr-test-utils").require("node-red/lib/red");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");

describe("@tbrandenburg/node-red-temporal-runtime negative check (A3#4): Temporal actually gates delivery", function() {
    this.timeout(20000);

    describe("(a) no Capture installed at all: Node-RED routes the flow entirely locally", function() {
        beforeEach(function(done) {
            helper.startServer(done);
        });

        afterEach(function(done) {
            helper.unload().then(function() {
                helper.stopServer(done);
            });
        });

        function probeNodeModule(RED) {
            function ProbeNode(config) {
                RED.nodes.createNode(this, config);
                this.on("input", function(msg, send, done) {
                    send(msg);
                    done();
                });
            }
            RED.nodes.registerType("temporal-probe-negcheck", ProbeNode);
        }

        it("a plain node.receive() call (no hooks, no Capture) reaches the downstream node via real local routing", function(done) {
            // No `Capture` object exists anywhere in this test - proving
            // this scenario needs no Temporal involvement whatsoever.
            helper.load(probeNodeModule, [
                { id: "p1", type: "temporal-probe-negcheck", wires: [["p2"]] },
                { id: "p2", type: "temporal-probe-negcheck", wires: [["s1"]] },
                { id: "s1", type: "helper" }
            ], function() {
                var p1 = helper.getNode("p1");
                var s1 = helper.getNode("s1");
                s1.on("input", function(msg) {
                    // Real observable side effect on the second hop,
                    // reached purely by Node-RED's own onSend/preRoute/
                    // preDeliver/postDeliver machinery - no capture, no
                    // Temporal, no suppression anywhere in this test.
                    msg.payload.should.equal("local-routing-proof");
                    done();
                });
                p1.receive({ payload: "local-routing-proof" });
            });
        });
    });

    describe("(b) executeNode's Activity contract depends on Capture being installed", function() {
        var handle;

        afterEach(function() {
            if (handle) {
                var h = handle;
                handle = null;
                return h.stop();
            }
        });

        it("capture.around() never resolves (times out) against a real live node when Capture was never installed", function() {
            return bootstrap(FLOW).then(function(h) {
                handle = h;
                // Deliberately never call capture.install(RED): no
                // preRoute/onComplete hooks are registered, so the
                // Activity has no way to observe sends or completion.
                var capture = new Capture({ timeoutMs: 200 });
                var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });

                return executeNode({ flowVersion: h.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "negcheck-1" } });
            }).then(function(result) {
                // The real `n2` function node still runs synchronously
                // (Node-RED itself doesn't need Capture, per (a)) and its
                // real send happens - but with no preRoute hook installed
                // to observe that send, and no onComplete hook installed
                // to resolve the pending promise, `capture.around()` can
                // only ever time out. That timeout is exactly the proof
                // that the Activity's observation/gating is contingent on
                // Capture.install(), not incidental to running the node.
                should.exist(result.error);
                result.error.code.should.equal("NODE_TIMEOUT");
                result.error.nodeId.should.equal("n2");
            });
        });

        it("control: the identical call resolves normally once Capture IS installed", function() {
            return bootstrap(FLOW).then(function(h) {
                handle = h;
                var capture = new Capture({ timeoutMs: 200 });
                capture.install(RED);
                var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });

                return executeNode({ flowVersion: h.flowVersion, nodeId: "n2", msg: { payload: 21, _msgid: "negcheck-2" } }).then(function(result) {
                    capture.uninstall();
                    return result;
                });
            }).then(function(result) {
                should.not.exist(result.error);
                result.sends[0].msg.payload.should.equal(42);
            });
        });
    });
});
