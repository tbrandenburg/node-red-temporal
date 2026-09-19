var should = require("should");
var path = require("path");
var { resolveNext, runFlow } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/workflows.js");
var { extractWireGraph } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/wireGraph.js");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");
var redUtil = require("../../../../../packages/node_modules/@node-red/util");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - resolveNext", function() {
    var graph = {
        n1: [["n2"], ["n3", "n4"]],
        n2: [[]]
    };

    it("returns the first destination wired to the send's port", function() {
        resolveNext(graph, "n1", { port: 0, msg: {} }).should.equal("n2");
    });

    it("ignores fan-out: only the first destination on a multi-destination port", function() {
        resolveNext(graph, "n1", { port: 1, msg: {} }).should.equal("n3");
    });

    it("returns undefined when the port has no wiring", function() {
        should.not.exist(resolveNext(graph, "n2", { port: 0, msg: {} }));
    });

    it("returns undefined when there is no send at all (end of path)", function() {
        should.not.exist(resolveNext(graph, "n1", undefined));
    });

    it("returns undefined when the node id is not in the graph", function() {
        should.not.exist(resolveNext(graph, "unknown", { port: 0, msg: {} }));
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - runFlow", function() {
    var graph = { n1: [["n2"]], n2: [[]] };

    it("walks a single path node-by-node until a send has no further wiring", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: { payload: input.msg.payload + 1 } }] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: { payload: 1 } })
            .then(function(result) {
                calls.length.should.equal(2);
                calls[0].should.eql({ flowVersion: "v1", nodeId: "n1", msg: { payload: 1 } });
                calls[1].should.eql({ flowVersion: "v1", nodeId: "n2", msg: { payload: 2 } });
                result.flowVersion.should.equal("v1");
                result.lastNode.should.equal("n2");
            });
    });

    it("throws a nonRetryable ApplicationFailure when the Activity result carries a NODE_ERROR", function() {
        var executeNode = function() {
            return Promise.resolve({
                sends: [],
                error: { code: "NODE_ERROR", nodeId: "n1", message: "boom" }
            });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.nonRetryable.should.equal(true);
                err.message.should.containEql("NODE_ERROR");
                err.message.should.containEql("boom");
            });
    });

    it("throws a nonRetryable ApplicationFailure on FLOW_VERSION_MISMATCH", function() {
        var executeNode = function() {
            return Promise.resolve({
                sends: [],
                error: { code: "FLOW_VERSION_MISMATCH", nodeId: "n1", message: "stale" }
            });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.nonRetryable.should.equal(true);
            });
    });

    it("never invokes executeNode for a falsy startNode", function() {
        var calls = 0;
        var executeNode = function() { calls++; return Promise.resolve({ sends: [] }); };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: undefined, startMsg: {} })
            .then(function(result) {
                calls.should.equal(0);
                should.not.exist(result.lastNode);
            });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - runFlow against a real bootstrapped flow (A2 #3)", function() {
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

    it("fails loudly (nonRetryable ApplicationFailure), not silently mis-routes, when flowVersion changes mid-flight", function() {
        return bootstrap(FLOW).then(function(h) {
            handle = h;
            capture = new Capture();
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            var graph = extractWireGraph(JSON.parse(require("fs").readFileSync(FLOW, "utf8")));

            // Simulate "the flow file changed while a workflow is in flight"
            // by starting the run with a flowVersion that no longer matches
            // what the (already-booted, unchanged) worker's `executeNode`
            // was closed over - exactly what a redeploy would produce for an
            // in-flight workflow still carrying the old `flowVersion` input.
            return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "stale-version-from-before-redeploy", startNode: "n1", startMsg: { payload: 1 } });
        }).then(function() {
            throw new Error("expected runFlow to throw on flowVersion mismatch");
        }, function(err) {
            err.nonRetryable.should.equal(true);
            err.message.should.containEql("FLOW_VERSION_MISMATCH");
        });
    });
});
