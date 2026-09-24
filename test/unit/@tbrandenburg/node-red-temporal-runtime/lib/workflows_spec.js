var should = require("should");
var path = require("path");
var { resolveDestinations, runFlow, createExecuteNode: makeExecuteNodeProxy, buildActivitySummary } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/workflows.js");
var { extractWireGraph } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/wireGraph.js");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");
var redUtil = require("../../../../../packages/node_modules/@node-red/util");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");
var FANOUT_FLOW = path.join(FIXTURES, "fanout-flow.json");
var MULTI_SEND_FLOW = path.join(FIXTURES, "multi-send-flow.json");
var MULTI_OUTPUT_FLOW = path.join(FIXTURES, "multi-output-flow.json");
var LINK_FLOW = path.join(FIXTURES, "link-flow.json");
var SUBFLOW_FLOW = path.join(FIXTURES, "subflow-flow.json");
var NESTED_SUBFLOW_FLOW = path.join(FIXTURES, "nested-subflow-flow.json");
var CATCH_FLOW = path.join(FIXTURES, "catch-flow.json");
var CATCH_PRESEND_FLOW = path.join(FIXTURES, "catch-presend-flow.json");
var ERROR_FLOW = path.join(FIXTURES, "error-flow.json");
var COMPLETE_FLOW = path.join(FIXTURES, "complete-flow.json");
var JOIN_FLOW = path.join(FIXTURES, "join-flow.json");
var LOOP_FLOW = path.join(FIXTURES, "loop-flow.json");
var DELAY_FLOW = path.join(FIXTURES, "delay-flow.json");

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - resolveDestinations", function() {
    var graph = {
        n1: [["n2"], ["n3", "n4"]],
        n2: [[]]
    };

    it("returns all destinations wired to the send's port", function() {
        resolveDestinations(graph, "n1", { port: 0, msg: {} }).should.eql(["n2"]);
    });

    it("follows fan-out: every destination on a multi-destination port", function() {
        resolveDestinations(graph, "n1", { port: 1, msg: {} }).should.eql(["n3", "n4"]);
    });

    it("returns an empty array when the port has no wiring", function() {
        resolveDestinations(graph, "n2", { port: 0, msg: {} }).should.eql([]);
    });

    it("returns an empty array when there is no send at all (end of path)", function() {
        resolveDestinations(graph, "n1", undefined).should.eql([]);
    });

    it("returns an empty array when the node id is not in the graph", function() {
        resolveDestinations(graph, "unknown", { port: 0, msg: {} }).should.eql([]);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - runFlow enqueues resolved destinationId directly (issue #13)", function() {
    it("enqueues each send's own destinationId without consulting graph at all", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "n2", msg: {} }] });
            }
            return Promise.resolve({ sends: [] });
        };
        // graph is intentionally EMPTY/wrong (would resolve to nothing, or
        // something different) to prove destinationId is used directly and
        // graph is never consulted when destinationId is present.
        return runFlow({ executeNode: executeNode, graph: { n1: [["some-other-node"]] }, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.should.eql(["n1", "n2"]);
            });
    });

    it("does NOT dedupe two distinct sends on the SAME port - both destinationIds are enqueued", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n1") {
                // two node.send() calls on the same port, each captured as
                // its own {port, destinationId, msg} entry by Capture -
                // must remain two distinct downstream deliveries.
                return Promise.resolve({ sends: [
                    { port: 0, destinationId: "n2", msg: { seq: 1 } },
                    { port: 0, destinationId: "n2", msg: { seq: 2 } }
                ] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: {}, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.should.eql(["n1", "n2", "n2"]);
            });
    });

    it("enqueues fan-out (multiple wires on one output) from destinationId entries alone", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [
                    { port: 0, destinationId: "n2", msg: {} },
                    { port: 0, destinationId: "n3", msg: {} }
                ] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: {}, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.sort().should.eql(["n1", "n2", "n3"]);
            });
    });

    it("falls back to resolveDestinations/graph for sends without a destinationId (backward compatibility)", function() {
        var calls = [];
        var fallbackGraph = { n1: [["n2"]] };
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: {} }] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: fallbackGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.should.eql(["n1", "n2"]);
            });
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

    it("fans out to every destination wired on a port (branch: A -> B and A -> C)", function() {
        var fanoutGraph = { n1: [["n2", "n3"]], n2: [[]], n3: [[]] };
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            return Promise.resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: { payload: 1 } }] : [] });
        };
        return runFlow({ executeNode: executeNode, graph: fanoutGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.should.eql(["n1", "n2", "n3"]);
            });
    });

    it("fans out across multiple sends and multiple ports on the same node", function() {
        var fanoutGraph = { n1: [["n2"], ["n3"]], n2: [[]], n3: [[]] };
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: {} }, { port: 1, msg: {} }] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: fanoutGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.should.eql(["n1", "n2", "n3"]);
            });
    });

    it("issue #24: dispatches an entire wave concurrently (independent branches no longer await one another)", function() {
        var fanoutGraph = { n1: [["n2", "n3"]], n2: [[]], n3: [[]] };
        var inFlight = 0;
        var maxInFlight = 0;
        var executeNode = function(input) {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            return new Promise(function(resolve) {
                setTimeout(function() {
                    inFlight--;
                    resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: {} }] : [] });
                }, 5);
            });
        };
        return runFlow({ executeNode: executeNode, graph: fanoutGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                // n1 runs alone (wave 1); n2 and n3 are siblings produced by
                // the SAME send and land in wave 2 together, so both are
                // in flight at once (issue #24 wave-based dispatch).
                maxInFlight.should.equal(2);
            });
    });

    it("still fails loudly on the first error, though same-wave siblings are dispatched concurrently first", function() {
        var fanoutGraph = { n1: [["n2", "n3"]], n2: [[]], n3: [[]] };
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n2") {
                return Promise.resolve({ sends: [], error: { code: "NODE_ERROR", nodeId: "n2", message: "boom" } });
            }
            return Promise.resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: {} }] : [] });
        };
        return runFlow({ executeNode: executeNode, graph: fanoutGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.nonRetryable.should.equal(true);
                // n2 and n3 are same-wave siblings (both produced by n1's
                // send), so both are dispatched together before the error is
                // even inspected (issue #24) - but no LATER wave is ever
                // scheduled, so the run still aborts after this one wave.
                calls.should.eql(["n1", "n2", "n3"]);
            });
    });

});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - runFlow with input.initial (M4 source fan-out)", function() {
    var graph = { n2: [[]], n3: [[]] };

    it("seeds the pending queue from initial instead of startNode/startMsg, invoking one executeNode per entry", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input);
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", initial: [{ nodeId: "n2", msg: { payload: "a" } }, { nodeId: "n3", msg: { payload: "b" } }] })
            .then(function() {
                calls.should.eql([
                    { flowVersion: "v1", nodeId: "n2", msg: { payload: "a" } },
                    { flowVersion: "v1", nodeId: "n3", msg: { payload: "b" } }
                ]);
            });
    });

    it("initial takes precedence over startNode/startMsg when both are given", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, initial: [{ nodeId: "n2", msg: {} }] })
            .then(function() {
                calls.should.eql(["n2"]);
            });
    });

    it("falls back to today's single startNode/startMsg behavior when initial is not provided", function() {
        var singleGraph = { n1: [["n2"]], n2: [[]] };
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            return Promise.resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: {} }] : [] });
        };
        return runFlow({ executeNode: executeNode, graph: singleGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                calls.should.eql(["n1", "n2"]);
            });
    });

    it("one Inject fire fanning out to two branches (Inject -> Change A, Inject -> Change B) runs both, no duplicate executions", function() {
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", initial: [{ nodeId: "n2", msg: { payload: 1 } }, { nodeId: "n3", msg: { payload: 1 } }] })
            .then(function() {
                calls.sort().should.eql(["n2", "n3"]);
            });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - executeFlow forwards input.initial", function() {
    it("threads input.initial through to runFlow via createExecuteNode (real Workflow entry point shape)", function() {
        // executeFlow itself calls the real Temporal-proxied createExecuteNode,
        // which requires a live Workflow context - so this just asserts the
        // function accepts and forwards the field, exercised indirectly via
        // the already-covered runFlow tests above (executeFlow is a thin
        // wrapper with no branching logic of its own to unit test in
        // isolation without a Workflow sandbox).
        var workflows = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/workflows.js");
        workflows.executeFlow.should.be.a.Function();
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - buildActivitySummary (issue #6)", function() {
    it("combines type and name when both are present", function() {
        buildActivitySummary({ type: "http request", name: "get httpbin" }).should.equal("http request — get httpbin");
    });

    it("falls back to just the type when name is absent/empty", function() {
        buildActivitySummary({ type: "debug" }).should.equal("debug");
        buildActivitySummary({ type: "debug", name: "" }).should.equal("debug");
    });

    it("returns undefined when there is no metadata at all for the node", function() {
        should(buildActivitySummary(undefined)).be.undefined();
        should(buildActivitySummary({})).be.undefined();
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - runFlow with createExecuteNode factory (issue #6)", function() {
    var graph = { n1: [["n2"]], n2: [[]] };
    var nodeMeta = { n1: { name: "start", type: "inject" }, n2: { name: "out", type: "debug" } };

    it("invokes the createExecuteNode factory once per queue item, with a per-node 1-based invocation counter", function() {
        var factoryCalls = [];
        var makeExecuteNode = function(nodeId, invocation, meta) {
            factoryCalls.push({ nodeId: nodeId, invocation: invocation, meta: meta });
            return function() { return Promise.resolve({ sends: nodeId === "n1" ? [{ port: 0, msg: {} }] : [] }); };
        };
        return runFlow({ createExecuteNode: makeExecuteNode, graph: graph, nodeMeta: nodeMeta, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                factoryCalls.should.eql([
                    { nodeId: "n1", invocation: 1, meta: nodeMeta },
                    { nodeId: "n2", invocation: 1, meta: nodeMeta }
                ]);
            });
    });

    it("increments the invocation counter per distinct visit of the SAME node id (e.g. a fan-in revisit)", function() {
        var revisitGraph = { n1: [["n2", "n3"]], n2: [["n4"]], n3: [["n4"]], n4: [[]] };
        var factoryCalls = [];
        var makeExecuteNode = function(nodeId, invocation) {
            factoryCalls.push(nodeId + ":" + invocation);
            return function(input) {
                return Promise.resolve({ sends: (revisitGraph[input.nodeId] && revisitGraph[input.nodeId][0] && revisitGraph[input.nodeId][0].length) ? [{ port: 0, msg: {} }] : [] });
            };
        };
        return runFlow({ createExecuteNode: makeExecuteNode, graph: revisitGraph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                // n4 is reached twice (once via n2, once via n3) - its invocation
                // counter must be 1 then 2, not 1 then 1.
                factoryCalls.should.eql(["n1:1", "n2:1", "n3:1", "n4:1", "n4:2"]);
            });
    });

    it("prefers the createExecuteNode factory over a plain executeNode when both are given (real Workflow precedence)", function() {
        var factoryCalls = [];
        var executeNode = function() { throw new Error("should not be called"); };
        var makeExecuteNode = function(nodeId) {
            factoryCalls.push(nodeId);
            return function() { return Promise.resolve({ sends: [] }); };
        };
        return runFlow({ executeNode: executeNode, createExecuteNode: makeExecuteNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function() {
                factoryCalls.should.eql(["n1"]);
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

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - runFlow fan-out against a real bootstrapped flow (issue #5)", function() {
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

    it("executes all 5 nodes of a true fan-out flow (Inject -> {Change A -> Debug A, Change B -> Debug B})", function() {
        return bootstrap(FANOUT_FLOW).then(function(h) {
            handle = h;
            capture = new Capture();
            capture.install(redUtil);
            var invoked = [];
            var realExecuteNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            var executeNode = function(input) {
                invoked.push(input.nodeId);
                return realExecuteNode(input);
            };
            var graph = extractWireGraph(JSON.parse(require("fs").readFileSync(FANOUT_FLOW, "utf8")));

            return runFlow({ executeNode: executeNode, graph: graph, flowVersion: h.flowVersion, startNode: "n1", startMsg: { payload: 1 } })
                .then(function() {
                    invoked.sort().should.eql(["n1", "n2", "n3", "n4", "n5"]);
                });
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - real Node-RED compatibility cases (issue #13)", function() {
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

    function runWithRealFlow(flowFile, startNode, startMsg, maxNodeExecutions) {
        return bootstrap(flowFile).then(function(h) {
            handle = h;
            capture = new Capture();
            capture.install(redUtil);
            var invoked = [];
            var realExecuteNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            var executeNode = function(input) {
                invoked.push(input.nodeId);
                return realExecuteNode(input);
            };
            // graph deliberately NOT extracted from flowFile (or extracted but
            // irrelevant for link-flow) - proves the run is driven entirely by
            // Node-RED's own resolved destinationId, not by a wire-graph lookup.
            return runFlow({ executeNode: executeNode, graph: {}, flowVersion: h.flowVersion, startNode: startNode, startMsg: startMsg, maxNodeExecutions: maxNodeExecutions })
                .then(function(result) {
                    return { invoked: invoked, result: result };
                });
        });
    }

    it("two distinct node.send() calls on the same port produce two distinct downstream deliveries (not deduped)", function() {
        return runWithRealFlow(MULTI_SEND_FLOW, "n2", { payload: 1, _msgid: "multisend-1" }).then(function(r) {
            // n2 calls node.send(msg) twice, wired to [n3, n4] - Node-RED's
            // own preRoute fires once per (send-call x destination) = 4
            // times, so both n3 and n4 must each be invoked TWICE.
            r.invoked.filter(function(id) { return id === "n3"; }).length.should.equal(2);
            r.invoked.filter(function(id) { return id === "n4"; }).length.should.equal(2);
        });
    });

    it("multi-output routing matches Node-RED: each output port's own send reaches only its own wired destination", function() {
        return runWithRealFlow(MULTI_OUTPUT_FLOW, "n2", { payload: 1, _msgid: "multiout-1" }).then(function(r) {
            r.invoked.sort().should.eql(["n2", "n3", "n4"]);
        });
    });

    it("Link Out -> Link In works with NO bespoke Link routing (Link Out's own wires are empty; destinationId alone drives it)", function() {
        return runWithRealFlow(LINK_FLOW, "n1", { payload: 1, _msgid: "link-1" }).then(function(r) {
            r.invoked.should.eql(["n1", "link-out-1", "link-in-1", "n2"]);
        });
    });

    it("issue #32: a Catch node's own real Node-RED routing resolves (not rejects) so the Workflow can enqueue it (capture-layer proof)", function() {
        // Fixed (was a known limitation): Capture's `_settleError` now
        // resolves - instead of rejecting - when Node-RED's own
        // `handleError` produced new sends during the `node.error()` call
        // (a Catch node it routed to actually forwarded the message). No
        // `result.error` means the Workflow-level drain loop below simply
        // continues, exactly like any ordinary successful invocation.
        return bootstrap(CATCH_FLOW).then(function(h) {
            handle = h;
            capture = new Capture();
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return executeNode({ flowVersion: h.flowVersion, nodeId: "n2", msg: { payload: 1, _msgid: "catch-1" } });
        }).then(function(result) {
            should.not.exist(result.error);
            result.handledError.code.should.equal("NODE_ERROR");
            // The Catch node (scope: ["n2"]) received the error synchronously
            // via Node-RED's own Flow.handleError and sent its own message to
            // n3 - captured here with n3's own resolved destinationId.
            result.sends.length.should.equal(1);
            result.sends[0].destinationId.should.equal("n3");
        });
    });

    it("issue #32: a handled Catch route is enqueued and the Workflow completes normally instead of failing", function() {
        return runWithRealFlow(CATCH_FLOW, "n2", { payload: 1, _msgid: "catch-workflow-1" }).then(function(r) {
            // n2 throws; Node-RED's own Flow.handleError routes it to catch1
            // (scope: ["n2"]), which sends onward to n3 - all captured as
            // n2's own resolved sends (same ALS invocationId throughout).
            // The Workflow enqueues n3 exactly like any ordinary send.
            r.invoked.should.eql(["n2", "n3"]);
            r.result.lastNode.should.equal("n3");
        });
    });

    it("issue #32: an unhandled node error (no Catch node wired) still fails the Workflow", function() {
        return runWithRealFlow(ERROR_FLOW, "e1", { payload: 1, _msgid: "unhandled-1" }).then(function() {
            throw new Error("expected runFlow to throw");
        }, function(err) {
            err.message.should.containEql("NODE_ERROR");
        });
    });

    it("issue #32: ordinary sends before a LATER unhandled error do not get falsely classified as a handled Catch route", function() {
        return runWithRealFlow(CATCH_PRESEND_FLOW, "n2", { payload: 1, _msgid: "presend-1" }).then(function() {
            throw new Error("expected runFlow to throw");
        }, function(err) {
            // n2 sends to n3 (ordinary output) BEFORE throwing. n3 is NOT the
            // Catch node's own scope target, and the flow's Catch node scope
            // does not cover n2, so the pre-error send must not be mistaken
            // for Node-RED's own Catch routing - the Workflow must still
            // fail for the later, genuinely unhandled error.
            err.message.should.containEql("NODE_ERROR");
        });
    });

    it("a Complete node's own real Node-RED routing is captured and reached by the Workflow (no error path involved)", function() {
        return runWithRealFlow(COMPLETE_FLOW, "n1", { payload: 1, _msgid: "complete-1" }).then(function(r) {
            // n2 (identity function, no wires of its own) completes -
            // Node-RED's Flow.handleComplete dispatches to the scoped
            // Complete node, which sends onward to n3.
            r.invoked.should.eql(["n1", "n2", "n3"]);
        });
    });

    it("a finite loop (self-wired node decrementing a counter) executes and terminates normally", function() {
        return runWithRealFlow(LOOP_FLOW, "loop1", { payload: 1, _msgid: "loop-1" }).then(function(r) {
            r.invoked.should.eql(["loop1", "loop1", "loop1", "n2"]);
        });
    });

    it("issue #34: a finite loop below the configured maxNodeExecutions limit is unaffected", function() {
        // LOOP_FLOW naturally terminates after 4 total node executions
        // (loop1 x3, then n2) - a limit comfortably above that must not
        // change behavior at all.
        return runWithRealFlow(LOOP_FLOW, "loop1", { payload: 1, _msgid: "loop-1" }, 10).then(function(r) {
            r.invoked.should.eql(["loop1", "loop1", "loop1", "n2"]);
        });
    });

    it("issue #34: a loop that would exceed maxNodeExecutions fails fast with a non-retryable FLOW_EXECUTION_LIMIT error instead of running to natural completion", function() {
        // Same finite LOOP_FLOW fixture, but with a maxNodeExecutions so low
        // (2) that it trips BEFORE the loop's own natural 4-execution
        // termination - proves the limit is enforced deterministically and
        // does not depend on a truly-infinite fixture to exercise it.
        return runWithRealFlow(LOOP_FLOW, "loop1", { payload: 1, _msgid: "loop-1" }, 2).then(function() {
            throw new Error("expected runFlow to throw FLOW_EXECUTION_LIMIT");
        }, function(err) {
            err.type.should.equal("FLOW_EXECUTION_LIMIT");
            err.nonRetryable.should.equal(true);
        });
    });

    it("issue #34: every scheduled node invocation in a fan-out wave counts toward the limit, not just one per wave", function() {
        // n1 fans out to n3 AND n4 (2 executions in wave 2) after itself (1
        // execution in wave 1) = 3 total. A limit of 2 must trip on the
        // fan-out wave even though only 1 "logical" node produced it,
        // proving the counter counts invocations, not waves or source nodes.
        var fanoutGraph = { n1: [["n3", "n4"]], n3: [[]], n4: [[]] };
        var executeNode = function(input) {
            return Promise.resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: {} }, { port: 0, msg: {} }] : [] });
        };
        return runFlow({ executeNode: executeNode, graph: fanoutGraph, flowVersion: "v1", startNode: "n1", startMsg: {}, maxNodeExecutions: 2 })
            .then(function() {
                throw new Error("expected runFlow to throw FLOW_EXECUTION_LIMIT");
            }, function(err) {
                err.type.should.equal("FLOW_EXECUTION_LIMIT");
                err.nonRetryable.should.equal(true);
            });
    });

    it("issue #23: a subflow instance executes through its normal Node-RED runtime representation - the subflow's OWN internal routing (n2 -> its internal doubling function) is no longer suppressed/timed out, and the subflow's real external hop to n3 is still captured", function() {
        return bootstrap(SUBFLOW_FLOW).then(function(h) {
            handle = h;
            capture = new Capture({ timeoutMs: 500 });
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return executeNode({ flowVersion: h.flowVersion, nodeId: "n2", msg: { payload: 5, _msgid: "subflow-1" } });
        }).then(function(result) {
            // Invoking n2 (the subflow instance) resolves normally instead
            // of timing out - the internal hop to the subflow's own
            // doubling function is routed locally by Node-RED, not
            // suppressed as an Activity-worthy send.
            should(result.error).be.undefined();
            // The subflow's genuinely external hop (its output boundary
            // wired to n3) IS still captured normally - a real external hop
            // must never be silently dropped just because internal hops
            // are now let through.
            result.sends.length.should.equal(1);
            result.sends[0].destinationId.should.equal("n3");
            result.sends[0].msg.payload.should.equal(10);
        });
    });

    it("issue #33: nested subflows - internal hops at BOTH levels (outer instance -> inner instance, inner instance -> its internal function) are routed locally by Node-RED and never suppressed/timed out; only the hop leaving the outer subflow's boundary (to n3) is captured", function() {
        return bootstrap(NESTED_SUBFLOW_FLOW).then(function(h) {
            handle = h;
            capture = new Capture({ timeoutMs: 500 });
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return executeNode({ flowVersion: h.flowVersion, nodeId: "n2", msg: { payload: 5, _msgid: "nested-subflow-1" } });
        }).then(function(result) {
            // Invoking n2 (the OUTER subflow instance) resolves normally
            // instead of timing out - both the hop into the nested inner
            // subflow instance and that inner instance's own hop to its
            // internal doubling function are routed locally, not
            // suppressed as Activity-worthy sends.
            should(result.error).be.undefined();
            // Only the outer subflow's genuine external hop (its output
            // boundary wired to n3) is captured - exactly one send.
            result.sends.length.should.equal(1);
            result.sends[0].destinationId.should.equal("n3");
            result.sends[0].msg.payload.should.equal(10);
        });
    });

    it("issue #31: real Join node (count-based fan-in) completes successfully - wave-based drain (#24) plus msg-identity ALS fallback (#31) fix both the scheduling deadlock and the capture.js correlation bug (previously a KNOWN LIMITATION)", function() {
        // n1 fans out to n2 and n3, both feeding join1 (count: 2). Before
        // #24's fix, the strictly-sequential drain awaited n2->join1's own
        // executeNode call to settle BEFORE ever calling n3 - so n3 (the
        // only source of join1's second required message) was NEVER
        // scheduled: a pure scheduling deadlock. The wave-based drain fixes
        // exactly that: n2 and n3 now land in the same wave and are
        // dispatched together via Promise.all, so join1 DOES receive both
        // messages and DOES produce its joined send.
        //
        // That alone still wasn't enough: Node-RED's own `join` node
        // (17-split.js's custom/count mode) defers the FIRST arriving
        // message's `done()` callback and only invokes it (together with
        // the second message's own `done()`) from within the SECOND
        // message's synchronous `node.receive()` call stack (`completeSend`'s
        // `group.dones.forEach(f => f())`). capture.js's `around()`/
        // `_onComplete()` used to assume each invocation's own `done()`
        // fires within ITS OWN AsyncLocalStorage call stack; here the first
        // invocation's `done()` used to be attributed to the SECOND
        // invocation's ALS context instead, so the first join1 invocation's
        // own Activity promise never settled and timed out (NODE_TIMEOUT).
        //
        // Fix (issue #31): capture.js now stores the exact `msg` object on
        // each pending entry and resolves `onComplete` events by matching
        // `completeEvent.msg` identity against pending entries for that
        // node, falling back away from the ALS-derived invocationId when it
        // doesn't match - correctly attributing the deferred `done()` back
        // to the FIRST (owning) invocation. The join flow now resolves
        // successfully end-to-end, with join1's joined message reaching n4.
        return bootstrap(JOIN_FLOW).then(function(h) {
            handle = h;
            capture = new Capture({ timeoutMs: 500 });
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return runFlow({ executeNode: executeNode, graph: {}, flowVersion: h.flowVersion, startNode: "n1", startMsg: { payload: 1, _msgid: "join-1" } });
        }).then(function(result) {
            should(result).be.ok();
            result.lastNode.should.equal("n4");
        });
    });

    it("issue #24: wave-based dispatch invokes both fan-in branches concurrently (mock executeNode)", function() {
        // n1 -> n2/n3 -> join1 -> n4, mirroring join-flow.json's shape but
        // driven by a plain mock executeNode + graph (no real Node-RED Join
        // semantics involved) - proves runFlow itself dispatches n2 and n3
        // in the SAME wave (both invoked before either's downstream send is
        // processed), not that Node-RED's Join node happens to work.
        var graph = { n1: [["n2", "n3"]], n2: [["join1"]], n3: [["join1"]], join1: [["n4"]], n4: [[]] };
        var invoked = [];
        var executeNode = function(input) {
            invoked.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: input.msg }] });
            }
            if (input.nodeId === "join1") {
                return Promise.resolve({ sends: [{ port: 0, msg: input.msg }] });
            }
            return Promise.resolve({ sends: [{ port: 0, msg: input.msg }] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {} }).then(function(result) {
            invoked.should.eql(["n1", "n2", "n3", "join1", "join1", "n4", "n4"]);
            result.lastNode.should.equal("n4");
        });
    });

    it("issue #24: an error in one branch of a wave still aborts the run (fail-loud preserved under concurrent dispatch)", function() {
        var graph = { n1: [["n2", "n3"]], n2: [[]], n3: [[]] };
        var executeNode = function(input) {
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: input.msg }] });
            }
            if (input.nodeId === "n3") {
                return Promise.resolve({ error: { code: "NODE_ERROR", message: "boom" } });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {} }).then(function() {
            throw new Error("expected runFlow to throw");
        }, function(err) {
            err.nonRetryable.should.equal(true);
            err.message.should.containEql("NODE_ERROR");
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #16: activityTaskQueue threading", function() {
    var graph = { n1: [["n2"]], n2: [[]] };

    it("runFlow forwards activityTaskQueue to the createExecuteNode factory as a 4th argument", function() {
        var factoryArgs = [];
        var makeExecuteNode = function(nodeId, invocation, meta, taskQueue) {
            factoryArgs.push(taskQueue);
            return function() { return Promise.resolve({ sends: [] }); };
        };
        return runFlow({ createExecuteNode: makeExecuteNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, activityTaskQueue: "custom-activity-q" })
            .then(function() {
                factoryArgs.should.eql(["custom-activity-q"]);
            });
    });

    it("createExecuteNode (real proxyActivities factory) accepts an optional taskQueue without throwing outside a Workflow sandbox check", function() {
        // proxyActivities() itself requires a live Workflow execution
        // context, so this only asserts the factory signature accepts the
        // 4th argument (real behavior is exercised end-to-end by the
        // manual multi-process acceptance test, per this file's own
        // existing convention for executeFlow/createExecuteNode above).
        makeExecuteNodeProxy.length.should.be.aboveOrEqual(3);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #47: node execution timeout threading", function() {
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

    it("createExecuteNode (real proxyActivities factory) accepts an optional nodeExecutionTimeoutMs as a 5th argument without throwing outside a Workflow sandbox", function() {
        // Mirrors the existing taskQueue (4th-argument) assertion above:
        // proxyActivities() itself requires a live Workflow execution
        // context, so only the factory signature is asserted here - the
        // derived startToCloseTimeout value itself is proven by the plain
        // Node.js unit test below (buildActivityOptionsFor-style check via
        // a stubbed proxyActivities) and by the real end-to-end Delay tests.
        makeExecuteNodeProxy.length.should.be.aboveOrEqual(4);
    });

    it("runFlow forwards nodeExecutionTimeoutMs to the createExecuteNode factory as a 5th argument", function() {
        var factoryArgs = [];
        var graph = { n1: [["n2"]], n2: [[]] };
        var makeExecuteNode = function(nodeId, invocation, meta, taskQueue, nodeExecutionTimeoutMs) {
            factoryArgs.push(nodeExecutionTimeoutMs);
            return function() { return Promise.resolve({ sends: [] }); };
        };
        return runFlow({ createExecuteNode: makeExecuteNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, nodeExecutionTimeoutMs: 12345 })
            .then(function() {
                factoryArgs.should.eql([12345]);
            });
    });

    it("a real Delay node completing after the old hardcoded 5s Capture default still succeeds when Capture is configured with a comfortably larger timeout (issue #47 fixes the hidden ceiling)", function() {
        this.timeout(10000);
        return bootstrap(DELAY_FLOW).then(function(h) {
            handle = h;
            // DELAY_FLOW's delay1 node waits 200ms before calling done() -
            // configuring Capture's timeout at 2000ms (comfortably above
            // 200ms, and would have been fine even under the OLD 5000ms
            // default) proves ordinary Delay usage is unaffected.
            capture = new Capture({ timeoutMs: 2000 });
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return executeNode({ flowVersion: h.flowVersion, nodeId: "delay1", msg: { payload: 1, _msgid: "delay-ok-1" } });
        }).then(function(result) {
            should(result.error).be.undefined();
            result.sends.length.should.equal(1);
            result.sends[0].destinationId.should.equal("n2");
        });
    });

    it("a real Delay node exceeding a deliberately SMALLER configured timeout fails clearly with NODE_TIMEOUT, not a silent hang or a different error", function() {
        this.timeout(10000);
        return bootstrap(DELAY_FLOW).then(function(h) {
            handle = h;
            // delay1 waits 200ms; configuring Capture's timeout at 100ms
            // (well below the node's own delay) proves the configured
            // ceiling is genuinely enforced, generically, with no
            // Delay-specific logic anywhere in Capture/the runner.
            capture = new Capture({ timeoutMs: 100 });
            capture.install(redUtil);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return executeNode({ flowVersion: h.flowVersion, nodeId: "delay1", msg: { payload: 1, _msgid: "delay-timeout-1" } });
        }).then(function(result) {
            should(result.error).be.ok();
            result.error.code.should.equal("NODE_TIMEOUT");
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #58 M1: resultNodeId explicit result marker", function() {
    it("no resultNodeId: return shape unchanged, no resultMsg key at all", function() {
        var graph = { n1: [["n2"]], n2: [[]] };
        var executeNode = function(input) {
            return Promise.resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: { payload: 2 } }] : [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: { payload: 1 } })
            .then(function(result) {
                result.should.eql({ flowVersion: "v1", lastNode: "n2" });
                result.should.not.have.property("resultMsg");
            });
    });

    it("linear flow reaches the marker once: returns its delivered msg as resultMsg", function() {
        var graph = { n1: [["n2"]], n2: [[]] };
        var executeNode = function(input) {
            return Promise.resolve({ sends: input.nodeId === "n1" ? [{ port: 0, msg: { payload: 42 } }] : [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: { payload: 1 }, resultNodeId: "n2" })
            .then(function(result) {
                result.resultMsg.should.eql({ payload: 42 });
                result.lastNode.should.equal("n2");
            });
    });

    it("other branches may exist; the configured marker still wins regardless of traversal order", function() {
        var graph = { n1: [["n2", "n3"]], n2: [[]], n3: [[]] };
        var executeNode = function(input) {
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: { from: "n1" } }] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, resultNodeId: "n3" })
            .then(function(result) {
                result.resultMsg.should.eql({ from: "n1" });
            });
    });

    it("marker never reached: throws nonRetryable FLOW_RESULT_MISSING", function() {
        var graph = { n1: [["n2"]], n2: [[]] };
        var executeNode = function() {
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, resultNodeId: "never-reached" })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.type.should.equal("FLOW_RESULT_MISSING");
                err.nonRetryable.should.equal(true);
            });
    });

    it("marker receives two messages across DIFFERENT waves: throws nonRetryable FLOW_RESULT_AMBIGUOUS", function() {
        // n1 and n2 both independently reach marker, but in two separate
        // waves (n1 -> marker in wave 2, then marker -> n2 -> marker in
        // wave 4) - exercises the cross-wave running-total accumulation,
        // not just the same-wave pre-scan.
        var graph = {
            n1: [["marker"]],
            marker: [["n2"]],
            n2: [["marker"]]
        };
        var visits = { marker: 0 };
        var executeNode = function(input) {
            if (input.nodeId === "marker") {
                visits.marker++;
                // Only the FIRST visit fans out to n2, so marker is reached
                // exactly twice total (no infinite loop).
                return Promise.resolve({ sends: visits.marker === 1 ? [{ port: 0, msg: { visit: 1 } }] : [] });
            }
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: { seq: 1 } }] });
            }
            if (input.nodeId === "n2") {
                return Promise.resolve({ sends: [{ port: 0, msg: { seq: 2 } }] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, resultNodeId: "marker" })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.type.should.equal("FLOW_RESULT_AMBIGUOUS");
                err.nonRetryable.should.equal(true);
            });
    });

    it("fan-out wave with two marker deliveries in the SAME wave is rejected deterministically before dispatch", function() {
        var graph = { n1: [["marker", "marker2"]], marker: [[]], marker2: [[]] };
        var dispatched = [];
        var executeNode = function(input) {
            dispatched.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [
                    { port: 0, destinationId: "marker", msg: { a: 1 } },
                    { port: 0, destinationId: "marker", msg: { a: 2 } }
                ] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, resultNodeId: "marker" })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.type.should.equal("FLOW_RESULT_AMBIGUOUS");
                err.nonRetryable.should.equal(true);
                // Only n1 (wave 1) was dispatched - the wave containing both
                // marker deliveries was rejected BEFORE either was dispatched.
                dispatched.should.eql(["n1"]);
            });
    });

    it("Workflow continues normal routing after the one marker delivery (downstream sends from the marker node still process)", function() {
        var graph = { n1: [["marker"]], marker: [["n3"]], n3: [[]] };
        var calls = [];
        var executeNode = function(input) {
            calls.push(input.nodeId);
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, msg: { payload: 1 } }] });
            }
            if (input.nodeId === "marker") {
                return Promise.resolve({ sends: [{ port: 0, msg: { payload: 2 } }] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, resultNodeId: "marker" })
            .then(function(result) {
                calls.should.eql(["n1", "marker", "n3"]);
                result.resultMsg.should.eql({ payload: 1 });
                result.lastNode.should.equal("n3");
            });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #75: httpBridge response aggregation", function() {
    it("httpBridge not set: httpResponse is never even considered, even if an Activity result carries one", function() {
        var graph = { n1: [[]] };
        var executeNode = function() {
            return Promise.resolve({ sends: [], httpResponse: { statusCode: 200, headers: {}, body: "x" } });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {} })
            .then(function(result) {
                result.should.not.have.property("httpResponse");
            });
    });

    it("httpBridge set, zero HTTP Response deliveries: throws nonRetryable HTTP_RESPONSE_MISSING", function() {
        var graph = { n1: [["n2"]], n2: [[]] };
        var executeNode = function() {
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridge: true })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.type.should.equal("HTTP_RESPONSE_MISSING");
                err.nonRetryable.should.equal(true);
            });
    });

    it("httpBridge set, exactly one HTTP Response delivery: resolves with output.httpResponse", function() {
        var graph = { n1: [["resp"]], resp: [[]] };
        var executeNode = function(input) {
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "resp", msg: {} }] });
            }
            if (input.nodeId === "resp") {
                return Promise.resolve({ sends: [], httpResponse: { statusCode: 201, headers: { "x-test": "1" }, body: { ok: true } } });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridge: true })
            .then(function(result) {
                result.httpResponse.should.eql({ statusCode: 201, headers: { "x-test": "1" }, body: { ok: true } });
            });
    });

    it("httpBridge set, two HTTP Response deliveries (conditional branches both fire): throws nonRetryable HTTP_RESPONSE_AMBIGUOUS", function() {
        var graph = { n1: [["respA", "respB"]], respA: [[]], respB: [[]] };
        var executeNode = function(input) {
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [
                    { port: 0, destinationId: "respA", msg: {} },
                    { port: 0, destinationId: "respB", msg: {} }
                ] });
            }
            return Promise.resolve({ sends: [], httpResponse: { statusCode: 200, headers: {}, body: input.nodeId } });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridge: true })
            .then(function() {
                throw new Error("expected runFlow to throw");
            }, function(err) {
                err.type.should.equal("HTTP_RESPONSE_AMBIGUOUS");
                err.nonRetryable.should.equal(true);
            });
    });

    it("httpBridge set: whichever HTTP Response node actually executes wins, no static resultNodeId required", function() {
        var graph = { n1: [["respB"]], respB: [[]] };
        var executeNode = function(input) {
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "respB", msg: {} }] });
            }
            return Promise.resolve({ sends: [], httpResponse: { statusCode: 404, headers: {}, body: "not found" } });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridge: true })
            .then(function(result) {
                result.httpResponse.body.should.equal("not found");
            });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #82: httpBridgeId propagation", function() {
    it("threads httpBridgeId through to every httpBridge executeNode Activity call, replay-deterministically", function() {
        var graph = { n1: [["resp"]], resp: [[]] };
        var seenBridgeIds = [];
        var executeNode = function(input) {
            seenBridgeIds.push({ nodeId: input.nodeId, httpBridge: input.httpBridge, httpBridgeId: input.httpBridgeId });
            if (input.nodeId === "n1") {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "resp", msg: {} }] });
            }
            return Promise.resolve({ sends: [], httpResponse: { statusCode: 200, headers: {}, body: "ok" } });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridge: true, httpBridgeId: "bridge-xyz" })
            .then(function(result) {
                result.httpResponse.body.should.equal("ok");
                seenBridgeIds.length.should.equal(2);
                seenBridgeIds.forEach(function(seen) {
                    seen.httpBridge.should.equal(true);
                    seen.httpBridgeId.should.equal("bridge-xyz");
                });
            });
    });

    it("does not add httpBridgeId to Activity input when httpBridge is set but no httpBridgeId was supplied", function() {
        var graph = { n1: [[]] };
        var executeNode = function(input) {
            input.should.not.have.property("httpBridgeId");
            return Promise.resolve({ sends: [], httpResponse: { statusCode: 200, headers: {}, body: "ok" } });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridge: true });
    });

    it("non-httpBridge runs never carry httpBridgeId on Activity input, even if one is supplied", function() {
        var graph = { n1: [[]] };
        var executeNode = function(input) {
            input.should.not.have.property("httpBridge");
            input.should.not.have.property("httpBridgeId");
            return Promise.resolve({ sends: [] });
        };
        return runFlow({ executeNode: executeNode, graph: graph, flowVersion: "v1", startNode: "n1", startMsg: {}, httpBridgeId: "should-be-ignored" });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #88: heartbeatTimeout on every executeNode Activity", function() {
    var WORKFLOWS_MODULE_PATH = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/workflows.js");
    var TEMPORAL_WORKFLOW_PATH = require.resolve("@temporalio/workflow");

    // Same "stub the SDK's own entry point via require.cache" convention as
    // worker_spec.js uses for @temporalio/client: `proxyActivities()` itself
    // asserts a live Workflow sandbox context and throws otherwise, so the
    // ONLY way to unit-test the ActivityOptions it is actually called with
    // (without a real Workflow execution) is to stub the SDK module it
    // comes from, then re-require workflows.js fresh so it picks up the
    // stub instead of the real `proxyActivities`.
    function withStubbedProxyActivities(fn) {
        var originalWorkflowModule = require.cache[TEMPORAL_WORKFLOW_PATH];
        var originalWorkflowsModule = require.cache[WORKFLOWS_MODULE_PATH];
        var seenOptions = [];
        require.cache[TEMPORAL_WORKFLOW_PATH] = {
            id: TEMPORAL_WORKFLOW_PATH,
            filename: TEMPORAL_WORKFLOW_PATH,
            loaded: true,
            exports: Object.assign({}, require(TEMPORAL_WORKFLOW_PATH), {
                proxyActivities: function(options) {
                    seenOptions.push(options);
                    return { executeNode: function() { return Promise.resolve({ sends: [] }); } };
                }
            })
        };
        delete require.cache[WORKFLOWS_MODULE_PATH];
        var stubbedWorkflows = require(WORKFLOWS_MODULE_PATH);
        try {
            fn(stubbedWorkflows, seenOptions);
        } finally {
            delete require.cache[WORKFLOWS_MODULE_PATH];
            if (originalWorkflowModule) {
                require.cache[TEMPORAL_WORKFLOW_PATH] = originalWorkflowModule;
            } else {
                delete require.cache[TEMPORAL_WORKFLOW_PATH];
            }
            if (originalWorkflowsModule) {
                require.cache[WORKFLOWS_MODULE_PATH] = originalWorkflowsModule;
            }
        }
    }

    it("every per-node executeNode Activity proxy is built with a heartbeatTimeout", function() {
        withStubbedProxyActivities(function(stubbedWorkflows, seenOptions) {
            stubbedWorkflows.createExecuteNode("n1", 1, {}, undefined, undefined);
            seenOptions.length.should.equal(1);
            seenOptions[0].should.have.property("heartbeatTimeout");
            seenOptions[0].heartbeatTimeout.should.be.a.Number();
            seenOptions[0].heartbeatTimeout.should.be.above(0);
        });
    });

    it("heartbeatTimeout stays strictly below startToCloseTimeout, so a healthy Activity has room to heartbeat repeatedly before either timeout", function() {
        withStubbedProxyActivities(function(stubbedWorkflows, seenOptions) {
            stubbedWorkflows.createExecuteNode("n1", 1, {}, undefined, 1800000);
            seenOptions[0].heartbeatTimeout.should.be.below(seenOptions[0].startToCloseTimeout);
        });
    });

    it("does not change existing ActivityOptions (activityId, summary, retry, startToCloseTimeout) when adding heartbeatTimeout", function() {
        withStubbedProxyActivities(function(stubbedWorkflows, seenOptions) {
            stubbedWorkflows.createExecuteNode("n1", 1, { n1: { type: "delay", name: "wait" } }, "custom-q", 1800000);
            var options = seenOptions[0];
            options.activityId.should.equal("node:n1:1");
            options.summary.should.equal("delay — wait");
            options.taskQueue.should.equal("custom-q");
            options.startToCloseTimeout.should.equal(1805000);
            options.retry.should.eql({ maximumAttempts: 3, initialInterval: "1s", backoffCoefficient: 2 });
        });
    });
});
describe("@tbrandenburg/node-red-temporal-runtime/lib/workflows - issue #89: suspension/resume ABI", function() {
    function deferred() {
        var resolve;
        var promise = new Promise(function(r) { resolve = r; });
        return { promise: promise, resolve: resolve };
    }

    it("a suspension result routes to a resume Activity call carrying {nodeId, msg, resume:{type, continuation}}, and its sends continue routing normally", function() {
        var timerGate = deferred();
        var calls = [];
        var executeNode = function(input) {
            calls.push(input);
            if (input.nodeId === "n1" && !input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "timer", durationMs: 60000, continuation: { step: 1 } } });
            }
            if (input.nodeId === "n1" && input.resume) {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "n2", msg: { payload: "resumed" } }] });
            }
            return Promise.resolve({ sends: [] });
        };
        var sleepFn = function(ms) { timerGate.ms = ms; return timerGate.promise; };
        var runP = runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: { payload: 0 },
            sleepFn: sleepFn,
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        });
        timerGate.resolve();
        return runP.then(function(output) {
            output.lastNode.should.equal("n2");
            calls.length.should.equal(3);
            calls[0].should.eql({ flowVersion: "v1", nodeId: "n1", msg: { payload: 0 } });
            calls[1].nodeId.should.equal("n1");
            calls[1].msg.should.eql({ payload: 0 });
            calls[1].resume.should.eql({ type: "timer", continuation: { step: 1 } });
            calls[2].nodeId.should.equal("n2");
            timerGate.ms.should.equal(60000);
        });
    });

    it("a signal suspension whose key is already in the inbox BEFORE the wait resolves immediately, without ever calling conditionFn (signal-before-wait)", function() {
        var conditionCalls = 0;
        var inbox = new Map();
        inbox.set("approval:1", { approved: true });
        var executeNode = function(input) {
            if (input.nodeId === "n1" && !input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "signal", key: "approval:1", continuation: { c: 1 } } });
            }
            if (input.nodeId === "n1" && input.resume) {
                return Promise.resolve({ sends: [] });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: { payload: 0 },
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { conditionCalls += 1; return Promise.resolve(); },
            signalInbox: inbox
        }).then(function(output) {
            output.lastNode.should.equal("n1");
            conditionCalls.should.equal(0);
        });
    });

    it("a signal suspension whose key arrives AFTER the wait has started still wakes it (signal-after-wait), and the resume Activity receives the delivered signal payload", function() {
        var inbox = new Map();
        var conditionGate = deferred();
        var resumeInputs = [];
        var executeNode = function(input) {
            if (input.nodeId === "n1" && !input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "signal", key: "approval:2", continuation: { c: 2 } } });
            }
            if (input.nodeId === "n1" && input.resume) {
                resumeInputs.push(input);
                return Promise.resolve({ sends: [] });
            }
            return Promise.resolve({ sends: [] });
        };
        var runP = runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: { payload: 0 },
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { return conditionGate.promise; },
            signalInbox: inbox
        });
        // simulate the signal arriving after the wait already started
        inbox.set("approval:2", { approvedBy: "bob" });
        conditionGate.resolve();
        return runP.then(function() {
            resumeInputs.length.should.equal(1);
            resumeInputs[0].resume.should.eql({ type: "signal", continuation: { c: 2 }, signal: { approvedBy: "bob" } });
        });
    });

    it("unrelated signal keys do not wake an unrelated suspension", function() {
        var inbox = new Map();
        var conditionCheck;
        var executeNode = function(input) {
            if (input.nodeId === "n1" && !input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "signal", key: "approval:mine", continuation: {} } });
            }
            return Promise.resolve({ sends: [] });
        };
        inbox.set("approval:other", { irrelevant: true });
        var settled = false;
        var runP = runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: {},
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function(predicate) {
                conditionCheck = predicate;
                predicate().should.equal(false); // unrelated key present, own key absent
                return new Promise(function() {}); // never resolves in this test
            },
            signalInbox: inbox
        });
        runP.then(function() { settled = true; });
        return new Promise(function(resolve) { setTimeout(resolve, 10); }).then(function() {
            settled.should.equal(false);
            should.exist(conditionCheck);
        });
    });

    it("duplicate signal delivery for the same key is deterministic: the LAST delivered value wins (Map.set overwrite)", function() {
        var inbox = new Map();
        inbox.set("k", "first");
        inbox.set("k", "second");
        inbox.get("k").should.equal("second");
    });

    it("throws a nonRetryable SUSPENSION_INVALID ApplicationFailure for an unknown suspension type, never silently executing locally", function() {
        var executeNode = function(input) {
            if (!input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "approval" } });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: {},
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        }).then(function() {
            should.fail("expected runFlow to throw");
        }, function(err) {
            err.type.should.equal("SUSPENSION_INVALID");
            err.nonRetryable.should.equal(true);
        });
    });

    it("throws a nonRetryable SUSPENSION_INVALID ApplicationFailure when a suspension is combined with non-empty sends", function() {
        var executeNode = function(input) {
            if (!input.resume) {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "n2", msg: {} }], suspension: { type: "timer", durationMs: 1000 } });
            }
            return Promise.resolve({ sends: [] });
        };
        return runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: {},
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        }).then(function() {
            should.fail("expected runFlow to throw");
        }, function(err) {
            err.type.should.equal("SUSPENSION_INVALID");
            err.nonRetryable.should.equal(true);
        });
    });

    it("propagates a resume Activity's own NODE_ERROR as a nonRetryable ApplicationFailure, same as any other node error", function() {
        var executeNode = function(input) {
            if (!input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "timer", durationMs: 1000 } });
            }
            return Promise.resolve({ sends: [], error: { code: "NODE_ERROR", nodeId: "n1", message: "resume failed" } });
        };
        return runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: {},
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        }).then(function() {
            should.fail("expected runFlow to throw");
        }, function(err) {
            err.type.should.equal("NODE_ERROR");
            err.nonRetryable.should.equal(true);
        });
    });

    it("does not block an unrelated sibling branch: the immediate branch completes while the suspended branch is still waiting, then the suspended branch resumes once released", function() {
        var timerGate = deferred();
        var order = [];
        var executeNode = function(input) {
            if (input.nodeId === "suspend" && !input.resume) {
                order.push("suspend:plan");
                return Promise.resolve({ sends: [], suspension: { type: "timer", durationMs: 172800000, continuation: {} } });
            }
            if (input.nodeId === "suspend" && input.resume) {
                order.push("suspend:resume");
                return Promise.resolve({ sends: [] });
            }
            if (input.nodeId === "b") {
                order.push("b");
                return Promise.resolve({ sends: [{ port: 0, destinationId: "c", msg: {} }] });
            }
            if (input.nodeId === "c") {
                order.push("c");
                return Promise.resolve({ sends: [] });
            }
            return Promise.resolve({ sends: [] });
        };
        var runP = runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            initial: [{ nodeId: "b", msg: {} }, { nodeId: "suspend", msg: {} }],
            sleepFn: function() { return timerGate.promise; },
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        });
        var immediateSettled = false;
        return new Promise(function(resolve) { setTimeout(resolve, 20); }).then(function() {
            // by now, the immediate branch (b -> c) must have fully drained
            // even though the suspended branch's timer is still pending -
            // proving a suspended branch never blocks sibling ready work.
            order.should.containEql("b");
            order.should.containEql("c");
            order.should.not.containEql("suspend:resume");
            timerGate.resolve();
            return runP;
        }).then(function(output) {
            order.should.containEql("suspend:resume");
            order.indexOf("c").should.be.below(order.indexOf("suspend:resume"));
        });
    });

    it("counts a suspend+resume pair as exactly ONE totalExecutions/logical invocation, not two, against maxNodeExecutions", function() {
        var executeNode = function(input) {
            if (input.nodeId === "n1" && !input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "timer", durationMs: 1000 } });
            }
            if (input.nodeId === "n1" && input.resume) {
                return Promise.resolve({ sends: [{ port: 0, destinationId: "n2", msg: {} }] });
            }
            return Promise.resolve({ sends: [] });
        };
        // maxNodeExecutions = 2 must be enough for n1 (suspend+resume, ONE
        // count) followed by n2 (one count) - if the resume were double
        // counted this would throw FLOW_EXECUTION_LIMIT.
        return runFlow({
            executeNode: executeNode,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: {},
            maxNodeExecutions: 2,
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        }).then(function(output) {
            output.lastNode.should.equal("n2");
        });
    });

    it("uses a deterministic, distinct resume Activity id suffixed :resume, sharing the SAME invocation number as the original suspending call (createExecuteNode factory path)", function() {
        var createdOptions = [];
        var fakeExecuteNode = function() { return function(input) {
            if (!input.resume) {
                return Promise.resolve({ sends: [], suspension: { type: "timer", durationMs: 1000 } });
            }
            return Promise.resolve({ sends: [] });
        }; };
        var createExecuteNodeFactory = function(nodeId, invocation, nodeMeta, taskQueue, nodeExecutionTimeoutMs, isResume) {
            createdOptions.push({ nodeId: nodeId, invocation: invocation, isResume: !!isResume });
            return fakeExecuteNode();
        };
        return runFlow({
            createExecuteNode: createExecuteNodeFactory,
            graph: {},
            flowVersion: "v1",
            startNode: "n1",
            startMsg: {},
            sleepFn: function() { return Promise.resolve(); },
            conditionFn: function() { return Promise.resolve(); },
            signalInbox: new Map()
        }).then(function() {
            createdOptions.should.eql([
                { nodeId: "n1", invocation: 1, isResume: false },
                { nodeId: "n1", invocation: 1, isResume: true }
            ]);
        });
    });

    describe("issue #91 M6a: consume-once signal delivery policy", function() {
        it("a later suspension reusing the same key does NOT resume from an already-consumed signal - it blocks until a fresh signal arrives", function() {
            var inbox = new Map();
            inbox.set("approval:1", { round: 1 });
            var resumeInputs = [];
            var secondGate = deferred();
            var executeNode = function(input) {
                if (input.nodeId === "first" && !input.resume) {
                    return Promise.resolve({ sends: [], suspension: { type: "signal", key: "approval:1", continuation: {} } });
                }
                if (input.nodeId === "first" && input.resume) {
                    resumeInputs.push(input);
                    return Promise.resolve({ sends: [{ port: 0, destinationId: "second", msg: {} }] });
                }
                if (input.nodeId === "second" && !input.resume) {
                    return Promise.resolve({ sends: [], suspension: { type: "signal", key: "approval:1", continuation: {} } });
                }
                if (input.nodeId === "second" && input.resume) {
                    resumeInputs.push(input);
                    return Promise.resolve({ sends: [] });
                }
                return Promise.resolve({ sends: [] });
            };
            var runP = runFlow({
                executeNode: executeNode,
                graph: {},
                flowVersion: "v1",
                startNode: "first",
                startMsg: {},
                sleepFn: function() { return Promise.resolve(); },
                conditionFn: function(predicate) {
                    // the second wait for the SAME key must re-register a
                    // condition (the first consumed the only inbox entry) -
                    // simulate the fresh signal arriving only once we get
                    // here for a second time.
                    if (!predicate()) {
                        return secondGate.promise;
                    }
                    return Promise.resolve();
                },
                signalInbox: inbox
            });
            return new Promise(function(resolve) { setTimeout(resolve, 10); }).then(function() {
                resumeInputs.length.should.equal(1);
                resumeInputs[0].resume.signal.should.eql({ round: 1 });
                inbox.has("approval:1").should.equal(false);
                // deliver a genuinely NEW signal for the reused key
                inbox.set("approval:1", { round: 2 });
                secondGate.resolve();
                return runP;
            }).then(function() {
                resumeInputs.length.should.equal(2);
                resumeInputs[1].resume.signal.should.eql({ round: 2 });
            });
        });

        it("two concurrent waits sharing the same key: only ONE consumes a given signal delivery (first-registered-wins, no broadcast/double-consume)", function() {
            var inbox = new Map();
            var resumeInputs = [];
            var pending = [];
            function conditionFn(predicate) {
                if (predicate()) {
                    return Promise.resolve();
                }
                return new Promise(function(resolve) {
                    pending.push({ predicate: predicate, resolve: resolve });
                });
            }
            function pump() {
                pending = pending.filter(function(entry) {
                    if (entry.predicate()) {
                        entry.resolve();
                        return false;
                    }
                    return true;
                });
            }
            var executeNode = function(input) {
                if ((input.nodeId === "a" || input.nodeId === "d") && !input.resume) {
                    return Promise.resolve({ sends: [], suspension: { type: "signal", key: "shared", continuation: {} } });
                }
                if (input.resume) {
                    resumeInputs.push(input);
                    return Promise.resolve({ sends: [] });
                }
                return Promise.resolve({ sends: [] });
            };
            var runP = runFlow({
                executeNode: executeNode,
                graph: {},
                flowVersion: "v1",
                initial: [{ nodeId: "a", msg: {} }, { nodeId: "d", msg: {} }],
                sleepFn: function() { return Promise.resolve(); },
                conditionFn: conditionFn,
                signalInbox: inbox
            });
            var settled = false;
            runP.then(function() { settled = true; });
            return new Promise(function(resolve) { setTimeout(resolve, 10); }).then(function() {
                // only one signal delivered for the shared key
                inbox.set("shared", { once: true });
                pump();
                return new Promise(function(resolve) { setTimeout(resolve, 10); });
            }).then(function() {
                // exactly one of the two waiters consumed it; the other
                // remains blocked (re-registered a fresh condition()) rather
                // than both resuming off the one delivery.
                resumeInputs.length.should.equal(1);
                settled.should.equal(false);
                inbox.has("shared").should.equal(false);
                pending.length.should.equal(1);
            });
        });
    });

    describe("issue #91 M6b: reject contradictory Activity outcomes", function() {
        it("throws a nonRetryable SUSPENSION_WITH_ERROR ApplicationFailure when an Activity result carries BOTH error and suspension, instead of silently suspending", function() {
            var executeNode = function(input) {
                if (!input.resume) {
                    return Promise.resolve({
                        sends: [],
                        error: { code: "NODE_ERROR", nodeId: "n1", message: "boom" },
                        suspension: { type: "signal", key: "k", continuation: {} }
                    });
                }
                return Promise.resolve({ sends: [] });
            };
            return runFlow({
                executeNode: executeNode,
                graph: {},
                flowVersion: "v1",
                startNode: "n1",
                startMsg: {},
                sleepFn: function() { return Promise.resolve(); },
                conditionFn: function() { return Promise.resolve(); },
                signalInbox: new Map()
            }).then(function() {
                should.fail("expected runFlow to throw");
            }, function(err) {
                err.type.should.equal("SUSPENSION_WITH_ERROR");
                err.nonRetryable.should.equal(true);
            });
        });
    });
});
