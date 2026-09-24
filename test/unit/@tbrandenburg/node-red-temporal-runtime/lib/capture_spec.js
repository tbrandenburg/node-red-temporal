var should = require("should");
var helper = require("node-red-node-test-helper");
var RED = require("nr-test-utils").require("node-red/lib/red");
var { Capture, DEFAULT_TIMEOUT_MS } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");

// A single configurable "probe" node used to drive every scenario in the
// spec below via msg.mode. Real Node-RED node registered through the real
// runtime/registry - nothing here is mocked.
function probeNodeModule(RED) {
    function ProbeNode(config) {
        RED.nodes.createNode(this, config);
        this.on("input", function(msg, send, done) {
            switch (msg.mode) {
                case "zero":
                    done();
                    break;
                case "multi":
                    send({ payload: 1, _msgid: msg._msgid });
                    send({ payload: 2, _msgid: msg._msgid });
                    send({ payload: 3, _msgid: msg._msgid });
                    done();
                    break;
                case "ports":
                    send([
                        { payload: "a", _msgid: msg._msgid },
                        { payload: "b", _msgid: msg._msgid }
                    ]);
                    done();
                    break;
                case "doneErr":
                    send({ payload: "sent-before-error", _msgid: msg._msgid });
                    done(new Error("boom"));
                    break;
                case "throw":
                    throw new Error("sync-throw");
                case "hang":
                    break;
                case "forward":
                    send(msg);
                    done();
                    break;
                case "doubleSend":
                    // Two SEPARATE node.send() calls, same _msgid - the
                    // exact scenario issue #12 finding 3 targets.
                    send({ payload: "first", _msgid: msg._msgid });
                    send({ payload: "second", _msgid: msg._msgid });
                    done();
                    break;
                case "delayedForward":
                    setTimeout(function() {
                        send({ payload: msg.payload, _msgid: msg._msgid });
                        done();
                    }, msg.delayMs || 10);
                    break;
                default:
                    done();
            }
        });
    }
    RED.nodes.registerType("temporal-probe", ProbeNode);
}

// issue #53: a second, test-only probe registered against the REAL
// pre-1.0 Node-RED input-handler API - 1 declared parameter, no `send`/
// `done` arguments - to prove Capture's legacy-node behavior against a
// real runtime rather than a mock. `node.send`/`node.error` (inherited,
// unmodified upstream Node.prototype methods) are used directly, exactly
// as real legacy contrib nodes do.
function legacyProbeNodeModule(RED) {
    function LegacyProbeNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        this.on("input", function(msg) {
            switch (msg.mode) {
                case "legacySyncSend":
                    node.send({ payload: "legacy-sync", _msgid: msg._msgid });
                    break;
                case "legacyAsyncSend":
                    setTimeout(function() {
                        node.send({ payload: "legacy-async", _msgid: msg._msgid });
                    }, msg.delayMs || 20);
                    break;
                default:
                    // legacySyncNoSend: do nothing at all.
                    break;
            }
        });
    }
    RED.nodes.registerType("temporal-legacy-probe", LegacyProbeNode);
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/capture", function() {

    beforeEach(function(done) {
        helper.startServer(done);
    });

    afterEach(function(done) {
        helper.unload().then(function() {
            helper.stopServer(done);
        });
    });

    function loadFlow(flow) {
        return new Promise((resolve) => helper.load(probeNodeModule, flow, resolve));
    }

    function loadLegacyFlow(flow) {
        return new Promise((resolve) => helper.load([probeNodeModule, legacyProbeNodeModule], flow, resolve));
    }

    it("Q1: resolves with sends: [] when the node sends nothing and calls done()", function() {
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "zero" };
            return capture.around(p1, msg, function() { p1.receive(msg); });
        }).then(function(result) {
            result.sends.should.eql([]);
            capture.uninstall();
        });
    });

    it("Q2: captures all N sends on one port, in order", function() {
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "multi" };
            return capture.around(p1, msg, function() { p1.receive(msg); });
        }).then(function(result) {
            result.sends.length.should.equal(3);
            result.sends.map((s) => s.msg.payload).should.eql([1, 2, 3]);
            result.sends.every((s) => s.port === 0).should.be.true();
            capture.uninstall();
        });
    });

    it("Q3: records the correct port per send across multiple wires", function() {
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"], ["s2"]] },
            { id: "s1", type: "helper" },
            { id: "s2", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "ports" };
            return capture.around(p1, msg, function() { p1.receive(msg); });
        }).then(function(result) {
            result.sends.length.should.equal(2);
            result.sends[0].port.should.equal(0);
            result.sends[0].msg.payload.should.equal("a");
            result.sends[1].port.should.equal(1);
            result.sends[1].msg.payload.should.equal("b");
            capture.uninstall();
        });
    });

    it("Q4: done(err) after sending rejects with {nodeId, error, sends} - not silently dropped", function() {
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "doneErr" };
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected rejection"); }, function(rejection) {
                    rejection.nodeId.should.equal("p1");
                    rejection.error.message.should.equal("boom");
                    rejection.sends.length.should.equal(1);
                    rejection.sends[0].msg.payload.should.equal("sent-before-error");
                });
        }).then(function() { capture.uninstall(); });
    });

    it("Q5: a synchronous throw in the input handler is captured as a rejection, not lost", function() {
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "throw" };
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected rejection"); }, function(rejection) {
                    rejection.nodeId.should.equal("p1");
                    rejection.error.message.should.equal("sync-throw");
                });
        }).then(function() { capture.uninstall(); });
    });

    it("Q6: a node that never calls done() times out instead of hanging forever", function() {
        var capture = new Capture({ timeoutMs: 100 });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "hang" };
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected timeout rejection"); }, function(rejection) {
                    rejection.timeout.should.be.true();
                    rejection.nodeId.should.equal("p1");
                });
        }).then(function() { capture.uninstall(); });
    });

    it("Q7: two concurrent in-flight invocations sharing the same _msgid do not cross-contaminate", function() {
        // p1 forwards msg (same _msgid) to p2. We concurrently await p1's own
        // invocation AND p2's downstream invocation of the *same* msgid. If the
        // correlation key were _msgid alone, these two pending entries would
        // collide.
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["p2"]] },
            { id: "p2", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var p2 = helper.getNode("p2");
            var sharedMsgid = "shared-msgid-1";
            var p1Msg = { mode: "forward", _msgid: sharedMsgid };
            var p2Msg = { mode: "zero", _msgid: sharedMsgid };
            var p1Result = capture.around(p1, p1Msg, function() { p1.receive(p1Msg); });
            var p2Result = capture.around(p2, p2Msg, function() { p2.receive(p2Msg); });
            return Promise.all([p1Result, p2Result]);
        }).then(function(results) {
            results[0].sends.length.should.equal(1);
            results[0].sends[0].destinationId.should.equal("p2");
            results[1].sends.should.eql([]);
            capture.uninstall();
        });
    });

    it("Q8: done(false) suppression means postDeliver never fires for that send", function() {
        var capture = new Capture();
        var postDeliverCalls = 0;
        RED.hooks.add("postDeliver.temporal-test", function() { postDeliverCalls++; });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "multi" };
            return capture.around(p1, msg, function() { p1.receive(msg); });
        }).then(function() {
            postDeliverCalls.should.equal(0);
            capture.uninstall();
            RED.hooks.remove("postDeliver.temporal-test");
        });
    });

    it("uninstall() removes the hooks: after uninstall, sends actually route locally again", function() {
        var capture = new Capture();
        capture.install(RED);
        capture.uninstall();
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            return new Promise(function(resolve) {
                var s1 = helper.getNode("s1");
                var p1 = helper.getNode("p1");
                s1.on("input", function(msg) {
                    msg.payload.should.equal(1);
                    resolve();
                });
                p1.receive({ mode: "multi" });
            });
        });
    });

    it("M2-1: autonomous single-destination send invokes onIngress exactly once, local delivery suppressed", function() {
        var ingressCalls = [];
        var postDeliverCalls = 0;
        var capture = new Capture({ onIngress: function(event) { ingressCalls.push(event); } });
        RED.hooks.add("postDeliver.temporal-m2-1", function() { postDeliverCalls++; });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "forward", payload: "single-dest" };
            p1.receive(msg);
            return new Promise((resolve) => setImmediate(resolve));
        }).then(function() {
            ingressCalls.length.should.equal(1);
            ingressCalls[0].sourceNodeId.should.equal("p1");
            ingressCalls[0].sends.length.should.equal(1);
            ingressCalls[0].sends[0].destinationId.should.equal("s1");
            postDeliverCalls.should.equal(0);
            capture.uninstall();
            RED.hooks.remove("postDeliver.temporal-m2-1");
        });
    });

    it("M2-2 (issue #12 finding 3): multi mode's 3 separate node.send() calls (same _msgid) are 3 DISTINCT logical emissions, not collapsed into one", function() {
        // Node-RED's onSend hook fires once per node.send() call, so three
        // synchronous send() calls inside one input handler tick - even
        // sharing the same _msgid - must produce three separate onIngress
        // groups, not one. This was the exact bug finding 3 fixed: the old
        // setImmediate-timing-based grouping collapsed them into one.
        var ingressCalls = [];
        var postDeliverCalls = 0;
        var capture = new Capture({ onIngress: function(event) { ingressCalls.push(event); } });
        RED.hooks.add("postDeliver.temporal-m2-2", function() { postDeliverCalls++; });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "multi" };
            p1.receive(msg);
            return new Promise((resolve) => setImmediate(resolve));
        }).then(function() {
            ingressCalls.length.should.equal(3);
            ingressCalls.forEach((call) => call.sourceNodeId.should.equal("p1"));
            ingressCalls.forEach((call) => call.sends.length.should.equal(1));
            ingressCalls.map((call) => call.sends[0].msg.payload).should.eql([1, 2, 3]);
            postDeliverCalls.should.equal(0);
            capture.uninstall();
            RED.hooks.remove("postDeliver.temporal-m2-2");
        });
    });

    it("M2-3: autonomous fan-out send across two wires from one output port groups into ONE onIngress event", function() {
        var ingressCalls = [];
        var capture = new Capture({ onIngress: function(event) { ingressCalls.push(event); } });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1", "s2"]] },
            { id: "s1", type: "helper" },
            { id: "s2", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "forward", payload: "fanout-2" };
            p1.receive(msg);
            return new Promise((resolve) => setImmediate(resolve));
        }).then(function() {
            var fanoutEvents = ingressCalls.filter((e) => e.sends.some((s) => s.msg.payload === "fanout-2"));
            fanoutEvents.length.should.equal(1);
            fanoutEvents[0].sends.length.should.equal(2);
            var destinationIds = fanoutEvents[0].sends.map((s) => s.destinationId).sort();
            destinationIds.should.eql(["s1", "s2"]);
            capture.uninstall();
        });
    });

    it("M2-4: two rapid but distinct autonomous emissions from the same source node do not cross-contaminate", function() {
        var ingressCalls = [];
        var capture = new Capture({ onIngress: function(event) { ingressCalls.push(event); } });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            p1.receive({ mode: "forward", _msgid: "m2-4-a", payload: "A" });
            p1.receive({ mode: "forward", _msgid: "m2-4-b", payload: "B" });
            return new Promise((resolve) => setImmediate(resolve));
        }).then(function() {
            ingressCalls.length.should.equal(2);
            var byMsgid = {};
            ingressCalls.forEach((e) => { byMsgid[e.msg._msgid] = e; });
            byMsgid["m2-4-a"].sends.length.should.equal(1);
            byMsgid["m2-4-a"].sends[0].msg.payload.should.equal("A");
            byMsgid["m2-4-b"].sends.length.should.equal(1);
            byMsgid["m2-4-b"].sends[0].msg.payload.should.equal("B");
            capture.uninstall();
        });
    });

    it("Q9 (issue #12 finding 1): two concurrent Activity invocations of the SAME node sharing the SAME _msgid do not cross-contaminate", function() {
        // A naive `nodeId::_msgid` correlation key would collide these two
        // concurrent around() calls in `_pending`, losing one invocation's
        // resolvers/sends. AsyncLocalStorage-based correlation must keep
        // them fully isolated regardless of shared _msgid.
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var sharedMsgid = "same-node-shared-msgid";
            var msgA = { mode: "delayedForward", payload: "A", delayMs: 30, _msgid: sharedMsgid };
            var msgB = { mode: "delayedForward", payload: "B", delayMs: 5, _msgid: sharedMsgid };
            var resultA = capture.around(p1, msgA, function() { p1.receive(msgA); });
            var resultB = capture.around(p1, msgB, function() { p1.receive(msgB); });
            return Promise.all([resultA, resultB]);
        }).then(function(results) {
            results[0].sends.length.should.equal(1);
            results[0].sends[0].msg.payload.should.equal("A");
            results[1].sends.length.should.equal(1);
            results[1].sends[0].msg.payload.should.equal("B");
            capture.uninstall();
        });
    });

    it("M2-6 (issue #12 finding 3): two synchronous node.send() calls with the same _msgid produce TWO distinct onIngress events", function() {
        var ingressCalls = [];
        var capture = new Capture({ onIngress: function(event) { ingressCalls.push(event); } });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "doubleSend", _msgid: "shared-double-send-msgid" };
            p1.receive(msg);
            return new Promise((resolve) => setImmediate(resolve));
        }).then(function() {
            ingressCalls.length.should.equal(2);
            ingressCalls.forEach((call) => call.sends.length.should.equal(1));
            ingressCalls.map((call) => call.sends[0].msg.payload).should.eql(["first", "second"]);
            capture.uninstall();
        });
    });

    it("M2-5: Activity-owned around() path is unchanged and never invokes onIngress", function() {
        var ingressCalls = [];
        var capture = new Capture({ onIngress: function(event) { ingressCalls.push(event); } });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "multi" };
            return capture.around(p1, msg, function() { p1.receive(msg); });
        }).then(function(result) {
            result.sends.length.should.equal(3);
            return new Promise((resolve) => setImmediate(resolve));
        }).then(function() {
            ingressCalls.length.should.equal(0);
            capture.uninstall();
        });
    });

    it("exports an overridable DEFAULT_TIMEOUT_MS", function() {
        DEFAULT_TIMEOUT_MS.should.be.a.Number();
        new Capture().timeoutMs.should.equal(DEFAULT_TIMEOUT_MS);
        new Capture({ timeoutMs: 42 }).timeoutMs.should.equal(42);
    });

    // issue #23: a hop whose DESTINATION node's `.z` equals the id of the
    // node currently being invoked (`pending.nodeId`) is "internal to the
    // subflow instance currently in flight" (exactly how Node-RED itself
    // rewrites a subflow instance's cloned internal nodes' `.z` - see
    // capture.js's `_onPreRoute` doc). `p2.z` is set to `p1.id` here to
    // simulate that relationship without needing a full subflow fixture -
    // real subflow behavior is covered end-to-end by workflows_spec.js's
    // "issue #23" test against the actual `subflow-flow.json` fixture.
    it("issue #23: a hop into a node whose z equals the currently-invoked node's id is routed locally, not suppressed/captured - while a hop to a node with a DIFFERENT z is still captured normally", function() {
        var capture = new Capture();
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["p2"]] },
            { id: "p2", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var p2 = helper.getNode("p2");
            p2.z = p1.id; // simulate p2 being an internal node of a subflow instance p1
            var msg = { mode: "forward", payload: "hi" };
            return capture.around(p1, msg, function() { p1.receive(msg); });
        }).then(function(result) {
            // p1 -> p2 was NOT captured (internal hop, routed locally by
            // Node-RED) - only p2's own real external send to s1 is.
            result.sends.length.should.equal(1);
            result.sends[0].destinationId.should.equal("s1");
            capture.uninstall();
        });
    });

    // --- issue #53: legacy (pre-1.0, non-done()) input handlers ---
    //
    // These use a REAL Node-RED runtime and a REAL pre-1.0-style `input`
    // handler (1 declared param, no `send`/`done` arguments) - not a mock -
    // to prove the discriminator (`node._expectedDoneCount`, read via
    // upstream's own unmodified `Node.js`) and the fail-fast behavior it
    // drives.

    it("issue #53 L1: a legacy node that sends synchronously and never calls done() rejects immediately with LEGACY_NODE_NO_DONE, not a timeout, and still returns its sends", function() {
        var capture = new Capture({ timeoutMs: 5000 });
        capture.install(RED);
        var start;
        return loadLegacyFlow([
            { id: "p1", type: "temporal-legacy-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "legacySyncSend" };
            start = Date.now();
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected rejection"); }, function(rejection) {
                    (Date.now() - start).should.be.below(1000); // fail-fast, not the 5s timeout
                    rejection.legacyNoDone.should.be.true();
                    rejection.nodeId.should.equal("p1");
                    rejection.error.code.should.equal("LEGACY_NODE_NO_DONE");
                    rejection.sends.length.should.equal(1);
                    rejection.sends[0].msg.payload.should.equal("legacy-sync");
                });
        }).then(function() { capture.uninstall(); });
    });

    it("issue #53 L2: a legacy node that sends nothing and never calls done() rejects immediately with LEGACY_NODE_NO_DONE and empty sends", function() {
        var capture = new Capture({ timeoutMs: 5000 });
        capture.install(RED);
        var start;
        return loadLegacyFlow([
            { id: "p1", type: "temporal-legacy-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "legacySyncNoSend" };
            start = Date.now();
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected rejection"); }, function(rejection) {
                    (Date.now() - start).should.be.below(1000);
                    rejection.legacyNoDone.should.be.true();
                    rejection.error.code.should.equal("LEGACY_NODE_NO_DONE");
                    rejection.sends.should.eql([]);
                });
        }).then(function() { capture.uninstall(); });
    });

    it("issue #53 L3: a legacy node whose send happens ASYNCHRONOUSLY (after its handler already returned) still fails fast, and the later async send is never silently attributed to a completed/reused invocation", function() {
        var capture = new Capture({ timeoutMs: 5000 });
        capture.install(RED);
        var start;
        return loadLegacyFlow([
            { id: "p1", type: "temporal-legacy-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "legacyAsyncSend", delayMs: 30 };
            start = Date.now();
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected rejection"); }, function(rejection) {
                    (Date.now() - start).should.be.below(1000); // did NOT wait for the 30ms async send, let alone the 5s timeout
                    rejection.legacyNoDone.should.be.true();
                    // The async send has not happened yet at rejection time.
                    rejection.sends.should.eql([]);
                });
        }).then(function() {
            // Let the node's delayed send actually fire; it must not throw,
            // hang, or resurrect the already-settled invocation.
            return new Promise((resolve) => setTimeout(resolve, 60));
        }).then(function() {
            capture.uninstall();
        });
    });

    it("issue #53 L4: modern done()-based nodes are completely unaffected by the postReceive discriminator (sync, delayed and zero-send cases all unchanged)", function() {
        var capture = new Capture({ timeoutMs: 2000 });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var zeroMsg = { mode: "zero" };
            var multiMsg = { mode: "multi" };
            var delayedMsg = { mode: "delayedForward", payload: "later", delayMs: 15 };
            return Promise.all([
                capture.around(p1, zeroMsg, function() { p1.receive(zeroMsg); }),
                capture.around(p1, multiMsg, function() { p1.receive(multiMsg); }),
                capture.around(p1, delayedMsg, function() { p1.receive(delayedMsg); })
            ]);
        }).then(function(results) {
            results[0].sends.should.eql([]);
            results[1].sends.length.should.equal(3);
            results[2].sends.length.should.equal(1);
            results[2].sends[0].msg.payload.should.equal("later");
            capture.uninstall();
        });
    });

    it("issue #53 L5: a genuine modern node that never calls done() still times out with the ordinary NODE_TIMEOUT rejection, not LEGACY_NODE_NO_DONE", function() {
        var capture = new Capture({ timeoutMs: 100 });
        capture.install(RED);
        return loadFlow([
            { id: "p1", type: "temporal-probe", wires: [["s1"]] },
            { id: "s1", type: "helper" }
        ]).then(function() {
            var p1 = helper.getNode("p1");
            var msg = { mode: "hang" }; // 3-arg handler, registered with done - just never calls it
            return capture.around(p1, msg, function() { p1.receive(msg); })
                .then(function() { throw new Error("expected timeout rejection"); }, function(rejection) {
                    rejection.timeout.should.be.true();
                    should(rejection.legacyNoDone).not.be.ok();
                    rejection.nodeId.should.equal("p1");
                });
        }).then(function() { capture.uninstall(); });
    });

    describe("issue #90: routeSend() - generic route-only helper, no node.receive()/onComplete involved", function() {
        it("captures a plain node.send() call's resolved destinationId without ever calling receive()", function() {
            var capture = new Capture();
            capture.install(RED);
            return loadFlow([
                { id: "p1", type: "temporal-probe", wires: [["s1"]] },
                { id: "s1", type: "helper" }
            ]).then(function() {
                var p1 = helper.getNode("p1");
                var msg = { payload: "resumed", _msgid: "r1" };
                var result = capture.routeSend(p1, msg, function() { p1.send(msg); });
                result.sends.length.should.equal(1);
                result.sends[0].destinationId.should.equal("s1");
                result.sends[0].msg.payload.should.equal("resumed");
                capture.uninstall();
            });
        });

        it("returns sends: [] when fn() sends nothing", function() {
            var capture = new Capture();
            capture.install(RED);
            return loadFlow([
                { id: "p1", type: "temporal-probe", wires: [["s1"]] },
                { id: "s1", type: "helper" }
            ]).then(function() {
                var p1 = helper.getNode("p1");
                var result = capture.routeSend(p1, { payload: 1 }, function() {});
                result.sends.should.eql([]);
                capture.uninstall();
            });
        });

        it("does not require or wait for a done() callback (resolves purely from fn() returning)", function() {
            var capture = new Capture({ timeoutMs: 50 });
            capture.install(RED);
            return loadFlow([
                { id: "p1", type: "temporal-probe", wires: [["s1"]] },
                { id: "s1", type: "helper" }
            ]).then(function() {
                var p1 = helper.getNode("p1");
                var msg = { payload: "fast", _msgid: "r2" };
                var start = Date.now();
                var result = capture.routeSend(p1, msg, function() { p1.send(msg); });
                (Date.now() - start).should.be.below(50);
                result.sends.length.should.equal(1);
                capture.uninstall();
            });
        });
    });
});
