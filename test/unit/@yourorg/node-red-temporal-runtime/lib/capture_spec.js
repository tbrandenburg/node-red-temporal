var should = require("should");
var helper = require("node-red-node-test-helper");
var RED = require("nr-test-utils").require("node-red/lib/red");
var { Capture, DEFAULT_TIMEOUT_MS } = require("../../../../../packages/node_modules/@yourorg/node-red-temporal-runtime/lib/capture.js");

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
                default:
                    done();
            }
        });
    }
    RED.nodes.registerType("temporal-probe", ProbeNode);
}

describe("@yourorg/node-red-temporal-runtime/lib/capture", function() {

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

    it("exports an overridable DEFAULT_TIMEOUT_MS", function() {
        DEFAULT_TIMEOUT_MS.should.be.a.Number();
        new Capture().timeoutMs.should.equal(DEFAULT_TIMEOUT_MS);
        new Capture({ timeoutMs: 42 }).timeoutMs.should.equal(42);
    });
});
