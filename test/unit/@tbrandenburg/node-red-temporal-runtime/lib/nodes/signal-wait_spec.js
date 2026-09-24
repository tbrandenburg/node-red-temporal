var should = require("should");
var helper = require("node-red-node-test-helper");
var signalWaitNode = require("../../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/nodes/signal-wait.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/nodes/signal-wait (issue #98)", function() {
    beforeEach(function(done) {
        helper.startServer(done);
    });

    afterEach(function(done) {
        helper.unload().then(function() {
            helper.stopServer(done);
        });
    });

    it("registers as node type \"temporal signal wait\" and defaults keyProperty to waitKey", function() {
        return new Promise(function(resolve) {
            helper.load(signalWaitNode, [
                { id: "w1", type: "temporal signal wait", name: "wait1", wires: [[]] }
            ], resolve);
        }).then(function() {
            var w1 = helper.getNode("w1");
            should.exist(w1);
            w1.type.should.equal("temporal signal wait");
            w1.keyProperty.should.equal("waitKey");
        });
    });

    it("honours a configured keyProperty", function() {
        return new Promise(function(resolve) {
            helper.load(signalWaitNode, [
                { id: "w1", type: "temporal signal wait", keyProperty: "correlationId", wires: [[]] }
            ], resolve);
        }).then(function() {
            var w1 = helper.getNode("w1");
            w1.keyProperty.should.equal("correlationId");
        });
    });

    it("falls back to waitKey when keyProperty is configured blank", function() {
        return new Promise(function(resolve) {
            helper.load(signalWaitNode, [
                { id: "w1", type: "temporal signal wait", keyProperty: "   ", wires: [[]] }
            ], resolve);
        }).then(function() {
            var w1 = helper.getNode("w1");
            w1.keyProperty.should.equal("waitKey");
        });
    });

    it("on('input') fallback (no suspension adapter installed) passes the message through unchanged", function() {
        return new Promise(function(resolve) {
            helper.load(signalWaitNode, [
                { id: "w1", type: "temporal signal wait", wires: [["s1"]] },
                { id: "s1", type: "helper" }
            ], resolve);
        }).then(function() {
            var w1 = helper.getNode("w1");
            var s1 = helper.getNode("s1");
            return new Promise(function(resolve) {
                s1.on("input", function(msg) {
                    resolve(msg);
                });
                w1.receive({ payload: 42, waitKey: "k" });
            });
        }).then(function(msg) {
            msg.payload.should.equal(42);
            msg.waitKey.should.equal("k");
        });
    });
});
