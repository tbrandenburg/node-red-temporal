var should = require("should");
var { createInteractionSuspensionAdapter } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/interactionSuspensionAdapter.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/interactionSuspensionAdapter", function() {
    it("does not suspend ordinary nodes", async function() {
        var adapter = createInteractionSuspensionAdapter({ capture: {} });
        should.not.exist(await adapter.plan({ type: "change" }, {}));
    });

    it("awaits Interaction planning and returns the existing signal descriptor", async function() {
        var msg = { payload: "original" };
        var plan = { interactionId: "interaction-1", prompt: "Continue?", decisions: [{ id: "approve" }] };
        var receivedMsg;
        var node = { type: "interaction", interaction: { plan: async function(value) { receivedMsg = value; return plan; } } };
        var descriptor = await createInteractionSuspensionAdapter({ capture: {} }).plan(node, msg);
        receivedMsg.should.equal(msg);
        descriptor.should.eql({ type: "signal", key: plan.interactionId, continuation: plan });
    });

    it("propagates planning rejection", async function() {
        var adapter = createInteractionSuspensionAdapter({ capture: {} });
        await adapter.plan({ type: "interaction", interaction: { plan: async function() { throw new Error("typed prompt failed"); } } }, {}).should.be.rejectedWith("typed prompt failed");
    });

    it("resumes through Capture with continuation, original message and signal", function() {
        var msg = { payload: "original" };
        var signal = { decision: "revise", text: "add one more test" };
        var continuation = { interactionId: "interaction-1" };
        var calls = [];
        var node = { type: "interaction", interaction: { resume: function() { calls.push(Array.from(arguments)); } } };
        var capture = { routeSend: function(routedNode, routedMsg, fn) {
            calls.push(["routeSend", routedNode, routedMsg]);
            fn();
            return { sends: [{ port: 0, destinationId: "downstream", msg: routedMsg }] };
        } };
        var result = createInteractionSuspensionAdapter({ capture: capture }).resume(node, msg, { continuation: continuation, signal: signal });
        calls.length.should.equal(2);
        calls[0].should.eql(["routeSend", node, msg]);
        calls[1].should.eql([continuation, msg, signal]);
        result.sends[0].destinationId.should.equal("downstream");
    });

    it("does not route downstream when the node rejects an undeclared decision", function() {
        var downstreamSends = 0;
        var node = { type: "interaction", interaction: { resume: function() { throw new Error("Undeclared interaction decision"); } }, send: function() { downstreamSends++; } };
        var capture = { routeSend: function(ignoredNode, ignoredMsg, fn) { return fn(); } };
        (function() {
            createInteractionSuspensionAdapter({ capture: capture }).resume(node, {}, { continuation: {}, signal: { decision: "invalid" } });
        }).should.throw("Undeclared interaction decision");
        downstreamSends.should.equal(0);
    });
});
