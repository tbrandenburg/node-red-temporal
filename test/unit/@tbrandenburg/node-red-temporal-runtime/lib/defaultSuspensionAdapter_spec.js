var should = require("should");
var { createDefaultSuspensionAdapter } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/defaultSuspensionAdapter.js");
var { NODE_TYPE } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/signalSuspensionAdapter.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/defaultSuspensionAdapter (issue #121)", function() {
    describe("plan()", function() {
        it("does not suspend a fixed-Delay node when durableFixedDelay is not enabled", async function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: false });
            should.not.exist(await adapter.plan({ id: "d1", type: "delay", pauseType: "delay", timeout: 200 }, { payload: 1 }));
        });

        it("suspends a fixed-Delay node with a timer descriptor when enabled", async function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: true });
            var descriptor = await adapter.plan({ id: "d1", type: "delay", pauseType: "delay", timeout: 200 }, { payload: 1 });
            descriptor.should.eql({ type: "timer", durationMs: 200, continuation: null });
        });

        it("always suspends the Signal Wait node", async function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: false });
            var descriptor = await adapter.plan({ id: "w1", type: NODE_TYPE }, { waitKey: "k" });
            descriptor.should.eql({ type: "signal", key: "k", continuation: null });
        });

        it("plans Interaction before Signal Wait and preserves the plan as continuation", async function() {
            var plan = { interactionId: "interaction-1", prompt: "Continue?", decisions: [] };
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: false });
            var descriptor = await adapter.plan({ id: "i1", type: "interaction", interaction: { plan: async function() { return plan; } } }, {});
            descriptor.should.eql({ type: "signal", key: "interaction-1", continuation: plan });
        });

        it("does not suspend ordinary nodes", async function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: true });
            should.not.exist(await adapter.plan({ id: "c1", type: "change" }, { payload: 1 }));
        });
    });

    describe("resume()", function() {
        function captureFor(msg) {
            return { routeSend: function(node, routedMsg, fn) {
                fn();
                return { sends: [{ port: 0, destinationId: "n2", msg: routedMsg }] };
            } };
        }

        it("dispatches a timer resume to the Delay sub-adapter", async function() {
            var msg = { payload: 1 };
            var adapter = createDefaultSuspensionAdapter({ capture: captureFor(msg), durableFixedDelay: true });
            var result = await adapter.resume({ id: "d1", type: "delay", send: function() {} }, msg, { type: "timer", continuation: null });
            result.sends.length.should.equal(1);
        });

        it("dispatches an Interaction signal resume to Interaction, not Signal Wait", async function() {
            var msg = { payload: "original" };
            var calls = [];
            var signal = { decision: "approve" };
            var continuation = { interactionId: "interaction-1" };
            var capture = captureFor(msg);
            var originalRouteSend = capture.routeSend;
            capture.routeSend = function(node, routedMsg, fn) {
                calls.push({ node: node, msg: routedMsg });
                return originalRouteSend(node, routedMsg, fn);
            };
            var node = { id: "i1", type: "interaction", interaction: { resume: function() { calls.push(Array.from(arguments)); } } };
            var adapter = createDefaultSuspensionAdapter({ capture: capture, durableFixedDelay: false });
            var result = await adapter.resume(node, msg, { type: "signal", continuation: continuation, signal: signal });
            calls.length.should.equal(2);
            calls[0].node.should.equal(node);
            calls[1].should.eql([continuation, msg, signal]);
            result.sends[0].destinationId.should.equal("n2");
        });

        it("dispatches a Signal Wait signal resume to the existing adapter", async function() {
            var msg = { payload: 1, waitKey: "k" };
            var adapter = createDefaultSuspensionAdapter({ capture: captureFor(msg), durableFixedDelay: true });
            var result = await adapter.resume({ id: "w1", type: NODE_TYPE, send: function() {} }, msg, { type: "signal", continuation: null, signal: { ok: true } });
            result.sends[0].msg.signal.should.eql({ ok: true });
        });

        it("throws for a timer resume when durableFixedDelay is not enabled", async function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: false });
            await adapter.resume({ id: "d1", type: "delay" }, { payload: 1 }, { type: "timer", continuation: null }).should.be.rejected();
        });
    });
});
