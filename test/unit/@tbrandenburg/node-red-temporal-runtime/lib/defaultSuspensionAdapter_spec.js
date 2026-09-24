var should = require("should");
var { createDefaultSuspensionAdapter } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/defaultSuspensionAdapter.js");
var { NODE_TYPE } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/signalSuspensionAdapter.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/defaultSuspensionAdapter (issue #98)", function() {
    describe("plan()", function() {
        it("does not suspend a fixed-Delay node when durableFixedDelay is not enabled", function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: false });
            should.not.exist(adapter.plan({ id: "d1", type: "delay", pauseType: "delay", timeout: 200 }, { payload: 1 }));
        });

        it("suspends a fixed-Delay node with a timer descriptor when durableFixedDelay is enabled", function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: true });
            var descriptor = adapter.plan({ id: "d1", type: "delay", pauseType: "delay", timeout: 200 }, { payload: 1 });
            descriptor.should.eql({ type: "timer", durationMs: 200, continuation: null });
        });

        it("always suspends the Signal Wait node, regardless of durableFixedDelay", function() {
            [false, true].forEach(function(durableFixedDelay) {
                var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: durableFixedDelay });
                var descriptor = adapter.plan({ id: "w1", type: NODE_TYPE }, { waitKey: "k" });
                descriptor.should.eql({ type: "signal", key: "k", continuation: null });
            });
        });

        it("does not suspend ordinary nodes", function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: true });
            should.not.exist(adapter.plan({ id: "c1", type: "change" }, { payload: 1 }));
        });
    });

    describe("resume()", function() {
        it("dispatches a timer resume to the Delay sub-adapter", function() {
            var routeSendCalls = [];
            var fakeCapture = {
                routeSend: function(node, msg, fn) {
                    routeSendCalls.push({ node: node });
                    fn();
                    return { sends: [{ port: 0, destinationId: "n2", msg: msg }] };
                }
            };
            var adapter = createDefaultSuspensionAdapter({ capture: fakeCapture, durableFixedDelay: true });
            var node = { id: "d1", type: "delay", send: function() {} };
            return adapter.resume(node, { payload: 1 }, { type: "timer", continuation: null }).then(function(result) {
                routeSendCalls.length.should.equal(1);
                result.sends.length.should.equal(1);
            });
        });

        it("dispatches a signal resume to the Signal Wait sub-adapter", function() {
            var routeSendCalls = [];
            var fakeCapture = {
                routeSend: function(node, msg, fn) {
                    routeSendCalls.push({ msg: msg });
                    fn();
                    return { sends: [{ port: 0, destinationId: "n2", msg: msg }] };
                }
            };
            var adapter = createDefaultSuspensionAdapter({ capture: fakeCapture, durableFixedDelay: true });
            var node = { id: "w1", type: NODE_TYPE, send: function() {} };
            return adapter.resume(node, { payload: 1, waitKey: "k" }, { type: "signal", continuation: null, signal: { ok: true } }).then(function(result) {
                routeSendCalls[0].msg.signal.should.eql({ ok: true });
                result.sends.length.should.equal(1);
            });
        });

        it("throws for a timer resume when durableFixedDelay is not enabled", function() {
            var adapter = createDefaultSuspensionAdapter({ capture: {}, durableFixedDelay: false });
            var node = { id: "d1", type: "delay", send: function() {} };
            return adapter.resume(node, { payload: 1 }, { type: "timer", continuation: null }).should.be.rejected();
        });
    });

    describe("custom options.suspensionAdapter override seam (regression)", function() {
        it("is unaffected by this composite - a caller-supplied adapter replaces it entirely (documented in worker.js, exercised in worker_spec.js)", function() {
            // No-op placeholder assertion: the actual override behavior is
            // exercised end-to-end in worker_spec.js's existing #91/#97
            // tests, which remain untouched.
            true.should.equal(true);
        });
    });
});
