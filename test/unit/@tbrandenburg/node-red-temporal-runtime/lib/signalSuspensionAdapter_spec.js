var should = require("should");
var { createSignalSuspensionAdapter, NODE_TYPE, DEFAULT_KEY_PROPERTY } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/signalSuspensionAdapter.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/signalSuspensionAdapter (issue #98)", function() {
    describe("plan()", function() {
        it("returns undefined for any node type other than the production Signal Wait node", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            should.not.exist(adapter.plan({ id: "n1", type: "change" }, { waitKey: "k" }));
            should.not.exist(adapter.plan({ id: "n1", type: "delay" }, { waitKey: "k" }));
        });

        it("throws when the resolved key property is missing/empty/non-string on a Signal Wait node", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            (function() { adapter.plan({ id: "n1", type: NODE_TYPE }, {}); }).should.throw();
            (function() { adapter.plan({ id: "n1", type: NODE_TYPE }, { waitKey: "" }); }).should.throw();
            (function() { adapter.plan({ id: "n1", type: NODE_TYPE }, { waitKey: 123 }); }).should.throw();
        });

        it("returns a signal suspension keyed by msg[DEFAULT_KEY_PROPERTY] by default", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            var descriptor = adapter.plan({ id: "n1", type: NODE_TYPE }, { waitKey: "order:123:approval" });
            descriptor.should.eql({ type: "signal", key: "order:123:approval", continuation: null });
            DEFAULT_KEY_PROPERTY.should.equal("waitKey");
        });

        it("resolves the key from node.keyProperty when configured", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            var descriptor = adapter.plan({ id: "n1", type: NODE_TYPE, keyProperty: "correlationId" }, { correlationId: "abc", waitKey: "irrelevant" });
            descriptor.should.eql({ type: "signal", key: "abc", continuation: null });
        });
    });

    describe("resume()", function() {
        it("maps the opaque delivered signal into msg.signal, strips the key property, and routes via capture.routeSend()", function() {
            var routeSendCalls = [];
            var fakeCapture = {
                routeSend: function(node, msg, fn) {
                    routeSendCalls.push({ node: node, msg: msg });
                    fn();
                    return { sends: [{ port: 0, destinationId: "n2", msg: msg }] };
                }
            };
            var adapter = createSignalSuspensionAdapter({ capture: fakeCapture });
            var sentCalls = [];
            var node = { id: "wait1", type: NODE_TYPE, send: function(msg) { sentCalls.push(msg); } };
            var originalMsg = { payload: 0, waitKey: "order:123:approval", _msgid: "m1" };
            var signalData = { approved: true, reason: "ok" };

            return adapter.resume(node, originalMsg, { type: "signal", continuation: null, signal: signalData }).then(function(result) {
                routeSendCalls.length.should.equal(1);
                routeSendCalls[0].msg.signal.should.eql(signalData);
                should.not.exist(routeSendCalls[0].msg.waitKey);
                routeSendCalls[0].msg.payload.should.equal(0);
                sentCalls.length.should.equal(1);
                sentCalls[0].signal.should.eql(signalData);
                result.sends.should.eql([{ port: 0, destinationId: "n2", msg: routeSendCalls[0].msg }]);
            });
        });

        it("uses the configured keyProperty when stripping the resumed msg", function() {
            var routeSendCalls = [];
            var fakeCapture = {
                routeSend: function(node, msg, fn) {
                    routeSendCalls.push({ msg: msg });
                    fn();
                    return { sends: [] };
                }
            };
            var adapter = createSignalSuspensionAdapter({ capture: fakeCapture });
            var node = { id: "wait1", type: NODE_TYPE, keyProperty: "correlationId", send: function() {} };
            var originalMsg = { payload: 0, correlationId: "abc" };
            return adapter.resume(node, originalMsg, { signal: { ok: true } }).then(function() {
                should.not.exist(routeSendCalls[0].msg.correlationId);
                originalMsg.correlationId.should.equal("abc");
            });
        });

        it("does not mutate the original msg object (send is a fresh merged object)", function() {
            var fakeCapture = { routeSend: function(node, msg, fn) { fn(); return { sends: [] }; } };
            var adapter = createSignalSuspensionAdapter({ capture: fakeCapture });
            var node = { id: "wait1", type: NODE_TYPE, send: function() {} };
            var originalMsg = { payload: 0, waitKey: "k" };
            return adapter.resume(node, originalMsg, { signal: { ok: false } }).then(function() {
                should.exist(originalMsg.waitKey);
                should.not.exist(originalMsg.signal);
            });
        });
    });
});
