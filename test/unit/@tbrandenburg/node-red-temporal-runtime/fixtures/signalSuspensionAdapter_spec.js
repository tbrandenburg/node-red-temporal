var should = require("should");
var { createSignalSuspensionAdapter } = require("./signalSuspensionAdapter.js");

describe("test fixture: signalSuspensionAdapter (issue #91)", function() {
    describe("plan()", function() {
        it("returns undefined (do not suspend) when msg.waitKey is absent", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            should.not.exist(adapter.plan({ id: "n1", type: "change" }, { payload: 1 }));
        });

        it("returns undefined when msg.waitKey is not a non-empty string", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            should.not.exist(adapter.plan({ id: "n1" }, { waitKey: 123 }));
            should.not.exist(adapter.plan({ id: "n1" }, { waitKey: "" }));
        });

        it("returns a signal suspension keyed by msg.waitKey, with small opaque continuation, regardless of node.type", function() {
            var adapter = createSignalSuspensionAdapter({ capture: {} });
            var descriptor = adapter.plan({ id: "n1", type: "debug" }, { waitKey: "approval:123", payload: "irrelevant" });
            descriptor.should.eql({ type: "signal", key: "approval:123", continuation: { waitKey: "approval:123" } });
        });
    });

    describe("resume()", function() {
        it("maps the opaque delivered signal data into msg.approval and routes the send via capture.routeSend(), without interpreting the data", function() {
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
            var node = { id: "wait1", send: function(msg) { sentCalls.push(msg); } };
            var originalMsg = { payload: 0, _msgid: "m1" };
            var signalData = { approved: true, approvedBy: "test-user", reason: "ok" };

            return adapter.resume(node, originalMsg, { type: "signal", continuation: { waitKey: "approval:123" }, signal: signalData }).then(function(result) {
                routeSendCalls.length.should.equal(1);
                routeSendCalls[0].node.should.equal(node);
                routeSendCalls[0].msg.approval.should.eql(signalData);
                routeSendCalls[0].msg.payload.should.equal(0);
                sentCalls.length.should.equal(1);
                sentCalls[0].approval.should.eql(signalData);
                result.sends.should.eql([{ port: 0, destinationId: "n2", msg: routeSendCalls[0].msg }]);
            });
        });

        it("does not mutate the original msg object (send is a fresh merged object)", function() {
            var fakeCapture = { routeSend: function(node, msg, fn) { fn(); return { sends: [] }; } };
            var adapter = createSignalSuspensionAdapter({ capture: fakeCapture });
            var node = { id: "wait1", send: function() {} };
            var originalMsg = { payload: 0 };
            return adapter.resume(node, originalMsg, { signal: { approved: false } }).then(function() {
                should.not.exist(originalMsg.approval);
            });
        });
    });
});
