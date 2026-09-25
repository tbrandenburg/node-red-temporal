var should = require("should");
var sinon = require("sinon");
var {
    buildTemporalMessageMetadata,
    createTemporalMetadataExecuteNode
} = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/temporalMetadata.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/temporalMetadata", function() {
    describe("buildTemporalMessageMetadata (pure helper)", function() {
        it("same workflowId/runId/activityId -> same operationId", function() {
            var identity = { workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" };
            var a = buildTemporalMessageMetadata(identity);
            var b = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" });
            a.operationId.should.equal(b.operationId);
        });

        it("changing workflowId alone changes operationId", function() {
            var base = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" });
            var changed = buildTemporalMessageMetadata({ workflowId: "wf-2", runId: "run-1", activityId: "node:n1:0" });
            base.operationId.should.not.equal(changed.operationId);
        });

        it("changing runId alone changes operationId", function() {
            var base = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" });
            var changed = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-2", activityId: "node:n1:0" });
            base.operationId.should.not.equal(changed.operationId);
        });

        it("changing activityId alone changes operationId", function() {
            var base = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" });
            var changed = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-1", activityId: "node:n2:0" });
            base.operationId.should.not.equal(changed.operationId);
        });

        it("output is a plain JSON-serializable object with exactly the documented fields", function() {
            var metadata = buildTemporalMessageMetadata({ workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" });
            Object.keys(metadata).sort().should.eql(["activityId", "operationId", "runId", "workflowId"]);
            JSON.parse(JSON.stringify(metadata)).should.eql(metadata);
            metadata.operationId.should.be.a.String();
        });

        it("never accepts an attempt number - the function signature has no such input", function() {
            // Two calls with the identical three identifiers must always
            // produce the same operationId, proving there is no hidden
            // attempt-dependent input at all (there is no `attempt`
            // parameter to even pass).
            var identity = { workflowId: "wf-1", runId: "run-1", activityId: "node:n1:0" };
            var attempt1 = buildTemporalMessageMetadata(identity);
            var attempt2 = buildTemporalMessageMetadata(identity);
            attempt1.should.eql(attempt2);
        });
    });

    describe("createTemporalMetadataExecuteNode (wrapper)", function() {
        function infoFor(workflowId, runId, activityId) {
            return {
                activityId: activityId,
                workflowExecution: { workflowId: workflowId, runId: runId }
            };
        }

        it("same Activity context across two calls (simulated retry) -> identical msg._temporal", function() {
            var getActivityInfo = sinon.stub().returns(infoFor("wf-1", "run-1", "node:n1:0"));
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            return wrapped({ nodeId: "n1", msg: { payload: 1 } }).then(function() {
                return wrapped({ nodeId: "n1", msg: { payload: 1 } }).then(function() {
                    var first = executeNode.firstCall.args[0].msg._temporal;
                    var second = executeNode.secondCall.args[0].msg._temporal;
                    first.should.eql(second);
                });
            });
        });

        it("different Activity context -> different msg._temporal", function() {
            var getActivityInfo = sinon.stub();
            getActivityInfo.onCall(0).returns(infoFor("wf-1", "run-1", "node:n1:0"));
            getActivityInfo.onCall(1).returns(infoFor("wf-1", "run-1", "node:n2:0"));
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            return wrapped({ nodeId: "n1", msg: {} }).then(function() {
                return wrapped({ nodeId: "n2", msg: {} }).then(function() {
                    var first = executeNode.firstCall.args[0].msg._temporal;
                    var second = executeNode.secondCall.args[0].msg._temporal;
                    first.operationId.should.not.equal(second.operationId);
                });
            });
        });

        it("overwrites a spoofed msg._temporal with authoritative values", function() {
            var getActivityInfo = sinon.stub().returns(infoFor("wf-1", "run-1", "node:n1:0"));
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            var input = { nodeId: "n1", msg: { payload: 1, _temporal: { operationId: "fake" } } };
            return wrapped(input).then(function() {
                var forwarded = executeNode.firstCall.args[0].msg._temporal;
                forwarded.operationId.should.not.equal("fake");
                forwarded.workflowId.should.equal("wf-1");
            });
        });

        it("does not mutate the caller's original input/msg objects", function() {
            var getActivityInfo = sinon.stub().returns(infoFor("wf-1", "run-1", "node:n1:0"));
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            var originalMsg = { payload: 1 };
            var input = { nodeId: "n1", msg: originalMsg };
            return wrapped(input).then(function() {
                should(originalMsg._temporal).be.undefined();
                input.msg.should.equal(originalMsg);
                executeNode.firstCall.args[0].should.not.equal(input);
                executeNode.firstCall.args[0].msg.should.not.equal(originalMsg);
            });
        });

        it("passes through the wrapped executeNode's resolved value unchanged", function() {
            var getActivityInfo = sinon.stub().returns(infoFor("wf-1", "run-1", "node:n1:0"));
            var resolved = { sends: [{ port: 0, destinationId: "n2", msg: { payload: 2 } }] };
            var executeNode = sinon.stub().resolves(resolved);
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            return wrapped({ nodeId: "n1", msg: {} }).then(function(result) {
                result.should.eql(resolved);
            });
        });

        it("passes through a rejection unchanged", function() {
            var getActivityInfo = sinon.stub().returns(infoFor("wf-1", "run-1", "node:n1:0"));
            var thrown = new Error("boom");
            var executeNode = sinon.stub().rejects(thrown);
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            return wrapped({ nodeId: "n1", msg: {} }).then(function() {
                throw new Error("expected rejection");
            }, function(err) {
                err.should.equal(thrown);
            });
        });

        it("is a transparent passthrough when there is no live Activity context (getActivityInfo throws)", function() {
            var getActivityInfo = sinon.stub().throws(new Error("no activity context"));
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            var input = { nodeId: "n1", msg: { payload: 1 } };
            return wrapped(input).then(function(result) {
                executeNode.firstCall.args[0].should.equal(input);
                result.should.eql({ sends: [] });
            });
        });

        it("is a transparent passthrough when getActivityInfo returns no workflowExecution", function() {
            var getActivityInfo = sinon.stub().returns({ activityId: "node:n1:0" });
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode, { getActivityInfo: getActivityInfo });

            var input = { nodeId: "n1", msg: { payload: 1 } };
            return wrapped(input).then(function() {
                executeNode.firstCall.args[0].should.equal(input);
            });
        });

        it("uses the real @temporalio/activity Context by default (no getActivityInfo DI'd in)", function() {
            // Outside a live Activity execution, Context.current() throws -
            // the wrapper must swallow that and passthrough, proving the
            // default DI is wired to the real SDK lazily, matching
            // heartbeat.js's `defaultHeartbeat` convention.
            var executeNode = sinon.stub().resolves({ sends: [] });
            var wrapped = createTemporalMetadataExecuteNode(executeNode);
            var input = { nodeId: "n1", msg: {} };
            return wrapped(input).then(function() {
                executeNode.firstCall.args[0].should.equal(input);
            });
        });
    });
});
