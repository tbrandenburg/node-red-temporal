var should = require("should");
var path = require("path");
var EventEmitter = require("events").EventEmitter;
var { bootstrap, computeFlowVersion } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");
var FLOW_REFORMATTED = path.join(FIXTURES, "four-node-flow.reformatted.json");
var FLOW_CHANGED = path.join(FIXTURES, "four-node-flow.changed.json");
var NODE_IDS = ["n1", "n2", "n3", "n4"];

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap", function() {
    this.timeout(20000);

    var runtimeHandle;

    afterEach(function() {
        if (runtimeHandle) {
            var handle = runtimeHandle;
            runtimeHandle = null;
            return handle.stop();
        }
    });

    it("boots a live flow and resolves every node id to a real node instance", function() {
        return bootstrap(FLOW).then(function(handle) {
            runtimeHandle = handle;
            NODE_IDS.forEach(function(id) {
                var node = handle.getNode(id);
                should.exist(node);
                node.id.should.equal(id);
                node.type.should.be.a.String();
                node.should.be.an.instanceof(EventEmitter);
                node.should.have.property("_flow");
            });
        });
    });

    it("flowVersion is stable/deterministic across two bootstraps of the same content", function() {
        return bootstrap(FLOW).then(function(handleA) {
            runtimeHandle = handleA;
            var versionA = handleA.flowVersion;
            return handleA.stop().then(function() {
                runtimeHandle = null;
                return bootstrap(FLOW);
            }).then(function(handleB) {
                runtimeHandle = handleB;
                handleB.flowVersion.should.equal(versionA);
            });
        });
    });

    it("flowVersion is identical for semantically-equal flows with different JSON formatting/key order", function() {
        return bootstrap(FLOW).then(function(handleA) {
            runtimeHandle = handleA;
            var versionA = handleA.flowVersion;
            return handleA.stop().then(function() {
                runtimeHandle = null;
                return bootstrap(FLOW_REFORMATTED);
            }).then(function(handleB) {
                runtimeHandle = handleB;
                handleB.flowVersion.should.equal(versionA);
            });
        });
    });

    it("flowVersion changes when actual flow content changes", function() {
        return bootstrap(FLOW).then(function(handleA) {
            runtimeHandle = handleA;
            var versionA = handleA.flowVersion;
            return handleA.stop().then(function() {
                runtimeHandle = null;
                return bootstrap(FLOW_CHANGED);
            }).then(function(handleB) {
                runtimeHandle = handleB;
                handleB.flowVersion.should.not.equal(versionA);
            });
        });
    });

    it("computeFlowVersion is a pure function usable without bootstrapping a runtime", function() {
        var fs = require("fs");
        var a = JSON.parse(fs.readFileSync(FLOW, "utf8"));
        var b = JSON.parse(fs.readFileSync(FLOW_REFORMATTED, "utf8"));
        var c = JSON.parse(fs.readFileSync(FLOW_CHANGED, "utf8"));
        computeFlowVersion(a).should.equal(computeFlowVersion(b));
        computeFlowVersion(a).should.not.equal(computeFlowVersion(c));
    });
});
