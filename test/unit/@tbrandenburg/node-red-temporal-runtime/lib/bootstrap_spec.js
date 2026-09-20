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

    it("does not accumulate comms:* listeners on the shared @node-red/util events singleton across repeated bootstrap()+stop() cycles (issue #18)", function() {
        var redUtil = require("../../../../../packages/node_modules/@node-red/util");
        var COMMS_EVENTS = [
            "comms:connection-removed",
            "comms:message:multiplayer/connect",
            "comms:message:multiplayer/disconnect",
            "comms:message:multiplayer/location"
        ];
        var warnings = [];
        function onWarning(warning) {
            if (warning.name === "MaxListenersExceededWarning") {
                warnings.push(warning);
            }
        }
        process.on("warning", onWarning);

        // Baseline BEFORE our cycles, not an assumed 0: in a full-suite run,
        // other (out-of-scope, upstream-owned) test files may legitimately
        // add their own listeners to this same shared singleton earlier in
        // the process. The bug this test guards against is OUR bootstrap()
        // calls leaking - i.e. no net growth relative to whatever baseline
        // already existed - not the singleton being pristine.
        var baseline = {};
        COMMS_EVENTS.forEach(function(name) {
            baseline[name] = redUtil.events.listenerCount(name);
        });

        var CYCLES = 15;
        var chain = Promise.resolve();
        for (var i = 0; i < CYCLES; i++) {
            chain = chain.then(function() {
                return bootstrap(FLOW).then(function(handle) {
                    return handle.stop();
                });
            });
        }

        return chain.then(function() {
            return new Promise(function(resolve) { setImmediate(resolve); });
        }).then(function() {
            process.removeListener("warning", onWarning);
            warnings.should.be.empty();
            COMMS_EVENTS.forEach(function(name) {
                redUtil.events.listenerCount(name).should.equal(baseline[name]);
            });
        }, function(err) {
            process.removeListener("warning", onWarning);
            throw err;
        });
    });

    describe("options.adminApi (issue #39 M2: runner Admin API surface)", function() {
        // NOTE: the actual admin-server-boots-and-accepts-a-deploy behavior
        // is verified in bootstrap_adminApi_spec.js via a SPAWNED child
        // process, not here. `@node-red/editor-api` and `@node-red/registry`
        // hold process-wide module-level singleton state (icon/template
        // routes, `disableEditor` handling, etc.) that upstream's OWN
        // editor-api unit tests (test/unit/@node-red/editor-api/**) also
        // depend on being pristine within this same mocha process - actually
        // exercising `options.adminApi` in-process here was observed to
        // permanently break ~60 unrelated, later-running tests (both ours
        // and upstream's) for the rest of the full-suite run. See this
        // repo's Lessons Learned: "never patch a shared prototype/
        // class-wide method as a global toggle" - the same principle
        // applies to booting a real Admin API server that touches these
        // shared singletons.
        it("does not start an HTTP server at all when options.adminApi is omitted", function() {
            return bootstrap(FLOW).then(function(handle) {
                runtimeHandle = handle;
                should.not.exist(handle.adminApiAddress);
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
