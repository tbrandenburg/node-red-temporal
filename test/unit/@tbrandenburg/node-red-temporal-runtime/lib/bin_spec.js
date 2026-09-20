var should = require("should");
var path = require("path");
var sinon = require("sinon");
var { execFileSync } = require("child_process");

var BIN = path.join(__dirname, "..", "..", "..", "..", "..", "packages", "node_modules", "@tbrandenburg", "node-red-temporal-runtime", "bin", "node-red-temporal");
var PKG = require(path.join(__dirname, "..", "..", "..", "..", "..", "packages", "node_modules", "@tbrandenburg", "node-red-temporal-runtime", "package.json"));

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FLOW = path.join(FIXTURES, "four-node-flow.json");

// Stubbing @temporalio/client via require.cache (rather than a subprocess
// spawn against a real/absent server) keeps the "start subcommand success"
// and "connection failure" cases fast and deterministic, following the
// same "stub the SDK's own entry point" convention as worker_spec.js's
// `sinon.stub(Worker, "create")`.
var CLIENT_PATH = require.resolve("@temporalio/client");

describe("@tbrandenburg/node-red-temporal-runtime bin/node-red-temporal", function() {
    describe("CLI process (spawned, no network calls)", function() {
        it("--help prints usage and exits 0", function() {
            var out = execFileSync(process.execPath, [BIN, "--help"], { encoding: "utf8" });
            out.should.match(/Usage: node-red-temporal/);
        });

        it("-h prints usage and exits 0", function() {
            var out = execFileSync(process.execPath, [BIN, "-h"], { encoding: "utf8" });
            out.should.match(/Usage: node-red-temporal/);
        });

        it("--version prints the package.json version and exits 0", function() {
            var out = execFileSync(process.execPath, [BIN, "--version"], { encoding: "utf8" });
            out.trim().should.equal(PKG.version);
        });

        it("start without required flags exits 1 with a clear error", function() {
            (function() {
                execFileSync(process.execPath, [BIN, "start", "--flow", FLOW], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
            }).should.throw(/start requires --flow <path> --start-node <id> --input <json>/);
        });

        it("start with malformed --input JSON exits 1 with a specific parse-error message", function() {
            (function() {
                execFileSync(process.execPath, [BIN, "start", "--flow", FLOW, "--start-node", "n1", "--input", "not-json"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
            }).should.throw(/invalid JSON for --input:/);
        });

        it("accepts both --key value and --key=value forms identically", function() {
            (function() {
                execFileSync(process.execPath, [BIN, "start", "--flow=" + FLOW, "--start-node=n1", "--input=not-json"], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
            }).should.throw(/invalid JSON for --input:/);
        });
    });

    describe("parseArgs()", function() {
        var bin = require(BIN);

        it("parses --key value pairs", function() {
            var values = bin.parseArgs(["--flow", "flows.json", "--start-node", "n1"]);
            values.flow.should.equal("flows.json");
            values["start-node"].should.equal("n1");
        });

        it("parses --key=value pairs", function() {
            var values = bin.parseArgs(["--flow=flows.json", "--start-node=n1"]);
            values.flow.should.equal("flows.json");
            values["start-node"].should.equal("n1");
        });
    });

    describe("in-process (require.cache-stubbed @temporalio/client)", function() {
        var originalClientModule;

        afterEach(function() {
            if (originalClientModule) {
                require.cache[CLIENT_PATH] = originalClientModule;
                originalClientModule = null;
            } else {
                delete require.cache[CLIENT_PATH];
            }
        });

        it("start subcommand success: valid flow/start-node/input starts a workflow via the client", function() {
            var handle = { workflowId: "node-red-temporal-test", firstExecutionRunId: "run-1" };
            var startStub = sinon.stub().resolves(handle);
            var connectStub = sinon.stub().resolves({});

            originalClientModule = require.cache[CLIENT_PATH];
            require.cache[CLIENT_PATH] = {
                id: CLIENT_PATH,
                filename: CLIENT_PATH,
                loaded: true,
                exports: {
                    Connection: { connect: connectStub },
                    Client: function() {
                        return { workflow: { start: startStub } };
                    }
                }
            };

            delete require.cache[BIN];
            var bin = require(BIN);

            return bin.runStart({ flow: FLOW, "start-node": "n1", input: JSON.stringify({ payload: 21 }) }).then(function() {
                connectStub.calledOnce.should.equal(true);
                startStub.calledOnce.should.equal(true);
                var call = startStub.firstCall.args;
                call[0].should.equal("executeFlow");
                call[1].args[0].startNode.should.equal("n1");
                call[1].args[0].startMsg.should.deepEqual({ payload: 21 });
                call[1].args[0].nodeMeta.should.have.property("n1");
                call[1].args[0].nodeMeta.n1.should.have.property("type", "inject");

                // issue #6: workflow-level memo/staticSummary, so the Temporal
                // Web UI's workflow list identifies flow/version/start-node
                // without opening the execution's raw input.
                call[1].memo.should.have.property("flowId", "four-node-flow");
                call[1].memo.should.have.property("startNode", "n1");
                call[1].memo.should.have.property("flowVersion");
                call[1].staticSummary.should.match(/^four-node-flow@/);
                call[1].staticSummary.should.containEql("from n1");
            });
        });

        it("connection failure: a rejected Connection.connect() propagates as a rejected runStart()", function() {
            var connectStub = sinon.stub().rejects(new Error("connection refused"));

            originalClientModule = require.cache[CLIENT_PATH];
            require.cache[CLIENT_PATH] = {
                id: CLIENT_PATH,
                filename: CLIENT_PATH,
                loaded: true,
                exports: {
                    Connection: { connect: connectStub },
                    Client: function() {
                        throw new Error("Client should not be constructed when connect() rejects");
                    }
                }
            };

            delete require.cache[BIN];
            var bin = require(BIN);

            return bin.runStart({ flow: FLOW, "start-node": "n1", input: JSON.stringify({ payload: 21 }) }).then(function() {
                throw new Error("expected runStart() to reject");
            }, function(err) {
                err.message.should.match(/connection refused/);
            });
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime bin/node-red-temporal - issue #16 role selection", function() {
    it("worker --role workflow requires no --flow flag (parsed correctly, no error thrown by parseArgs)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        var values = bin.parseArgs(["--role", "workflow"]);
        values.role.should.equal("workflow");
        should.not.exist(values.flow);
    });

    it("worker --role activity without --flow rejects with a clear error (runWorker)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "activity" }).then(function() {
            throw new Error("expected runWorker to reject");
        }, function(err) {
            err.message.should.match(/--flow <path> is required for --role activity/);
        });
    });

    it("worker --role combined without --flow rejects with a clear error (runWorker)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "combined" }).then(function() {
            throw new Error("expected runWorker to reject");
        }, function(err) {
            err.message.should.match(/--flow <path> is required/);
        });
    });

    it("worker --role bogus rejects with a clear 'unknown role' error", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "bogus" }).then(function() {
            throw new Error("expected runWorker to reject");
        }, function(err) {
            err.message.should.match(/unknown --role "bogus"/);
        });
    });

    it("--help lists the worker --role subcommand and Temporal config flags", function() {
        var out = execFileSync(process.execPath, [BIN, "--help"], { encoding: "utf8" });
        out.should.match(/--role combined\|workflow\|activity/);
        out.should.match(/--address/);
        out.should.match(/--namespace/);
        out.should.match(/--workflow-task-queue/);
        out.should.match(/--activity-task-queue/);
        out.should.match(/--admin-port/);
        out.should.match(/--admin-host/);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime bin/node-red-temporal - issue #39 --admin-port", function() {
    it("worker --role workflow --admin-port rejects (the workflow role never boots Node-RED)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "workflow", "admin-port": "1881" }).then(function() {
            throw new Error("expected runWorker to reject");
        }, function(err) {
            err.message.should.match(/--admin-port is not supported for --role workflow/);
        });
    });

    it("worker --role activity --admin-port with a non-numeric value rejects with a clear error", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "activity", flow: "/tmp/does-not-matter.json", "admin-port": "not-a-port" }).then(function() {
            throw new Error("expected runWorker to reject");
        }, function(err) {
            err.message.should.match(/--admin-port must be a non-negative integer/);
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime bin/node-red-temporal - issue #46 --user-dir/--settings", function() {
    var fs = require("fs");
    var os = require("os");
    var origUserDirEnv;
    var origSettingsEnv;

    beforeEach(function() {
        origUserDirEnv = process.env.NODE_RED_TEMPORAL_USER_DIR;
        origSettingsEnv = process.env.NODE_RED_TEMPORAL_SETTINGS;
        delete process.env.NODE_RED_TEMPORAL_USER_DIR;
        delete process.env.NODE_RED_TEMPORAL_SETTINGS;
    });

    afterEach(function() {
        if (origUserDirEnv === undefined) {
            delete process.env.NODE_RED_TEMPORAL_USER_DIR;
        } else {
            process.env.NODE_RED_TEMPORAL_USER_DIR = origUserDirEnv;
        }
        if (origSettingsEnv === undefined) {
            delete process.env.NODE_RED_TEMPORAL_SETTINGS;
        } else {
            process.env.NODE_RED_TEMPORAL_SETTINGS = origSettingsEnv;
        }
    });

    it("--help documents --user-dir and --settings", function() {
        var out = execFileSync(process.execPath, [BIN, "--help"], { encoding: "utf8" });
        out.should.match(/--user-dir/);
        out.should.match(/--settings/);
    });

    it("parseArgs() parses --user-dir and --settings", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        var values = bin.parseArgs(["--user-dir", "/tmp/foo", "--settings", "/tmp/settings.js"]);
        values["user-dir"].should.equal("/tmp/foo");
        values.settings.should.equal("/tmp/settings.js");
    });

    it("worker --role activity threads --user-dir/--settings into createActivityWorker's bootstrapOptions", function() {
        var settingsPath = path.join(os.tmpdir(), "nrt-46-settings-" + Date.now() + ".js");
        fs.writeFileSync(settingsPath, "module.exports = { functionGlobalContext: { marker: 42 } };");
        var userDir = path.join(os.tmpdir(), "nrt-46-userdir-" + Date.now());

        var workerModulePath = require.resolve(path.join(__dirname, "..", "..", "..", "..", "..", "packages", "node_modules", "@tbrandenburg", "node-red-temporal-runtime", "lib", "worker"));
        var original = require.cache[workerModulePath];
        var createActivityWorkerStub = sinon.stub().resolves({
            flowVersion: "v1",
            temporalConfig: { activityTaskQueue: "q", address: "a", namespace: "n" },
            handle: {},
            worker: { run: sinon.stub().resolves() },
            stop: sinon.stub().resolves()
        });

        require.cache[workerModulePath] = {
            id: workerModulePath,
            filename: workerModulePath,
            loaded: true,
            exports: {
                createActivityWorker: createActivityWorkerStub,
                createWorkflowWorker: sinon.stub(),
                createCombinedWorker: sinon.stub(),
                resolveTemporalConfig: sinon.stub()
            }
        };

        delete require.cache[BIN];
        var bin = require(BIN);

        return bin.runWorker({ role: "activity", flow: FLOW, "user-dir": userDir, settings: settingsPath }).then(function() {
            createActivityWorkerStub.calledOnce.should.equal(true);
            var options = createActivityWorkerStub.firstCall.args[1];
            options.bootstrapOptions.userDir.should.equal(path.resolve(userDir));
            options.bootstrapOptions.settings.should.have.property("functionGlobalContext");
            options.bootstrapOptions.settings.functionGlobalContext.should.have.property("marker", 42);
        }).finally(function() {
            if (original) {
                require.cache[workerModulePath] = original;
            } else {
                delete require.cache[workerModulePath];
            }
            fs.unlinkSync(settingsPath);
        });
    });

    it("omitting --user-dir/--settings does not add them to bootstrapOptions", function() {
        var workerModulePath = require.resolve(path.join(__dirname, "..", "..", "..", "..", "..", "packages", "node_modules", "@tbrandenburg", "node-red-temporal-runtime", "lib", "worker"));
        var original = require.cache[workerModulePath];
        var createActivityWorkerStub = sinon.stub().resolves({
            flowVersion: "v1",
            temporalConfig: { activityTaskQueue: "q", address: "a", namespace: "n" },
            handle: {},
            worker: { run: sinon.stub().resolves() },
            stop: sinon.stub().resolves()
        });

        require.cache[workerModulePath] = {
            id: workerModulePath,
            filename: workerModulePath,
            loaded: true,
            exports: {
                createActivityWorker: createActivityWorkerStub,
                createWorkflowWorker: sinon.stub(),
                createCombinedWorker: sinon.stub(),
                resolveTemporalConfig: sinon.stub()
            }
        };

        delete require.cache[BIN];
        var bin = require(BIN);

        return bin.runWorker({ role: "activity", flow: FLOW }).then(function() {
            createActivityWorkerStub.calledOnce.should.equal(true);
            var options = createActivityWorkerStub.firstCall.args[1];
            should.not.exist(options.bootstrapOptions);
        }).finally(function() {
            if (original) {
                require.cache[workerModulePath] = original;
            } else {
                delete require.cache[workerModulePath];
            }
        });
    });
});
