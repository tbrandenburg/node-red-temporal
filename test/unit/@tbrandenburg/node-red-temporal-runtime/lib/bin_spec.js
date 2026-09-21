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
        // Each test here spawns a real Node process; the default 3000ms
        // mocha timeout (.mocharc.json, upstream-owned) is occasionally too
        // tight under full-suite CPU contention (many concurrent mocha/nyc
        // processes), causing flaky timeouts unrelated to actual behavior.
        // A generous per-suite timeout absorbs that without masking a real
        // hang (still fails fast on an actual infinite loop/deadlock).
        this.timeout(15000);

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
        this.timeout(15000); // spawns a real Node process, see other block's comment
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
        this.timeout(15000); // spawns a real Node process, see other block's comment
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

describe("@tbrandenburg/node-red-temporal-runtime bin/node-red-temporal - issue #47 --node-timeout-ms", function() {
    var ENV_VAR = "NODE_RED_TEMPORAL_NODE_TIMEOUT_MS";
    var originalEnv;

    beforeEach(function() {
        originalEnv = process.env[ENV_VAR];
        delete process.env[ENV_VAR];
    });

    afterEach(function() {
        if (originalEnv === undefined) {
            delete process.env[ENV_VAR];
        } else {
            process.env[ENV_VAR] = originalEnv;
        }
    });

    it("nodeTimeoutMsFrom returns undefined when neither --node-timeout-ms nor the env var is given (caller applies its own default)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        should(bin.nodeTimeoutMsFrom({})).be.undefined();
    });

    it("nodeTimeoutMsFrom parses --node-timeout-ms as an integer", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        bin.nodeTimeoutMsFrom({ "node-timeout-ms": "12345" }).should.equal(12345);
    });

    it("nodeTimeoutMsFrom falls back to the NODE_RED_TEMPORAL_NODE_TIMEOUT_MS env var when the flag is not given", function() {
        process.env[ENV_VAR] = "54321";
        delete require.cache[BIN];
        var bin = require(BIN);
        bin.nodeTimeoutMsFrom({}).should.equal(54321);
    });

    it("nodeTimeoutMsFrom prefers the explicit flag over the env var", function() {
        process.env[ENV_VAR] = "54321";
        delete require.cache[BIN];
        var bin = require(BIN);
        bin.nodeTimeoutMsFrom({ "node-timeout-ms": "111" }).should.equal(111);
    });

    it("nodeTimeoutMsFrom rejects a non-positive-integer value with a clear error", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        (function() {
            bin.nodeTimeoutMsFrom({ "node-timeout-ms": "not-a-number" });
        }).should.throw(/--node-timeout-ms must be a positive integer/);
    });

    it("--help lists --node-timeout-ms", function() {
        this.timeout(15000); // spawns a real Node process, see other block's comment
        var out = execFileSync(process.execPath, [BIN, "--help"], { encoding: "utf8" });
        out.should.match(/--node-timeout-ms/);
    });

    it("worker --role activity threads --node-timeout-ms into createActivityWorker's options", function() {
        var workerModule = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/worker.js");
        var stub = sinon.stub(workerModule, "createActivityWorker").resolves({
            flowVersion: "v1",
            temporalConfig: { activityTaskQueue: "q", address: "a", namespace: "n" },
            handle: {},
            worker: { run: sinon.stub().resolves() },
            stop: sinon.stub().resolves()
        });
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "activity", flow: "/tmp/does-not-matter.json", "node-timeout-ms": "9999" }).then(function() {
            stub.firstCall.args[1].nodeExecutionTimeoutMs.should.equal(9999);
        }).finally(function() {
            stub.restore();
            delete require.cache[BIN];
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime bin/node-red-temporal - issue #58 --http-ingress-port/--http-ingress-host/--http-sync-timeout-ms", function() {
    var PORT_ENV = "NODE_RED_TEMPORAL_HTTP_INGRESS_PORT";
    var HOST_ENV = "NODE_RED_TEMPORAL_HTTP_INGRESS_HOST";
    var TIMEOUT_ENV = "NODE_RED_TEMPORAL_HTTP_SYNC_TIMEOUT_MS";
    var originalPortEnv, originalHostEnv, originalTimeoutEnv;

    beforeEach(function() {
        originalPortEnv = process.env[PORT_ENV];
        originalHostEnv = process.env[HOST_ENV];
        originalTimeoutEnv = process.env[TIMEOUT_ENV];
        delete process.env[PORT_ENV];
        delete process.env[HOST_ENV];
        delete process.env[TIMEOUT_ENV];
    });

    afterEach(function() {
        if (originalPortEnv === undefined) { delete process.env[PORT_ENV]; } else { process.env[PORT_ENV] = originalPortEnv; }
        if (originalHostEnv === undefined) { delete process.env[HOST_ENV]; } else { process.env[HOST_ENV] = originalHostEnv; }
        if (originalTimeoutEnv === undefined) { delete process.env[TIMEOUT_ENV]; } else { process.env[TIMEOUT_ENV] = originalTimeoutEnv; }
    });

    it("httpIngressOptionsFrom returns undefined when neither --http-ingress-port nor the env var is given (fully opt-in)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        should(bin.httpIngressOptionsFrom({})).be.undefined();
    });

    it("httpIngressOptionsFrom parses --http-ingress-port (including 0) as a non-negative integer", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        bin.httpIngressOptionsFrom({ "http-ingress-port": "0" }).should.eql({ port: 0, host: undefined, syncTimeoutMs: undefined });
        bin.httpIngressOptionsFrom({ "http-ingress-port": "8080" }).port.should.equal(8080);
    });

    it("httpIngressOptionsFrom falls back to env vars when flags are not given", function() {
        process.env[PORT_ENV] = "9090";
        process.env[HOST_ENV] = "0.0.0.0";
        process.env[TIMEOUT_ENV] = "5000";
        delete require.cache[BIN];
        var bin = require(BIN);
        bin.httpIngressOptionsFrom({}).should.eql({ port: 9090, host: "0.0.0.0", syncTimeoutMs: 5000 });
    });

    it("httpIngressOptionsFrom prefers explicit flags over env vars", function() {
        process.env[PORT_ENV] = "9090";
        delete require.cache[BIN];
        var bin = require(BIN);
        bin.httpIngressOptionsFrom({ "http-ingress-port": "1234" }).port.should.equal(1234);
    });

    it("httpIngressOptionsFrom rejects a non-negative-integer --http-ingress-port with a clear error", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        (function() {
            bin.httpIngressOptionsFrom({ "http-ingress-port": "not-a-port" });
        }).should.throw(/--http-ingress-port must be a non-negative integer/);
    });

    it("httpIngressOptionsFrom rejects a non-positive-integer --http-sync-timeout-ms with a clear error", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        (function() {
            bin.httpIngressOptionsFrom({ "http-ingress-port": "8080", "http-sync-timeout-ms": "0" });
        }).should.throw(/--http-sync-timeout-ms must be a positive integer/);
    });

    it("--help lists --http-ingress-port/--http-ingress-host/--http-sync-timeout-ms", function() {
        this.timeout(15000); // spawns a real Node process, see other block's comment
        var out = execFileSync(process.execPath, [BIN, "--help"], { encoding: "utf8" });
        out.should.match(/--http-ingress-port/);
        out.should.match(/--http-ingress-host/);
        out.should.match(/--http-sync-timeout-ms/);
    });

    it("worker --role workflow --http-ingress-port rejects (the workflow role never boots Node-RED)", function() {
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "workflow", "http-ingress-port": "8080" }).then(function() {
            throw new Error("expected runWorker to reject");
        }, function(err) {
            err.message.should.match(/--http-ingress-port is not supported for --role workflow/);
        });
    });

    it("worker --role activity threads --http-ingress-port/--http-ingress-host/--http-sync-timeout-ms into createActivityWorker's options", function() {
        var workerModule = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/worker.js");
        var stub = sinon.stub(workerModule, "createActivityWorker").resolves({
            flowVersion: "v1",
            temporalConfig: { activityTaskQueue: "q", address: "a", namespace: "n" },
            handle: {},
            httpIngressAddress: { host: "127.0.0.1", port: 8080 },
            worker: { run: sinon.stub().resolves() },
            stop: sinon.stub().resolves()
        });
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({
            role: "activity",
            flow: "/tmp/does-not-matter.json",
            "http-ingress-port": "8080",
            "http-ingress-host": "0.0.0.0",
            "http-sync-timeout-ms": "5000"
        }).then(function() {
            stub.firstCall.args[1].httpIngress.should.eql({ port: 8080, host: "0.0.0.0", syncTimeoutMs: 5000 });
        }).finally(function() {
            stub.restore();
            delete require.cache[BIN];
        });
    });

    it("worker --role activity logs the bound HTTP ingress address when enabled", function() {
        var workerModule = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/worker.js");
        var stub = sinon.stub(workerModule, "createActivityWorker").resolves({
            flowVersion: "v1",
            temporalConfig: { activityTaskQueue: "q", address: "a", namespace: "n" },
            handle: {},
            httpIngressAddress: { host: "127.0.0.1", port: 8080 },
            worker: { run: sinon.stub().resolves() },
            stop: sinon.stub().resolves()
        });
        var logSpy = sinon.spy(console, "log");
        delete require.cache[BIN];
        var bin = require(BIN);
        return bin.runWorker({ role: "activity", flow: "/tmp/does-not-matter.json", "http-ingress-port": "8080" }).then(function() {
            var logged = logSpy.getCalls().some(function(call) {
                return /http ingress listening on http:\/\/127\.0\.0\.1:8080\//.test(call.args[0]);
            });
            logged.should.equal(true);
        }).finally(function() {
            logSpy.restore();
            stub.restore();
            delete require.cache[BIN];
        });
    });
});
