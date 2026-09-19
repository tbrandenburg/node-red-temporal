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
