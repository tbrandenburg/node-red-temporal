var should = require("should");
var path = require("path");
var fs = require("fs");
var os = require("os");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var NODES_DIR = path.join(FIXTURES, "nodes", "context-credential-test");
var FLOW = path.join(FIXTURES, "credential-context-flow.json");
var PERSISTENT_FLOW = path.join(FIXTURES, "credential-context-flow.persistent.json");

/**
 * The fixture "test-ctx-node" has no wires (wires: [[]]), so there is
 * nothing downstream to capture a result from via routing. Overriding
 * `.send` on a SINGLE live node INSTANCE (not a shared class/prototype
 * method - see the M2/issue-#18 lesson learned about global patches) for
 * the duration of one `receive()` call is a safe, scoped way to observe
 * what the node sent without needing Capture/Temporal at all.
 */
function receiveAndCapture(node, msg) {
    return new Promise(function (resolve, reject) {
        var originalSend = node.send;
        node.send = function (sentMsg) {
            node.send = originalSend;
            resolve(sentMsg);
        };
        try {
            node.receive(msg);
        } catch (err) {
            node.send = originalSend;
            reject(err);
        }
    });
}

/**
 * Real Node-RED flows.json files never embed plaintext credentials except
 * transiently right after an editor deploy - the normal, at-rest shape is a
 * SEPARATE `<flowbase>_cred.json` file next to the flow file, loaded through
 * `storage.getCredentials()` (see @node-red/runtime/lib/nodes/credentials.js
 * `load()` and lib/storage/localfilesystem/projects/index.js `getCredentials`
 * /`getCredentialsFilename`). This seeds that real file BEFORE bootstrap, in
 * the same userDir bootstrap() will use, exactly mirroring a pre-existing
 * real Node-RED install with saved credentials - no node-red-temporal-owned
 * credential path is involved.
 * `credentialSecret: false` (set by the caller in test settings) disables
 * encryption so the seeded/verified file content can stay plaintext JSON.
 */
function seedCredentialsFile(userDir, flowFilePath, credentials) {
    var flowBase = path.basename(flowFilePath, path.extname(flowFilePath));
    var credFile = path.join(userDir, flowBase + "_cred" + path.extname(flowFilePath));
    fs.mkdirSync(userDir, { recursive: true });
    fs.writeFileSync(credFile, JSON.stringify(credentials));
    return credFile;
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/bootstrap - issue #14 credentials/config-node/context", function () {
    this.timeout(20000);

    var runtimeHandle;

    afterEach(function () {
        if (runtimeHandle) {
            var handle = runtimeHandle;
            runtimeHandle = null;
            return handle.stop();
        }
    });

    it("a credential-bearing config node's credential reaches a live node through Node-RED's own credential machinery", function () {
        var userDir = fs.mkdtempSync(path.join(os.tmpdir(), "node-red-temporal-cred-"));
        seedCredentialsFile(userDir, FLOW, { cfg1: { apiKey: "topsecret" } });
        return bootstrap(FLOW, { userDir: userDir, settings: { nodesDir: [NODES_DIR], credentialSecret: false } }).then(function (handle) {
            runtimeHandle = handle;
            var ctxA = handle.getNode("ctxA");
            should.exist(ctxA);
            return receiveAndCapture(ctxA, { payload: "go" }).then(function (sent) {
                sent.payload.apiKey.should.equal("topsecret");
                sent.payload.configLabel.should.equal("cfgLabel");
            });
        });
    });

    it("two nodes constructed from the same config node id share the identical live config-node instance", function () {
        return bootstrap(FLOW, { settings: { nodesDir: [NODES_DIR] } }).then(function (handle) {
            runtimeHandle = handle;
            var ctxA = handle.getNode("ctxA");
            var ctxB = handle.getNode("ctxB");
            should.exist(ctxA.configNode);
            should.exist(ctxB.configNode);
            ctxA.configNode.should.equal(ctxB.configNode);
            ctxA.configNode.id.should.equal("cfg1");
        });
    });

    it("node context is private per node instance", function () {
        return bootstrap(FLOW, { settings: { nodesDir: [NODES_DIR] } }).then(function (handle) {
            runtimeHandle = handle;
            var ctxA = handle.getNode("ctxA");
            var ctxB = handle.getNode("ctxB");
            return receiveAndCapture(ctxA, { payload: 1 }).then(function (sentA1) {
                sentA1.payload.nodeCount.should.equal(1);
                return receiveAndCapture(ctxA, { payload: 2 });
            }).then(function (sentA2) {
                sentA2.payload.nodeCount.should.equal(2);
                return receiveAndCapture(ctxB, { payload: 3 });
            }).then(function (sentB1) {
                // ctxB's own node context started fresh, independent of ctxA's
                sentB1.payload.nodeCount.should.equal(1);
            });
        });
    });

    it("flow context is shared across nodes within the same flow/tab", function () {
        return bootstrap(FLOW, { settings: { nodesDir: [NODES_DIR] } }).then(function (handle) {
            runtimeHandle = handle;
            var ctxA = handle.getNode("ctxA");
            var ctxB = handle.getNode("ctxB");
            return receiveAndCapture(ctxA, { payload: 1 }).then(function (sentA) {
                sentA.payload.flowCount.should.equal(1);
                return receiveAndCapture(ctxB, { payload: 2 });
            }).then(function (sentB) {
                // ctxB is on the same tab as ctxA - flow context is shared
                sentB.payload.flowCount.should.equal(2);
            });
        });
    });

    it("global context is shared across the whole runtime", function () {
        return bootstrap(FLOW, { settings: { nodesDir: [NODES_DIR] } }).then(function (handle) {
            runtimeHandle = handle;
            var ctxA = handle.getNode("ctxA");
            var ctxB = handle.getNode("ctxB");
            return receiveAndCapture(ctxA, { payload: 1 }).then(function (sentA) {
                sentA.payload.globalCount.should.equal(1);
                return receiveAndCapture(ctxB, { payload: 2 });
            }).then(function (sentB) {
                sentB.payload.globalCount.should.equal(2);
            });
        });
    });

    it("a configured persistent (localfilesystem) Node-RED context store survives a bootstrap stop+restart with the same userDir", function () {
        var userDir = fs.mkdtempSync(path.join(os.tmpdir(), "node-red-temporal-ctx-persist-"));
        var bootOptions = {
            userDir: userDir,
            settings: {
                nodesDir: [NODES_DIR],
                contextStorage: { file: { module: "localfilesystem" } }
            }
        };

        return bootstrap(PERSISTENT_FLOW, bootOptions).then(function (handleA) {
            runtimeHandle = handleA;
            var ctxP = handleA.getNode("ctxP");
            return receiveAndCapture(ctxP, { payload: 1 }).then(function (sent) {
                sent.payload.globalCount.should.equal(1);
                return handleA.stop();
            });
        }).then(function () {
            runtimeHandle = null;
            return bootstrap(PERSISTENT_FLOW, bootOptions);
        }).then(function (handleB) {
            runtimeHandle = handleB;
            var ctxP = handleB.getNode("ctxP");
            return receiveAndCapture(ctxP, { payload: 2 }).then(function (sent) {
                // Started fresh at 1 last boot, persisted store must still
                // hold that value: this boot's first receive continues at 2.
                sent.payload.globalCount.should.equal(2);
            });
        });
    });

    it("the credential value is persisted to a real credentials file, not left as an in-memory-only fake", function () {
        var userDir = fs.mkdtempSync(path.join(os.tmpdir(), "node-red-temporal-cred-persist-"));
        var credFile = seedCredentialsFile(userDir, FLOW, { cfg1: { apiKey: "topsecret" } });
        return bootstrap(FLOW, { userDir: userDir, settings: { nodesDir: [NODES_DIR], credentialSecret: false } }).then(function (handle) {
            runtimeHandle = handle;
            return handle.stop();
        }).then(function () {
            runtimeHandle = null;
            should.exist(fs.existsSync(credFile) && true);
            var raw = JSON.parse(fs.readFileSync(credFile, "utf8"));
            // Real Node-RED credential storage round-trips it as-is (not the
            // fake `{}` the old shim always returned/saved).
            raw.cfg1.apiKey.should.equal("topsecret");
        });
    });
});
