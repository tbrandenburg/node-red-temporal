var should = require("should");
var fs = require("fs");
var os = require("os");
var path = require("path");
var { execFileSync } = require("child_process");

var SCRIPT = path.join(
    __dirname, "..", "..", "..", "..", "..",
    "packages", "node_modules", "@tbrandenburg", "node-red-temporal-runtime",
    "run", "generate-editor-settings.js"
);

describe("@tbrandenburg/node-red-temporal-runtime/run/generate-editor-settings", function() {
    var userDir;

    beforeEach(function() {
        userDir = fs.mkdtempSync(path.join(os.tmpdir(), "editor-settings-spec-"));
    });

    afterEach(function() {
        fs.rmSync(userDir, { recursive: true, force: true });
    });

    it("prints usage and exits non-zero when required args are missing", function() {
        should(function() {
            execFileSync(process.execPath, [SCRIPT, "--user-dir", userDir], { stdio: "pipe" });
        }).throw();
    });

    it("writes a settings.js wiring createRemoteDeployStorage at the given target/port", function() {
        execFileSync(process.execPath, [
            SCRIPT, "--user-dir", userDir, "--target", "http://127.0.0.1:18811", "--port", "18800"
        ], { stdio: "pipe" });

        var settingsPath = path.join(userDir, "settings.js");
        fs.existsSync(settingsPath).should.be.true();

        var settings = require(settingsPath);
        settings.uiPort.should.equal("18800");
        settings.runtimeFlowState.should.equal("stop");
        settings.runtimeState.should.eql({ enabled: false, ui: false });
        should(settings.credentialSecret).be.a.String();
        settings.credentialSecret.length.should.be.above(0);
        should(settings.storageModule).be.an.Object();
    });

    it("also wires issues #59/#61 by default: httpAdminMiddleware proxy and a started events receiver", function() {
        execFileSync(process.execPath, [
            SCRIPT, "--user-dir", userDir, "--target", "http://127.0.0.1:18811", "--port", "18800"
        ], { stdio: "pipe" });

        var settingsPath = path.join(userDir, "settings.js");
        var content = fs.readFileSync(settingsPath, "utf8");

        // issue #59: the events receiver must be started at settings-load time,
        // not left for the caller to remember to wire in by hand.
        content.should.containEql("createRuntimeEventsReceiverForTarget");
        content.should.containEql(".start()");

        // issue #61: httpAdminMiddleware must proxy runtime-affine Admin
        // actions to the same runner B target, reusing the SAME remoteRunner
        // object as createRemoteDeployStorage (no second target/token config).
        content.should.containEql("createRemoteRuntimeAdminProxy");
        content.should.containEql("httpAdminMiddleware");

        var settings = require(settingsPath);
        settings.httpAdminMiddleware.should.be.an.Array();
        settings.httpAdminMiddleware.length.should.equal(1);
        settings.httpAdminMiddleware[0].should.be.a.Function();
    });

    it("persists the credentialSecret across repeated runs (restart-safe)", function() {
        execFileSync(process.execPath, [
            SCRIPT, "--user-dir", userDir, "--target", "http://127.0.0.1:18811"
        ], { stdio: "pipe" });
        var firstSecret = require(path.join(userDir, "settings.js")).credentialSecret;

        delete require.cache[require.resolve(path.join(userDir, "settings.js"))];

        execFileSync(process.execPath, [
            SCRIPT, "--user-dir", userDir, "--target", "http://127.0.0.1:18811"
        ], { stdio: "pipe" });
        var secondSecret = require(path.join(userDir, "settings.js")).credentialSecret;

        secondSecret.should.equal(firstSecret);
    });

    it("regenerates settings.js with a new target when re-run with a different --target", function() {
        execFileSync(process.execPath, [
            SCRIPT, "--user-dir", userDir, "--target", "http://127.0.0.1:18811"
        ], { stdio: "pipe" });

        execFileSync(process.execPath, [
            SCRIPT, "--user-dir", userDir, "--target", "http://127.0.0.1:19999"
        ], { stdio: "pipe" });

        var content = fs.readFileSync(path.join(userDir, "settings.js"), "utf8");
        content.should.containEql("http://127.0.0.1:19999");
        content.should.not.containEql("http://127.0.0.1:18811");
    });
});
