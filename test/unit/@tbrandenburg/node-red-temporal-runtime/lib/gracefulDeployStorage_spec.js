var should = require("should");
var sinon = require("sinon");
var { createGracefulRemoteDeployStorage } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/gracefulDeployStorage.js");
var { createGenerationManager } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/generationManager.js");

function makeDelegate(overrides) {
    return Object.assign({
        init: sinon.stub().resolves(),
        getFlows: sinon.stub().resolves({ flows: [] }),
        saveFlows: sinon.stub().resolves(),
        getCredentials: sinon.stub().resolves({ node1: { secret: "encrypted-blob" } }),
        saveCredentials: sinon.stub().resolves(),
        getSettings: sinon.stub().resolves({}),
        saveSettings: sinon.stub().resolves()
    }, overrides || {});
}

function okResponse() {
    return { ok: true, status: 200, text: sinon.stub().resolves("") };
}

function genB(overrides) {
    return Object.assign({
        id: "gen-b",
        flowVersion: "hash-b",
        activityTaskQueue: "queue-b",
        target: "http://localhost:1882"
    }, overrides || {});
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/gracefulDeployStorage", function() {
    var flows = [{ id: "n1", type: "inject" }];

    it("throws without options.buildGeneration/readinessCheck", function() {
        (function() {
            createGracefulRemoteDeployStorage(makeDelegate(), { readinessCheck: async function() {} });
        }).should.throw(/buildGeneration/);

        (function() {
            createGracefulRemoteDeployStorage(makeDelegate(), { buildGeneration: async function() { return genB(); } });
        }).should.throw(/readinessCheck/);
    });

    it("saves locally, stages into B via a full v2 POST /flows, and promotes only after readiness succeeds", async function() {
        var delegate = makeDelegate();
        var fetchStub = sinon.stub().resolves(okResponse());
        var buildGeneration = sinon.stub().resolves(genB());
        var readinessCheck = sinon.stub().resolves();

        var storage = createGracefulRemoteDeployStorage(delegate, {
            buildGeneration,
            readinessCheck,
            fetch: fetchStub
        });

        await storage.saveFlows(flows, { username: "alice" });

        delegate.saveFlows.calledOnceWith(flows, { username: "alice" }).should.be.true();
        delegate.getCredentials.calledOnce.should.be.true();
        buildGeneration.calledOnceWith(flows, { node1: { secret: "encrypted-blob" } }).should.be.true();

        fetchStub.calledOnce.should.be.true();
        var [url, init] = fetchStub.firstCall.args;
        url.should.equal("http://localhost:1882/flows");
        init.headers["Node-RED-Deployment-Type"].should.equal("full");
        JSON.parse(init.body).should.not.have.property("rev");

        readinessCheck.calledOnceWith(genB()).should.be.true();

        var manager = storage.getGenerationManager();
        manager.getActive().id.should.equal("gen-b");
    });

    it("rejects and never promotes when staging (the remote POST) fails", async function() {
        var delegate = makeDelegate();
        var fetchStub = sinon.stub().resolves({ ok: false, status: 502, text: sinon.stub().resolves("bad gateway") });
        var readinessCheck = sinon.stub().resolves();
        var manager = createGenerationManager();

        var storage = createGracefulRemoteDeployStorage(delegate, {
            buildGeneration: async function() { return genB(); },
            readinessCheck,
            manager,
            fetch: fetchStub
        });

        await storage.saveFlows(flows).should.be.rejectedWith(/502/);

        readinessCheck.called.should.be.false();
        should.equal(manager.getActive(), null);
    });

    it("rejects and never promotes when readinessCheck fails, even though staging succeeded", async function() {
        var delegate = makeDelegate();
        var fetchStub = sinon.stub().resolves(okResponse());
        var readinessCheck = sinon.stub().rejects(new Error("flowVersion mismatch"));
        var manager = createGenerationManager();

        var storage = createGracefulRemoteDeployStorage(delegate, {
            buildGeneration: async function() { return genB(); },
            readinessCheck,
            manager,
            fetch: fetchStub
        });

        await storage.saveFlows(flows).should.be.rejectedWith(/flowVersion mismatch/);

        should.equal(manager.getActive(), null);
    });

    it("keeps the previously active generation (A) authoritative when promoting B fails", async function() {
        var manager = createGenerationManager();
        await manager.promote(
            { id: "gen-a", flowVersion: "hash-a", activityTaskQueue: "queue-a", target: "http://localhost:1881" },
            async function() {}
        );

        var delegate = makeDelegate();
        var fetchStub = sinon.stub().resolves(okResponse());
        var storage = createGracefulRemoteDeployStorage(delegate, {
            buildGeneration: async function() { return genB(); },
            readinessCheck: sinon.stub().rejects(new Error("B never came up")),
            manager,
            fetch: fetchStub
        });

        await storage.saveFlows(flows).should.be.rejectedWith(/B never came up/);

        manager.getActive().id.should.equal("gen-a");
        manager.listDraining().should.eql([]);
    });

    it("never forwards a rev and forwards the opaque credential bundle byte-for-byte during staging", async function() {
        var opaqueCredentials = { nodeA: { $: "abcdef0123456789encrypted" } };
        var delegate = makeDelegate({ getCredentials: sinon.stub().resolves(opaqueCredentials) });
        var fetchStub = sinon.stub().resolves(okResponse());

        var storage = createGracefulRemoteDeployStorage(delegate, {
            buildGeneration: async function() { return genB(); },
            readinessCheck: sinon.stub().resolves(),
            fetch: fetchStub
        });

        await storage.saveFlows(flows);

        var body = JSON.parse(fetchStub.firstCall.args[1].body);
        body.credentials.should.eql(opaqueCredentials);
        body.should.not.have.property("rev");
    });
});
