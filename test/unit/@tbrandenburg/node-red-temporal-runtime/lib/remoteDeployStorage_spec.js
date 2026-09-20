var should = require("should");
var sinon = require("sinon");
var EventEmitter = require("events");
var { createRemoteDeployStorage, createRuntimeEventsReceiverForTarget } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/remoteDeployStorage.js");

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

describe("@tbrandenburg/node-red-temporal-runtime/lib/remoteDeployStorage", function() {
    describe("createRemoteDeployStorage()", function() {
        it("throws without a delegate implementing saveFlows()/getCredentials()", function() {
            (function() {
                createRemoteDeployStorage(null, { target: "http://localhost:1881" });
            }).should.throw(/delegate storageModule/);

            (function() {
                createRemoteDeployStorage({}, { target: "http://localhost:1881" });
            }).should.throw(/delegate storageModule/);
        });

        it("throws without options.target", function() {
            (function() {
                createRemoteDeployStorage(makeDelegate(), {});
            }).should.throw(/options\.target/);
        });

        it("throws when no fetch implementation is available at all", function() {
            var originalFetch = global.fetch;
            delete global.fetch;
            try {
                (function() {
                    createRemoteDeployStorage(makeDelegate(), { target: "http://localhost:1881" });
                }).should.throw(/fetch implementation/);
            } finally {
                global.fetch = originalFetch;
            }
        });

        it("preserves all delegate methods other than saveFlows unchanged", function() {
            var delegate = makeDelegate();
            var fetchStub = sinon.stub().resolves(okResponse());
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881", fetch: fetchStub });

            adapter.init.should.equal(delegate.init);
            adapter.getFlows.should.equal(delegate.getFlows);
            adapter.saveCredentials.should.equal(delegate.saveCredentials);
            adapter.getCredentials.should.equal(delegate.getCredentials);
            adapter.getSettings.should.equal(delegate.getSettings);
            adapter.saveFlows.should.not.equal(delegate.saveFlows);
        });
    });

    describe("saveFlows()", function() {
        var flows = [{ id: "n1", type: "inject" }];

        it("delegates locally first, then reads current credentials and POSTs a v2 full deploy without a rev", async function() {
            var delegate = makeDelegate();
            var fetchStub = sinon.stub().resolves(okResponse());
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881/", fetch: fetchStub });

            await adapter.saveFlows(flows, { username: "alice" });

            delegate.saveFlows.calledOnceWith(flows, { username: "alice" }).should.be.true();
            delegate.getCredentials.calledOnce.should.be.true();

            fetchStub.calledOnce.should.be.true();
            var [url, init] = fetchStub.firstCall.args;
            url.should.equal("http://localhost:1881/flows");
            init.method.should.equal("POST");
            init.headers["Node-RED-API-Version"].should.equal("v2");
            init.headers["Node-RED-Deployment-Type"].should.equal("full");
            should.not.exist(init.headers.Authorization);

            var body = JSON.parse(init.body);
            body.flows.should.eql(flows);
            body.credentials.should.eql({ node1: { secret: "encrypted-blob" } });
            body.should.not.have.property("rev");
        });

        it("forwards a configured bearer token as Authorization", async function() {
            var delegate = makeDelegate();
            var fetchStub = sinon.stub().resolves(okResponse());
            var adapter = createRemoteDeployStorage(delegate, {
                target: "http://localhost:1881",
                token: "runner-admin-token",
                fetch: fetchStub
            });

            await adapter.saveFlows(flows);

            var init = fetchStub.firstCall.args[1];
            init.headers.Authorization.should.equal("Bearer runner-admin-token");
        });

        it("rejects when the local delegate save fails, and never attempts a remote deploy", async function() {
            var delegate = makeDelegate({ saveFlows: sinon.stub().rejects(new Error("disk full")) });
            var fetchStub = sinon.stub().resolves(okResponse());
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881", fetch: fetchStub });

            await adapter.saveFlows(flows).should.be.rejectedWith(/disk full/);
            fetchStub.called.should.be.false();
        });

        it("rejects when the remote runner returns a non-2xx response, even though local save already succeeded", async function() {
            var delegate = makeDelegate();
            var fetchStub = sinon.stub().resolves({ ok: false, status: 502, text: sinon.stub().resolves("bad gateway") });
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881", fetch: fetchStub });

            await adapter.saveFlows(flows).should.be.rejectedWith(/502/);
            delegate.saveFlows.calledOnce.should.be.true();
        });

        it("rejects when the remote runner is unreachable (fetch throws/rejects)", async function() {
            var delegate = makeDelegate();
            var fetchStub = sinon.stub().rejects(new Error("ECONNREFUSED"));
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881", fetch: fetchStub });

            await adapter.saveFlows(flows).should.be.rejectedWith(/ECONNREFUSED/);
        });

        it("never inspects or transforms the credential bundle - forwards it byte-for-byte", async function() {
            var opaqueCredentials = { nodeA: { $: "abcdef0123456789encrypted" } };
            var delegate = makeDelegate({ getCredentials: sinon.stub().resolves(opaqueCredentials) });
            var fetchStub = sinon.stub().resolves(okResponse());
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881", fetch: fetchStub });

            await adapter.saveFlows(flows);

            var body = JSON.parse(fetchStub.firstCall.args[1].body);
            body.credentials.should.eql(opaqueCredentials);
        });

        it("re-reads getCredentials() on every deploy even when credentials were not dirty for this deploy", async function() {
            var delegate = makeDelegate();
            var fetchStub = sinon.stub().resolves(okResponse());
            var adapter = createRemoteDeployStorage(delegate, { target: "http://localhost:1881", fetch: fetchStub });

            await adapter.saveFlows(flows);
            await adapter.saveFlows(flows);

            delegate.getCredentials.callCount.should.equal(2);
            delegate.saveCredentials.called.should.be.false();
        });
    });

    describe("createRuntimeEventsReceiverForTarget() (issue #59)", function() {
        it("reuses the same target/token to build a runtime-events receiver, without affecting saveFlows/init behavior", function() {
            var events = new EventEmitter();
            var fetchStub = sinon.stub().returns(new Promise(function() {}));
            var receiver = createRuntimeEventsReceiverForTarget({
                target: "http://localhost:1881",
                token: "s3cr3t",
                events: events,
                fetch: fetchStub,
                log: { warn() {} }
            });

            receiver.start();
            fetchStub.calledOnce.should.be.true();
            var headers = fetchStub.firstCall.args[1].headers;
            headers.Authorization.should.equal("Bearer s3cr3t");
            receiver.stop();
        });

        it("does not require options.events (defaults to @node-red/util's events singleton)", function() {
            var receiver = createRuntimeEventsReceiverForTarget({
                target: "http://localhost:1881",
                fetch: sinon.stub().returns(new Promise(function() {}))
            });
            receiver.should.have.properties(["start", "stop"]);
            receiver.stop();
        });
    });
});
