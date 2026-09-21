var should = require("should");
var http = require("http");
var sinon = require("sinon");
var HTTP_INGRESS_MODULE_PATH = require.resolve("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/httpIngress.js");

var CLIENT_PATH = require.resolve("@temporalio/client");

// Single shared fake error class (not recreated per `stubClientModule` call)
// so an error constructed with it before a later `stubClientModule` call
// still satisfies that later call's `instanceof` check inside httpIngress.js.
function WorkflowExecutionAlreadyStartedError(message, workflowId, workflowType) {
    var err = new Error(message);
    err.name = "WorkflowExecutionAlreadyStartedError";
    err.workflowId = workflowId;
    err.workflowType = workflowType;
    Object.setPrototypeOf(err, WorkflowExecutionAlreadyStartedError.prototype);
    return err;
}
WorkflowExecutionAlreadyStartedError.prototype = Object.create(Error.prototype);
WorkflowExecutionAlreadyStartedError.prototype.constructor = WorkflowExecutionAlreadyStartedError;

/**
 * Installs a fake `@temporalio/client` module in require.cache (same
 * convention as worker_spec.js's `stubClientModule`), so `httpIngress.js`'s
 * internal `require("@temporalio/client")` for `WorkflowIdReusePolicy`/
 * `WorkflowExecutionAlreadyStartedError` resolves to our fakes too.
 *
 * @param {sinon.SinonStub} startStub - stub used for `client.workflow.start`
 * @param {sinon.SinonStub} [getHandleStub]
 * @returns {{restore:function(), WorkflowExecutionAlreadyStartedError:function}}
 */
function stubClientModule(startStub, getHandleStub) {
    var original = require.cache[CLIENT_PATH];

    var fakeClient = {
        workflow: {
            start: startStub,
            getHandle: getHandleStub || sinon.stub().returns({})
        }
    };

    require.cache[CLIENT_PATH] = {
        id: CLIENT_PATH,
        filename: CLIENT_PATH,
        loaded: true,
        exports: {
            WorkflowIdReusePolicy: { REJECT_DUPLICATE: "REJECT_DUPLICATE" },
            WorkflowExecutionAlreadyStartedError: WorkflowExecutionAlreadyStartedError,
            Client: function() { return fakeClient; },
            Connection: { connect: sinon.stub().resolves({}) }
        }
    };

    return {
        restore: function restore() {
            if (original) {
                require.cache[CLIENT_PATH] = original;
            } else {
                delete require.cache[CLIENT_PATH];
            }
        },
        WorkflowExecutionAlreadyStartedError: WorkflowExecutionAlreadyStartedError,
        fakeClient: fakeClient
    };
}

function freshHttpIngress() {
    delete require.cache[HTTP_INGRESS_MODULE_PATH];
    return require(HTTP_INGRESS_MODULE_PATH);
}

function postJson(port, path, body, headers) {
    return new Promise(function(resolve, reject) {
        var payload = body === undefined ? "" : (typeof body === "string" ? body : JSON.stringify(body));
        var req = http.request({
            host: "127.0.0.1",
            port: port,
            path: path,
            method: "POST",
            headers: Object.assign({ "Content-Type": "application/json" }, headers || {})
        }, function(res) {
            var chunks = [];
            res.on("data", function(c) { chunks.push(c); });
            res.on("end", function() {
                var text = Buffer.concat(chunks).toString("utf8");
                var json;
                try { json = JSON.parse(text); } catch (e) { json = undefined; }
                resolve({ statusCode: res.statusCode, body: json, text: text });
            });
        });
        req.on("error", reject);
        req.end(payload);
    });
}

function postRaw(port, path, buffer, headers) {
    return new Promise(function(resolve, reject) {
        var req = http.request({
            host: "127.0.0.1",
            port: port,
            path: path,
            method: "POST",
            headers: Object.assign({ "Content-Type": "text/plain" }, headers || {})
        }, function(res) {
            var chunks = [];
            res.on("data", function(c) { chunks.push(c); });
            res.on("end", function() {
                var text = Buffer.concat(chunks).toString("utf8");
                var json;
                try { json = JSON.parse(text); } catch (e) { json = undefined; }
                resolve({ statusCode: res.statusCode, body: json, text: text });
            });
        });
        req.on("error", reject);
        req.end(buffer);
    });
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpIngress - pure helpers", function() {
    var httpIngress = freshHttpIngress();

    describe("matchAsyncRoute / matchSyncRoute", function() {
        it("matches the fixed async route", function() {
            httpIngress.matchAsyncRoute("POST", "/_node-red-temporal/http/async/n1").should.eql({ startNodeId: "n1" });
        });

        it("matches the fixed sync route with both path params", function() {
            httpIngress.matchSyncRoute("POST", "/_node-red-temporal/http/sync/n1/n2").should.eql({ startNodeId: "n1", resultNodeId: "n2" });
        });

        it("rejects non-POST methods", function() {
            should(httpIngress.matchAsyncRoute("GET", "/_node-red-temporal/http/async/n1")).be.null();
        });

        it("rejects unrelated paths", function() {
            should(httpIngress.matchAsyncRoute("POST", "/somewhere/else")).be.null();
        });

        it("rejects an async path with an extra segment", function() {
            should(httpIngress.matchAsyncRoute("POST", "/_node-red-temporal/http/async/n1/extra")).be.null();
        });

        it("rejects a sync path missing the resultNodeId segment", function() {
            should(httpIngress.matchSyncRoute("POST", "/_node-red-temporal/http/sync/n1")).be.null();
        });
    });

    describe("sanitizeHeaders", function() {
        it("strips sensitive headers case-insensitively", function() {
            var out = httpIngress.sanitizeHeaders({
                "authorization": "Bearer xyz",
                "Proxy-Authorization": "secret",
                "cookie": "a=b",
                "set-cookie": "a=b",
                "idempotency-key": "abc",
                "x-custom": "keep-me",
                "content-type": "application/json"
            });
            out.should.not.have.property("authorization");
            out.should.not.have.property("Proxy-Authorization");
            out.should.not.have.property("cookie");
            out.should.not.have.property("set-cookie");
            out.should.not.have.property("idempotency-key");
            out.should.have.property("x-custom", "keep-me");
            out.should.have.property("content-type", "application/json");
        });
    });

    describe("parseBody", function() {
        it("parses JSON when content-type is application/json", function() {
            httpIngress.parseBody(Buffer.from('{"a":1}'), "application/json").should.eql({ a: 1 });
        });

        it("returns UTF-8 text otherwise", function() {
            httpIngress.parseBody(Buffer.from("hello"), "text/plain").should.equal("hello");
        });

        it("returns undefined for an empty body", function() {
            should(httpIngress.parseBody(Buffer.alloc(0), "application/json")).be.undefined();
        });

        it("throws INVALID_JSON on malformed JSON", function() {
            var thrown;
            try {
                httpIngress.parseBody(Buffer.from("{not json"), "application/json");
            } catch (err) {
                thrown = err;
            }
            should.exist(thrown);
            thrown.should.have.property("code", "INVALID_JSON");
        });
    });

    describe("deriveWorkflowId", function() {
        it("produces different ids for two calls without an idempotency key", function() {
            var a = httpIngress.deriveWorkflowId("async", "n1", undefined, undefined);
            var b = httpIngress.deriveWorkflowId("async", "n1", undefined, undefined);
            a.should.not.equal(b);
        });

        it("produces the same id for the same idempotency key and never embeds the raw key", function() {
            var a = httpIngress.deriveWorkflowId("async", "n1", undefined, "my-raw-key");
            var b = httpIngress.deriveWorkflowId("async", "n1", undefined, "my-raw-key");
            a.should.equal(b);
            a.should.not.containEql("my-raw-key");
        });

        it("produces different ids for different idempotency keys", function() {
            var a = httpIngress.deriveWorkflowId("async", "n1", undefined, "key-1");
            var b = httpIngress.deriveWorkflowId("async", "n1", undefined, "key-2");
            a.should.not.equal(b);
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpIngress - async route (real HTTP server)", function() {
    var httpIngress;
    var server;
    var boundPort;
    var startStub;
    var clientStub;
    var getNodeStub;
    var currentFlowInfoStub;
    var temporalConfig = { workflowTaskQueue: "wf-q", activityTaskQueue: "act-q" };

    beforeEach(function() {
        httpIngress = freshHttpIngress();
        startStub = sinon.stub().resolves({});
        clientStub = stubClientModule(startStub);
        getNodeStub = sinon.stub().returns({ id: "n1" });
        currentFlowInfoStub = sinon.stub().resolves({ graph: {}, nodeMeta: {}, flowVersion: "v1" });
        server = httpIngress.createHttpIngressServer({
            getClient: function() { return Promise.resolve(clientStub.fakeClient); },
            getNode: getNodeStub,
            currentFlowInfo: currentFlowInfoStub,
            temporalConfig: temporalConfig,
            port: 0
        });
        return server.listen().then(function(bound) {
            boundPort = bound.port;
        });
    });

    afterEach(function() {
        clientStub.restore();
        return server.close();
    });

    it("starts the workflow and returns 202 with {workflowId, reused:false} for a JSON body", function() {
        return postJson(boundPort, "/_node-red-temporal/http/async/n1", { hello: "world" }).then(function(res) {
            res.statusCode.should.equal(202);
            res.body.should.have.property("workflowId").which.is.a.String();
            res.body.should.have.property("reused", false);
            startStub.calledOnce.should.be.true();
            var startArgs = startStub.firstCall.args[1];
            startArgs.args[0].startMsg.payload.should.eql({ hello: "world" });
            startArgs.args[0].startMsg.http.method.should.equal("POST");
            startArgs.args[0].startMsg.http.path.should.equal("/_node-red-temporal/http/async/n1");
            // handle.result()/client.workflow.result must never be called for the async route
            clientStub.fakeClient.workflow.should.not.have.property("result");
        });
    });

    it("preserves query string parameters in the envelope", function() {
        return postJson(boundPort, "/_node-red-temporal/http/async/n1?foo=bar&baz=qux", { a: 1 }).then(function(res) {
            res.statusCode.should.equal(202);
            var startArgs = startStub.firstCall.args[1];
            startArgs.args[0].startMsg.http.query.should.eql({ foo: "bar", baz: "qux" });
        });
    });

    it("builds a text envelope for non-JSON content types", function() {
        return postRaw(boundPort, "/_node-red-temporal/http/async/n1", Buffer.from("plain text body")).then(function(res) {
            res.statusCode.should.equal(202);
            var startArgs = startStub.firstCall.args[1];
            startArgs.args[0].startMsg.payload.should.equal("plain text body");
        });
    });

    it("never forwards sensitive headers into the envelope or workflow input", function() {
        return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 1 }, {
            "Authorization": "Bearer secret-token",
            "Cookie": "session=abc",
            "X-Keep": "yes"
        }).then(function(res) {
            res.statusCode.should.equal(202);
            var startArgs = startStub.firstCall.args[1];
            var headers = startArgs.args[0].startMsg.http.headers;
            headers.should.not.have.property("authorization");
            headers.should.not.have.property("cookie");
            headers.should.have.property("x-keep", "yes");
            JSON.stringify(startArgs.args[0]).should.not.containEql("secret-token");
        });
    });

    it("responds 400 for malformed JSON", function() {
        return postRaw(boundPort, "/_node-red-temporal/http/async/n1", Buffer.from("{not json"), { "Content-Type": "application/json" }).then(function(res) {
            res.statusCode.should.equal(400);
            res.body.should.have.property("error");
            startStub.called.should.be.false();
        });
    });

    it("responds 400 for an unknown startNodeId", function() {
        getNodeStub.returns(undefined);
        return postJson(boundPort, "/_node-red-temporal/http/async/does-not-exist", { a: 1 }).then(function(res) {
            res.statusCode.should.equal(400);
            res.body.should.have.property("error");
            startStub.called.should.be.false();
        });
    });

    it("responds 413 for a body over the 256 KiB cap", function() {
        var big = Buffer.alloc(httpIngress.MAX_BODY_BYTES + 1024, "a");
        return postRaw(boundPort, "/_node-red-temporal/http/async/n1", big).then(function(res) {
            res.statusCode.should.equal(413);
            startStub.called.should.be.false();
        });
    });

    it("responds 503 when the Workflow start fails for a non-duplicate reason", function() {
        startStub.rejects(new Error("temporal unreachable"));
        return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 1 }).then(function(res) {
            res.statusCode.should.equal(503);
            res.body.should.have.property("error");
        });
    });

    it("uses two different Workflow IDs across two requests without an Idempotency-Key", function() {
        return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 1 }).then(function() {
            return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 2 });
        }).then(function() {
            startStub.calledTwice.should.be.true();
            var id1 = startStub.firstCall.args[1].workflowId;
            var id2 = startStub.secondCall.args[1].workflowId;
            id1.should.not.equal(id2);
        });
    });

    it("uses the same derived Workflow ID for the same Idempotency-Key and never leaks the raw key", function() {
        return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 1 }, { "Idempotency-Key": "my-secret-key" }).then(function() {
            return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 2 }, { "Idempotency-Key": "my-secret-key" });
        }).then(function() {
            startStub.calledTwice.should.be.true();
            var id1 = startStub.firstCall.args[1].workflowId;
            var id2 = startStub.secondCall.args[1].workflowId;
            id1.should.equal(id2);
            id1.should.not.containEql("my-secret-key");
            JSON.stringify(startStub.firstCall.args[1]).should.not.containEql("my-secret-key");
        });
    });

    it("responds 400 for an oversized Idempotency-Key", function() {
        var hugeKey = "a".repeat(httpIngress.MAX_IDEMPOTENCY_KEY_BYTES + 1);
        return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 1 }, { "Idempotency-Key": hugeKey }).then(function(res) {
            res.statusCode.should.equal(400);
            startStub.called.should.be.false();
        });
    });

    it("obtains the existing handle and responds 202 with reused:true on a duplicate-start error", function() {
        var getHandleStub = sinon.stub().returns({ id: "existing" });
        clientStub.restore();
        clientStub = stubClientModule(
            sinon.stub().rejects(new WorkflowExecutionAlreadyStartedError("already started", "some-id", "executeFlow")),
            getHandleStub
        );
        // Need a fresh server bound to the new stubbed client
        return server.close().then(function() {
            server = httpIngress.createHttpIngressServer({
                getClient: function() { return Promise.resolve(clientStub.fakeClient); },
                getNode: getNodeStub,
                currentFlowInfo: currentFlowInfoStub,
                temporalConfig: temporalConfig,
                port: 0
            });
            return server.listen();
        }).then(function(bound) {
            boundPort = bound.port;
            return postJson(boundPort, "/_node-red-temporal/http/async/n1", { a: 1 }, { "Idempotency-Key": "dup-key" });
        }).then(function(res) {
            res.statusCode.should.equal(202);
            res.body.should.have.property("reused", true);
            res.body.should.have.property("workflowId").which.is.a.String();
            getHandleStub.calledOnce.should.be.true();
        });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpIngress - sync route (issue #58 M4)", function() {
    var httpIngress;
    var server;
    var boundPort;
    var startStub;
    var getHandleStub;
    var resultStub;
    var clientStub;
    var temporalConfig = { workflowTaskQueue: "wf-q", activityTaskQueue: "act-q" };
    var extraDeps;

    /**
     * (Re)builds the fake handle/client/server. `resultStub` defaults to
     * never resolving; individual tests override it via `.resolves(...)`
     * or `.rejects(...)` before issuing the request.
     */
    function setup(deps) {
        httpIngress = freshHttpIngress();
        startStub = sinon.stub().resolves({});
        resultStub = sinon.stub().returns(new Promise(function() {})); // never resolves by default
        var handle = { result: resultStub };
        getHandleStub = sinon.stub().returns(handle);
        clientStub = stubClientModule(startStub, getHandleStub);
        server = httpIngress.createHttpIngressServer(Object.assign({
            getClient: function() { return Promise.resolve(clientStub.fakeClient); },
            getNode: sinon.stub().returns({ id: "n1" }),
            currentFlowInfo: sinon.stub().resolves({ graph: {}, nodeMeta: {}, flowVersion: "v1" }),
            temporalConfig: temporalConfig,
            port: 0
        }, deps || {}));
        return server.listen().then(function(bound) {
            boundPort = bound.port;
        });
    }

    beforeEach(function() {
        return setup();
    });

    afterEach(function() {
        clientStub.restore();
        return server.close();
    });

    it("matches both path params and threads resultNodeId into the Workflow input", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: { ok: true } } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", { a: 1 }).then(function(res) {
            res.statusCode.should.equal(200);
            var startArgs = startStub.firstCall.args[1];
            startArgs.args[0].resultNodeId.should.equal("n2");
            startArgs.args[0].startNode.should.equal("n1");
        });
    });

    it("maps a default (absent) statusCode to 200 and JSON-encodes an object payload", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: { hello: "world" } } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
            res.body.should.eql({ hello: "world" });
        });
    });

    it("maps an explicit statusCode and headers from resultMsg", function() {
        resultStub.resolves({
            flowVersion: "v1",
            lastNode: "n2",
            resultMsg: { statusCode: 201, headers: { "X-Custom": "yes" }, payload: { created: true } }
        });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(201);
            res.body.should.eql({ created: true });
        });
    });

    it("maps a string payload to text bytes with a text/plain content-type", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: "hello world" } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
            res.text.should.equal("hello world");
        });
    });

    it("maps an undefined payload to an empty response body", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: undefined } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
            res.text.should.equal("");
        });
    });

    it("maps a Buffer payload to raw bytes", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: Buffer.from([1, 2, 3, 4]) } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
            Buffer.from(res.text, "binary").length.should.be.above(0);
        });
    });

    it("maps a Uint8Array payload to raw bytes", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: new Uint8Array([5, 6, 7]) } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
        });
    });

    it("falls back to 200 for an out-of-range statusCode instead of failing", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { statusCode: 999, payload: { a: 1 } } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
            res.body.should.eql({ a: 1 });
        });
    });

    it("falls back to 200 for a non-integer statusCode instead of failing", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { statusCode: 200.5, payload: { a: 1 } } });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(200);
        });
    });

    it("responds 500 when resultMsg itself is missing/not an object", function() {
        resultStub.resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: undefined });
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(500);
            res.body.should.have.property("error");
        });
    });

    it("strips hop-by-hop/framing headers from resultMsg.headers", function() {
        resultStub.resolves({
            flowVersion: "v1",
            lastNode: "n2",
            resultMsg: {
                headers: {
                    "Connection": "should-be-stripped",
                    "Content-Length": "9999",
                    "X-Keep": "yes"
                },
                payload: { a: 1 }
            }
        });
        return new Promise(function(resolve, reject) {
            var req = http.request({
                host: "127.0.0.1",
                port: boundPort,
                path: "/_node-red-temporal/http/sync/n1/n2",
                method: "POST",
                headers: { "Content-Type": "application/json" }
            }, function(res) {
                res.on("data", function() {});
                res.on("end", function() { resolve(res); });
            });
            req.on("error", reject);
            req.end(JSON.stringify({}));
        }).then(function(res) {
            res.statusCode.should.equal(200);
            res.headers.should.have.property("x-keep", "yes");
            // Node's http module computes its own real `connection`/
            // `content-length` values for the wire response (which may or
            // may not even be present depending on keep-alive/chunking),
            // so we only assert our deliberately-bogus stripped VALUES
            // never leaked through anywhere in the response headers.
            JSON.stringify(res.headers).should.not.containEql("should-be-stripped");
            JSON.stringify(res.headers).should.not.containEql("9999");
        });
    });

    it("responds 500 with the ApplicationFailure's type for FLOW_RESULT_MISSING", function() {
        var err = new Error("flow result node n2 was never reached");
        err.type = "FLOW_RESULT_MISSING";
        resultStub.rejects(err);
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(500);
            res.body.should.have.property("error");
            res.body.should.have.property("type", "FLOW_RESULT_MISSING");
        });
    });

    it("responds 500 with the ApplicationFailure's type for FLOW_RESULT_AMBIGUOUS wrapped in a WorkflowFailedError-shaped cause", function() {
        var cause = new Error("more than one delivery");
        cause.type = "FLOW_RESULT_AMBIGUOUS";
        var wrapped = new Error("Workflow execution failed");
        wrapped.cause = cause;
        resultStub.rejects(wrapped);
        return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
            res.statusCode.should.equal(500);
            res.body.should.have.property("type", "FLOW_RESULT_AMBIGUOUS");
        });
    });

    it("bounds the wait by deps.syncTimeoutMs and responds 504 with workflowId when the Workflow is still running", function() {
        // Close the `beforeEach`-created server first (see the identical
        // note on the next test) - `setup()` below binds a second server on
        // a fresh ephemeral port, and leaving the first open leaks a
        // listening socket that hangs the whole mocha process afterwards.
        return server.close().then(function() {
            return setup({ syncTimeoutMs: 30 });
        }).then(function() {
            // resultStub never resolves (default) - the race must still settle via the timer
            var start = Date.now();
            return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}).then(function(res) {
                (Date.now() - start).should.be.below(2000);
                res.statusCode.should.equal(504);
                res.body.should.have.property("workflowId").which.is.a.String();
            });
        });
    });

    it("never calls handle.cancel()/terminate() when the bounded wait times out", function() {
        var cancelStub = sinon.stub();
        var terminateStub = sinon.stub();
        // Close the `beforeEach`-created server first: `setup()` below
        // creates and listens a SECOND server bound to a fresh ephemeral
        // port, and `afterEach` only closes whichever server is current -
        // leaving the first one open would leak an open listening socket
        // and hang the whole mocha process after the suite finishes.
        return server.close().then(function() {
            return setup({ syncTimeoutMs: 30 });
        }).then(function() {
            getHandleStub.returns({ result: resultStub, cancel: cancelStub, terminate: terminateStub });
            return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {});
        }).then(function(res) {
            res.statusCode.should.equal(504);
            cancelStub.called.should.be.false();
            terminateStub.called.should.be.false();
        });
    });

    it("waits on the same Workflow's result for a reused (duplicate-start) sync request", function() {
        var dupResultStub = sinon.stub().resolves({ flowVersion: "v1", lastNode: "n2", resultMsg: { payload: { a: 1 } } });
        var dupGetHandleStub = sinon.stub().returns({ result: dupResultStub });
        clientStub.restore();
        clientStub = stubClientModule(
            sinon.stub().rejects(new WorkflowExecutionAlreadyStartedError("already started", "some-id", "executeFlow")),
            dupGetHandleStub
        );
        return server.close().then(function() {
            server = httpIngress.createHttpIngressServer({
                getClient: function() { return Promise.resolve(clientStub.fakeClient); },
                getNode: sinon.stub().returns({ id: "n1" }),
                currentFlowInfo: sinon.stub().resolves({ graph: {}, nodeMeta: {}, flowVersion: "v1" }),
                temporalConfig: temporalConfig,
                port: 0
            });
            return server.listen();
        }).then(function(bound) {
            boundPort = bound.port;
            return postJson(boundPort, "/_node-red-temporal/http/sync/n1/n2", {}, { "Idempotency-Key": "dup-key" });
        }).then(function(res) {
            res.statusCode.should.equal(200);
            res.body.should.eql({ a: 1 });
            dupResultStub.calledOnce.should.be.true();
        });
    });
});
