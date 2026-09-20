var should = require("should");
var sinon = require("sinon");
var express = require("express");
var http = require("http");
var {
    createRemoteRuntimeAdminProxy,
    matchRuntimeAdminRoute
} = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/remoteRuntimeAdminProxy.js");

function jsonResponse(status, body, contentType) {
    var text = typeof body === "string" ? body : JSON.stringify(body || {});
    return {
        ok: status >= 200 && status < 300,
        status: status,
        headers: { get: function(name) { return name.toLowerCase() === "content-type" ? (contentType || "application/json") : null; } },
        arrayBuffer: function() { return Promise.resolve(Buffer.from(text)); }
    };
}

function allowMiddleware() {
    return function(req, res, next) { next(); };
}

function denyMiddleware(status) {
    return function(req, res) { res.status(status || 401).end(); };
}

function fakeReqWithBody(method, path, bodyString, extraHeaders) {
    var endListeners = [];
    var req = {
        method: method,
        path: path.split("?")[0],
        originalUrl: path,
        headers: extraHeaders || {},
        get: function(name) { return (extraHeaders || {})[name.toLowerCase()]; },
        on: function(event, cb) {
            // Deliver the (already-buffered) body lazily, on the tick after
            // the consumer actually subscribes - this mirrors readRawBody()
            // attaching its listeners only once it starts reading.
            if (event === "data") {
                if (bodyString) {
                    process.nextTick(function() { cb(Buffer.from(bodyString)); });
                }
            } else if (event === "end") {
                process.nextTick(function() { cb(); });
            }
            return req;
        }
    };
    return req;
}

function fakeRes(onSend) {
    var state = {};
    var res = {
        status: function(code) { state.status = code; return res; },
        set: function(name, value) { state.headers = state.headers || {}; state.headers[name] = value; return res; },
        send: function(body) { state.body = body; onSend(state); return res; },
        json: function(body) { state.status = state.status || 500; state.body = JSON.stringify(body); onSend(state); return res; },
        headersSent: false
    };
    return res;
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/remoteRuntimeAdminProxy", function() {

    describe("matchRuntimeAdminRoute() (pure allow-list matcher)", function() {
        it("accepts manual Inject POST", function() {
            matchRuntimeAdminRoute("POST", "/inject/abc123").should.equal("inject.write");
        });

        it("rejects Inject GET", function() {
            should(matchRuntimeAdminRoute("GET", "/inject/abc123")).be.null();
        });

        it("rejects a similarly-prefixed attacker path", function() {
            should(matchRuntimeAdminRoute("POST", "/injectx")).be.null();
            should(matchRuntimeAdminRoute("POST", "/inject")).be.null();
            should(matchRuntimeAdminRoute("POST", "/inject/")).be.null();
        });

        it("accepts valid Debug control routes", function() {
            matchRuntimeAdminRoute("POST", "/debug/enable").should.equal("debug.write");
            matchRuntimeAdminRoute("POST", "/debug/disable").should.equal("debug.write");
            matchRuntimeAdminRoute("POST", "/debug/abc123/enable").should.equal("debug.write");
            matchRuntimeAdminRoute("POST", "/debug/abc123/disable").should.equal("debug.write");
        });

        it("rejects /debug/view/* (Debug sidebar static assets)", function() {
            should(matchRuntimeAdminRoute("GET", "/debug/view/view.html")).be.null();
            should(matchRuntimeAdminRoute("GET", "/debug/view/debug-utils.js")).be.null();
            should(matchRuntimeAdminRoute("POST", "/debug/view/enable")).be.null();
        });

        it("rejects similarly-prefixed Debug attacker paths", function() {
            should(matchRuntimeAdminRoute("POST", "/debug/enablex")).be.null();
            should(matchRuntimeAdminRoute("POST", "/debug/abc123/enablex")).be.null();
            should(matchRuntimeAdminRoute("GET", "/debug/enable")).be.null();
        });

        it("accepts Context GET and DELETE", function() {
            matchRuntimeAdminRoute("GET", "/context/global").should.equal("context.read");
            matchRuntimeAdminRoute("GET", "/context/node/abc123/foo").should.equal("context.read");
            matchRuntimeAdminRoute("DELETE", "/context/flow/abc123/foo").should.equal("context.write");
        });

        it("rejects Context POST", function() {
            should(matchRuntimeAdminRoute("POST", "/context/global")).be.null();
        });

        it("rejects /flows, /nodes and unknown paths", function() {
            should(matchRuntimeAdminRoute("POST", "/flows")).be.null();
            should(matchRuntimeAdminRoute("GET", "/nodes")).be.null();
            should(matchRuntimeAdminRoute("GET", "/settings")).be.null();
            should(matchRuntimeAdminRoute("GET", "/diagnostics")).be.null();
            should(matchRuntimeAdminRoute("GET", "/plugins")).be.null();
            should(matchRuntimeAdminRoute("GET", "/some-contrib-node-route")).be.null();
        });

        it("is case-insensitive on method and handles empty/invalid input", function() {
            matchRuntimeAdminRoute("post", "/inject/abc123").should.equal("inject.write");
            should(matchRuntimeAdminRoute("GET", "")).be.null();
            should(matchRuntimeAdminRoute("GET", null)).be.null();
        });
    });

    describe("createRemoteRuntimeAdminProxy()", function() {
        it("throws without remoteRunner.target", function() {
            (function() {
                createRemoteRuntimeAdminProxy({});
            }).should.throw(/remoteRunner\.target/);
        });

        it("throws when no fetch implementation is available", function() {
            var originalFetch = global.fetch;
            delete global.fetch;
            try {
                (function() {
                    createRemoteRuntimeAdminProxy({ target: "http://localhost:1881" }, { needsPermission: function() { return allowMiddleware(); } });
                }).should.throw(/fetch implementation/);
            } finally {
                global.fetch = originalFetch;
            }
        });

        it("calls next() for non-allow-listed requests without invoking the permission check or fetch", function(done) {
            var needsPermission = sinon.stub();
            var fetchStub = sinon.stub();
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://localhost:1881" },
                { fetch: fetchStub, needsPermission: needsPermission }
            );
            var req = { method: "GET", path: "/flows", originalUrl: "/flows", get: function() { return undefined; }, on: function() {} };
            var res = {};
            proxy(req, res, function next() {
                needsPermission.called.should.be.false();
                fetchStub.called.should.be.false();
                done();
            });
        });

        it("selects the correct permission per route family and never reaches fetch when unauthorized", function(done) {
            var needsPermission = sinon.stub().returns(denyMiddleware(401));
            var fetchStub = sinon.stub();
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://localhost:1881" },
                { fetch: fetchStub, needsPermission: needsPermission }
            );
            var statusCode;
            var req = { method: "POST", path: "/inject/abc", originalUrl: "/inject/abc", get: function() { return undefined; }, on: function() {} };
            var res = {
                status: function(code) { statusCode = code; return res; },
                end: function() {
                    needsPermission.calledWith("inject.write").should.be.true();
                    fetchStub.called.should.be.false();
                    statusCode.should.equal(401);
                    done();
                }
            };
            proxy(req, res, function next() { throw new Error("next() must not be called when unauthorized"); });
        });

        it("proxies an authorized Inject request preserving method/path/body and returns B's status/body", function(done) {
            var fetchStub = sinon.stub().resolves(jsonResponse(200, "OK", "text/plain"));
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://runner-b:1881", token: "s3cr3t" },
                { fetch: fetchStub, needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("POST", "/inject/abc123", JSON.stringify({ __user_inject_props__: true }), { "content-type": "application/json" });
            var res = fakeRes(function(state) {
                fetchStub.calledOnce.should.be.true();
                var call = fetchStub.firstCall;
                call.args[0].should.equal("http://runner-b:1881/inject/abc123");
                call.args[1].method.should.equal("POST");
                call.args[1].headers.authorization.should.equal("Bearer s3cr3t");
                call.args[1].body.toString().should.equal(JSON.stringify({ __user_inject_props__: true }));
                state.status.should.equal(200);
                done();
            });
            proxy(req, res, function next() { throw new Error("next() must not be called for an authorized allow-listed route"); });
        });

        it("never forwards the browser's inbound Authorization header to B", function(done) {
            var fetchStub = sinon.stub().resolves(jsonResponse(204, ""));
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://runner-b:1881" },
                { fetch: fetchStub, needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("DELETE", "/context/global/foo", "", { authorization: "Bearer browser-A-token" });
            var res = fakeRes(function() {
                var headers = fetchStub.firstCall.args[1].headers;
                should(headers.authorization).be.undefined();
                done();
            });
            proxy(req, res, function next() { throw new Error("must not fall through"); });
        });

        it("preserves query string and Context response body/content-type", function(done) {
            var fetchStub = sinon.stub().resolves(jsonResponse(200, { foo: "bar" }, "application/json"));
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://runner-b:1881" },
                { fetch: fetchStub, needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("GET", "/context/node/abc123?store=file", "");
            var res = fakeRes(function(state) {
                fetchStub.firstCall.args[0].should.equal("http://runner-b:1881/context/node/abc123?store=file");
                state.headers["content-type"].should.equal("application/json");
                state.body.toString().should.equal(JSON.stringify({ foo: "bar" }));
                done();
            });
            proxy(req, res, function next() { throw new Error("must not fall through"); });
        });

        it("preserves a Debug multi-node request body and 404 status", function(done) {
            var fetchStub = sinon.stub().resolves(jsonResponse(404, ""));
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://runner-b:1881" },
                { fetch: fetchStub, needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("POST", "/debug/enable", JSON.stringify({ nodes: ["n1", "n2"] }), { "content-type": "application/json" });
            var res = fakeRes(function(state) {
                fetchStub.firstCall.args[1].body.toString().should.equal(JSON.stringify({ nodes: ["n1", "n2"] }));
                state.status.should.equal(404);
                done();
            });
            proxy(req, res, function next() { throw new Error("must not fall through"); });
        });

        it("returns 502 (not a fall-through to A) when B is unavailable", function(done) {
            var fetchStub = sinon.stub().rejects(new Error("ECONNREFUSED"));
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://runner-b:1881" },
                { fetch: fetchStub, needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("POST", "/inject/abc", "");
            var res = fakeRes(function(state) {
                state.status.should.equal(502);
                done();
            });
            proxy(req, res, function next() { throw new Error("must not fall through to A on B failure"); });
        });

        it("does not mutate any global auth state (needsPermission is invoked fresh per request, no shared toggles)", function(done) {
            var needsPermission = sinon.stub().returns(allowMiddleware());
            var fetchStub = sinon.stub().resolves(jsonResponse(200, "OK"));
            var proxy = createRemoteRuntimeAdminProxy(
                { target: "http://runner-b:1881" },
                { fetch: fetchStub, needsPermission: needsPermission }
            );
            var req1 = fakeReqWithBody("POST", "/inject/abc", "");
            var req2 = fakeReqWithBody("POST", "/debug/enable", "");
            var res1 = fakeRes(function() {
                proxy(req2, res2, function next() { throw new Error("must not fall through"); });
            });
            var res2 = fakeRes(function() {
                needsPermission.calledWith("inject.write").should.be.true();
                needsPermission.calledWith("debug.write").should.be.true();
                done();
            });
            proxy(req1, res1, function next() { throw new Error("must not fall through"); });
        });
    });

    describe("integration: Express app + fake runner B HTTP server (M5)", function() {
        var server;
        var baseUrl;

        before(function(done) {
            var app = express();
            app.post("/inject/:id", function(req, res) {
                var chunks = [];
                req.on("data", function(c) { chunks.push(c); });
                req.on("end", function() {
                    res.status(200).json({ received: JSON.parse(Buffer.concat(chunks).toString() || "{}"), id: req.params.id });
                });
            });
            app.get("/context/node/:id", function(req, res) {
                res.status(200).json({ id: req.params.id, value: 42 });
            });
            server = http.createServer(app);
            server.listen(0, "127.0.0.1", function() {
                baseUrl = "http://127.0.0.1:" + server.address().port;
                done();
            });
        });

        after(function(done) {
            server.close(done);
        });

        it("round-trips a real Inject request to the fake B server", function(done) {
            var proxy = createRemoteRuntimeAdminProxy(
                { target: baseUrl },
                { needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("POST", "/inject/real123", JSON.stringify({ __user_inject_props__: true }), { "content-type": "application/json" });
            var res = fakeRes(function(state) {
                state.status.should.equal(200);
                var parsed = JSON.parse(state.body.toString());
                parsed.id.should.equal("real123");
                parsed.received.should.deepEqual({ __user_inject_props__: true });
                done();
            });
            proxy(req, res, function next() { throw new Error("must not fall through"); });
        });

        it("round-trips a real Context GET request to the fake B server", function(done) {
            var proxy = createRemoteRuntimeAdminProxy(
                { target: baseUrl },
                { needsPermission: function() { return allowMiddleware(); } }
            );
            var req = fakeReqWithBody("GET", "/context/node/real456", "");
            var res = fakeRes(function(state) {
                state.status.should.equal(200);
                JSON.parse(state.body.toString()).value.should.equal(42);
                done();
            });
            proxy(req, res, function next() { throw new Error("must not fall through"); });
        });
    });
});
