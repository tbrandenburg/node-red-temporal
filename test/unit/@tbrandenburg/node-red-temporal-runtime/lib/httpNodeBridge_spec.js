var should = require("should");
var path = require("path");
var RED = require("nr-test-utils").require("node-red/lib/red");
var {
    isStockHttpIngress,
    isStockHttpResponse,
    snapshotRequest,
    snapshotRequestMessage,
    createResponseRecorder,
    applyResponseDescriptor,
    serializeCookie
} = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge.js");
var { bootstrap } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/bootstrap.js");
var { Capture } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/capture.js");
var { createExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/activities.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var HTTP_RESPONSE_FLOW = path.join(FIXTURES, "http-response-flow.json");

/**
 * Minimal fake `http.ServerResponse` sufficient for
 * `applyResponseDescriptor()`'s own contract - records what was written
 * without opening a real socket.
 */
function fakeServerResponse() {
    return {
        headersSent: false,
        writableEnded: false,
        writeHead(statusCode, headers) {
            this.statusCode = statusCode;
            this.headers = headers;
            this.headersSent = true;
        },
        end(body) {
            this.body = body;
            this.writableEnded = true;
        }
    };
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge - classification", function() {
    it("isStockHttpIngress: true only for the stock \"http in\" node type", function() {
        isStockHttpIngress({ type: "http in" }).should.equal(true);
        isStockHttpIngress({ type: "inject" }).should.equal(false);
        isStockHttpIngress(undefined).should.equal(false);
    });

    it("isStockHttpResponse: true only for the stock \"http response\" node type", function() {
        isStockHttpResponse({ type: "http response" }).should.equal(true);
        isStockHttpResponse({ type: "debug" }).should.equal(false);
        isStockHttpResponse(undefined).should.equal(false);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge - snapshotRequest / snapshotRequestMessage (M1)", function() {
    var liveReq = {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer secret", cookie: "a=b" },
        params: { customerId: "123" },
        query: { source: "test" },
        body: { hello: "world" },
        originalUrl: "/customer-request/123?source=test",
        path: "/customer-request/123",
        hostname: "runner-b",
        ip: "127.0.0.1",
        protocol: "http",
        secure: false,
        socket: { destroy() {} },
        pipe: function() {},
        on: function() {}
    };

    it("preserves method/params/query/body/originalUrl/path/hostname/ip/protocol/secure", function() {
        var snapshot = snapshotRequest(liveReq);
        snapshot.method.should.equal("POST");
        snapshot.params.should.eql({ customerId: "123" });
        snapshot.query.should.eql({ source: "test" });
        snapshot.body.should.eql({ hello: "world" });
        snapshot.originalUrl.should.equal("/customer-request/123?source=test");
        snapshot.path.should.equal("/customer-request/123");
        snapshot.hostname.should.equal("runner-b");
        snapshot.ip.should.equal("127.0.0.1");
        snapshot.protocol.should.equal("http");
        snapshot.secure.should.equal(false);
    });

    it("strips sensitive headers (authorization/cookie) per issue #58's security posture", function() {
        var snapshot = snapshotRequest(liveReq);
        snapshot.headers.should.not.have.property("authorization");
        snapshot.headers.should.not.have.property("cookie");
        snapshot.headers.should.have.property("content-type", "application/json");
    });

    it("never carries socket/stream/function properties through", function() {
        var snapshot = snapshotRequest(liveReq);
        snapshot.should.not.have.property("socket");
        snapshot.should.not.have.property("pipe");
        snapshot.should.not.have.property("on");
        JSON.stringify(snapshot).should.be.a.String(); // passes plain JSON serialization
    });

    it("snapshotRequestMessage: preserves ordinary msg fields, drops live req/res, replaces req with its snapshot", function() {
        var liveRes = { _res: { send() {} } };
        var msg = { _msgid: "m1", payload: { hello: "world" }, req: liveReq, res: liveRes };
        var snapshot = snapshotRequestMessage(msg);
        snapshot._msgid.should.equal("m1");
        snapshot.payload.should.eql({ hello: "world" });
        snapshot.should.not.have.property("res");
        snapshot.req.method.should.equal("POST");
        should(function() { JSON.stringify(snapshot); }).not.throw();
    });

    it("does not mutate the original live message", function() {
        var liveRes = { _res: {} };
        var msg = { _msgid: "m2", req: liveReq, res: liveRes };
        snapshotRequestMessage(msg);
        msg.req.should.equal(liveReq);
        msg.res.should.equal(liveRes);
    });

    it("snapshotRequest/snapshotRequestMessage return undefined/absent req when there is none", function() {
        should(snapshotRequest(undefined)).equal(undefined);
        var snapshot = snapshotRequestMessage({ payload: 1 });
        snapshot.should.not.have.property("req");
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge - createResponseRecorder (M4, pure)", function() {
    it("defaults to status 200 when the node never calls status()", function() {
        var entry = createResponseRecorder();
        entry.res.send("hi");
        entry.toDescriptor().statusCode.should.equal(200);
    });

    it("records status(), set() (single + object form), get(), send()", function() {
        var entry = createResponseRecorder();
        entry.res.status(201).set("x-a", "1").set({ "x-b": "2" });
        entry.res.get("x-a").should.equal("1");
        entry.res.send("body-text");
        var descriptor = entry.toDescriptor();
        descriptor.statusCode.should.equal(201);
        descriptor.headers.should.eql({ "x-a": "1", "x-b": "2" });
        descriptor.body.should.equal("body-text");
    });

    it("jsonp() records the body and defaults content-type if unset", function() {
        var entry = createResponseRecorder();
        entry.res.jsonp({ ok: true });
        var descriptor = entry.toDescriptor();
        descriptor.body.should.eql({ ok: true });
        descriptor.headers["content-type"].should.equal("application/json");
    });

    it("cookie()/clearCookie() are recorded, chainable, and returned in the descriptor", function() {
        var entry = createResponseRecorder();
        entry.res.cookie("session", "abc", { httpOnly: true }).clearCookie("stale");
        var descriptor = entry.toDescriptor();
        descriptor.cookies.length.should.equal(2);
        descriptor.cookies[0].should.eql({ name: "session", value: "abc", options: { httpOnly: true }, cleared: false });
        descriptor.cookies[1].cleared.should.equal(true);
    });

    it("status().send() chains onto the SAME recorder object (Express chaining contract)", function() {
        var entry = createResponseRecorder();
        var result = entry.res.status(404).send("nope");
        result.should.equal(entry.res);
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge - applyResponseDescriptor (M7)", function() {
    it("writes statusCode/headers/body onto the real response", function() {
        var res = fakeServerResponse();
        applyResponseDescriptor(res, { statusCode: 201, headers: { "x-test": "yes" }, body: "hello", cookies: [] });
        res.statusCode.should.equal(201);
        res.headers["x-test"].should.equal("yes");
        res.body.toString().should.equal("hello");
    });

    it("JSON-encodes an object body", function() {
        var res = fakeServerResponse();
        applyResponseDescriptor(res, { statusCode: 200, headers: {}, body: { ok: true }, cookies: [] });
        JSON.parse(res.body.toString()).should.eql({ ok: true });
    });

    it("falls back to 200 for an invalid statusCode", function() {
        var res = fakeServerResponse();
        applyResponseDescriptor(res, { statusCode: 9999, headers: {}, body: "x", cookies: [] });
        res.statusCode.should.equal(200);
    });

    it("strips hop-by-hop/framing response headers", function() {
        var res = fakeServerResponse();
        applyResponseDescriptor(res, { statusCode: 200, headers: { "content-length": "999", "x-test": "1" }, body: "x", cookies: [] });
        res.headers.should.not.have.property("content-length");
        res.headers["x-test"].should.equal("1");
    });

    it("serializes cookies into a Set-Cookie header", function() {
        var res = fakeServerResponse();
        applyResponseDescriptor(res, { statusCode: 200, headers: {}, body: "x", cookies: [{ name: "a", value: "b", options: {}, cleared: false }] });
        res.headers["Set-Cookie"][0].should.equal("a=b; Path=/");
    });

    it("never writes if headers already sent (client-disconnect / already-ended contract)", function() {
        var res = fakeServerResponse();
        res.headersSent = true;
        applyResponseDescriptor(res, { statusCode: 200, headers: {}, body: "x", cookies: [] });
        should.not.exist(res.body);
    });

    it("never writes if writableEnded is already true", function() {
        var res = fakeServerResponse();
        res.writableEnded = true;
        applyResponseDescriptor(res, { statusCode: 200, headers: {}, body: "x", cookies: [] });
        should.not.exist(res.body);
    });

    it("is a no-op when res is missing entirely (defensive)", function() {
        should(function() { applyResponseDescriptor(undefined, { statusCode: 200 }); }).not.throw();
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge - serializeCookie", function() {
    it("serializes a cleared cookie with an Expires-in-the-past header", function() {
        serializeCookie({ name: "a", value: "", options: {}, cleared: true }).should.match(/^a=; Expires=/);
    });

    it("serializes httpOnly/secure attributes", function() {
        var value = serializeCookie({ name: "a", value: "b", options: { httpOnly: true, secure: true }, cleared: false });
        value.should.containEql("HttpOnly");
        value.should.containEql("Secure");
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/httpNodeBridge - M4: real stock \"http response\" node against the recorder", function() {
    this.timeout(20000);

    var handle;
    var capture;

    afterEach(function() {
        if (capture) {
            capture.uninstall();
            capture = null;
        }
        if (handle) {
            var h = handle;
            handle = null;
            return h.stop();
        }
    });

    function bootWith(flowFile) {
        return bootstrap(flowFile).then(function(h) {
            handle = h;
            capture = new Capture();
            capture.install(RED);
            var executeNode = createExecuteNode({ getNode: h.getNode, flowVersion: h.flowVersion, capture: capture });
            return { handle: h, executeNode: executeNode };
        });
    }

    it("default status 200, object payload -> jsonp, no configured status/headers", function() {
        return bootWith(HTTP_RESPONSE_FLOW).then(function(ctx) {
            return ctx.executeNode({
                flowVersion: ctx.handle.flowVersion,
                nodeId: "r1",
                msg: { payload: { ok: true }, _msgid: "m1" },
                httpBridge: true
            });
        }).then(function(result) {
            should.not.exist(result.error);
            result.httpResponse.statusCode.should.equal(200);
            result.httpResponse.body.should.eql({ ok: true });
        });
    });

    it("msg.statusCode overrides the default when the node has no configured status", function() {
        return bootWith(HTTP_RESPONSE_FLOW).then(function(ctx) {
            return ctx.executeNode({
                flowVersion: ctx.handle.flowVersion,
                nodeId: "r1",
                msg: { payload: "created", statusCode: 201, _msgid: "m2" },
                httpBridge: true
            });
        }).then(function(result) {
            result.httpResponse.statusCode.should.equal(201);
            result.httpResponse.body.should.equal("created");
        });
    });

    it("msg.headers merge onto the response headers (string payload)", function() {
        return bootWith(HTTP_RESPONSE_FLOW).then(function(ctx) {
            return ctx.executeNode({
                flowVersion: ctx.handle.flowVersion,
                nodeId: "r1",
                msg: { payload: "hello", headers: { "x-test": "temporal-http" }, _msgid: "m3" },
                httpBridge: true
            });
        }).then(function(result) {
            result.httpResponse.headers["x-test"].should.equal("temporal-http");
            result.httpResponse.body.should.equal("hello");
        });
    });

    it("configured node status/headers (r2) take precedence over msg.statusCode", function() {
        return bootWith(HTTP_RESPONSE_FLOW).then(function(ctx) {
            return ctx.executeNode({
                flowVersion: ctx.handle.flowVersion,
                nodeId: "r2",
                msg: { payload: "ignored-status", statusCode: 500, _msgid: "m4" },
                httpBridge: true
            });
        }).then(function(result) {
            result.httpResponse.statusCode.should.equal(418);
            result.httpResponse.headers["x-configured"].should.equal("yes");
        });
    });

    it("without httpBridge:true, an http response node executes normally and carries NO httpResponse field", function() {
        return bootWith(HTTP_RESPONSE_FLOW).then(function(ctx) {
            return ctx.executeNode({
                flowVersion: ctx.handle.flowVersion,
                nodeId: "r1",
                msg: { payload: "x", _msgid: "m5" }
            });
        }).then(function(result) {
            result.should.not.have.property("httpResponse");
        });
    });
});
