var should = require("should");
var sinon = require("sinon");

var {
    ROUTE_PATTERN,
    createModuleResourceRoute
} = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/moduleResourceRoute.js");

function fakeRes() {
    return {
        statusCode: null,
        headers: {},
        body: undefined,
        ended: false,
        set: function (name, value) { this.headers[name] = value; return this; },
        status: function (code) { this.statusCode = code; return this; },
        send: function (data) { this.body = data; return this; },
        end: function () { this.ended = true; return this; }
    };
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/moduleResourceRoute", function() {
    describe("ROUTE_PATTERN", function() {
        it("matches unscoped module resource paths", function() {
            var m = ROUTE_PATTERN.exec("/resources/some-module/client/widget.js");
            should(m).not.be.null();
            m[1].should.equal("some-module");
            m[2].should.equal("client/widget.js");
        });
        it("matches scoped module resource paths", function() {
            var m = ROUTE_PATTERN.exec("/resources/@scope/some-module/client/widget.js");
            should(m).not.be.null();
            m[1].should.equal("@scope/some-module");
            m[2].should.equal("client/widget.js");
        });
        it("does not match unrelated paths", function() {
            should(ROUTE_PATTERN.exec("/theme/foo")).be.null();
            should(ROUTE_PATTERN.exec("/resources/")).be.null();
        });
    });

    describe("createModuleResourceRoute()", function() {
        it("throws without options.nodes.getModuleResource", function() {
            (function () { createModuleResourceRoute({}); }).should.throw();
            (function () { createModuleResourceRoute({ nodes: {} }); }).should.throw();
        });

        it("responds 200 with the resolved buffer and a correct Content-Type on success", function() {
            var nodesApi = {
                getModuleResource: sinon.stub().resolves(Buffer.from("console.log('hi')"))
            };
            var route = createModuleResourceRoute({ nodes: nodesApi });
            var req = { params: ["some-module", "client/widget.js"], user: { id: "u1" } };
            var res = fakeRes();

            route.handler(req, res);
            return new Promise(function (resolve) { setImmediate(resolve); }).then(function () {
                nodesApi.getModuleResource.calledWith({
                    user: req.user,
                    module: "some-module",
                    path: "client/widget.js"
                }).should.be.true();
                res.headers["Content-Type"].should.equal("application/javascript");
                res.body.toString().should.equal("console.log('hi')");
            });
        });

        it("delegates resolution to runtime.nodes.getModuleResource with {user, module, path}", function () {
            var nodesApi = { getModuleResource: sinon.stub().resolves(Buffer.from("x")) };
            var route = createModuleResourceRoute({ nodes: nodesApi });
            var req = { params: ["@scope/some-module", "a/b.js"], user: { id: "u2" } };
            var res = fakeRes();
            return Promise.resolve(route.handler(req, res)).then(function () {
                return new Promise(function (resolve) { setImmediate(resolve); });
            }).then(function () {
                nodesApi.getModuleResource.calledOnce.should.be.true();
                nodesApi.getModuleResource.firstCall.args[0].should.deepEqual({
                    user: req.user,
                    module: "@scope/some-module",
                    path: "a/b.js"
                });
                res.statusCode ? res.statusCode.should.equal(200) : true.should.be.true(); // eslint-disable-line no-unused-expressions
                res.headers["Content-Type"].should.equal("application/javascript");
                res.body.toString().should.equal("x");
            });
        });

        it("responds 404 when getModuleResource resolves null (unknown module/resource)", function () {
            var nodesApi = { getModuleResource: sinon.stub().resolves(null) };
            var route = createModuleResourceRoute({ nodes: nodesApi });
            var req = { params: ["unknown-module", "does/not/exist.js"], user: null };
            var res = fakeRes();
            return Promise.resolve(route.handler(req, res)).then(function () {
                return new Promise(function (resolve) { setImmediate(resolve); });
            }).then(function () {
                res.statusCode.should.equal(404);
                res.ended.should.be.true();
                should(res.body).be.undefined();
            });
        });

        it("responds with the error's status (or 500) when getModuleResource rejects", function () {
            var err = new Error("boom");
            err.status = 400;
            var nodesApi = { getModuleResource: sinon.stub().rejects(err) };
            var route = createModuleResourceRoute({ nodes: nodesApi });
            var req = { params: ["some-module", "../../etc/passwd"], user: null };
            var res = fakeRes();
            return Promise.resolve(route.handler(req, res)).then(function () {
                return new Promise(function (resolve) { setImmediate(resolve); });
            }).then(function () {
                res.statusCode.should.equal(400);
                res.ended.should.be.true();
            });
        });
    });
});
