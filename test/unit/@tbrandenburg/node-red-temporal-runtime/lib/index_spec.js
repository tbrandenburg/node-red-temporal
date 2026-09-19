var should = require("should");
var runtime = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/index.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/index", function() {
    it("exposes the package version", function() {
        should.exist(runtime.version);
        runtime.version.should.be.a.String();
    });
});
