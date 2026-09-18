var should = require("should");
var runtime = require("../../../../../packages/node_modules/@yourorg/node-red-temporal-runtime/lib/index.js");

describe("@yourorg/node-red-temporal-runtime/lib/index", function() {
    it("exposes the package version", function() {
        should.exist(runtime.version);
        runtime.version.should.be.a.String();
    });
});
