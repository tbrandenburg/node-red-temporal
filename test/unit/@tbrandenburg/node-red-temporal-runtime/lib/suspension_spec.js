var should = require("should");
var { validateSuspension } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/suspension.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/suspension - issue #89 descriptor validation", function() {
    describe("timer", function() {
        it("normalizes a valid timer descriptor", function() {
            var result = validateSuspension({ type: "timer", durationMs: 60000, continuation: { a: 1 } });
            result.should.eql({ type: "timer", durationMs: 60000, continuation: { a: 1 } });
        });

        it("rejects a missing durationMs", function() {
            (function() { validateSuspension({ type: "timer" }); }).should.throw(/durationMs/);
        });

        it("rejects a non-positive durationMs", function() {
            (function() { validateSuspension({ type: "timer", durationMs: 0 }); }).should.throw(/durationMs/);
            (function() { validateSuspension({ type: "timer", durationMs: -5 }); }).should.throw(/durationMs/);
        });

        it("rejects a non-finite durationMs", function() {
            (function() { validateSuspension({ type: "timer", durationMs: Infinity }); }).should.throw(/durationMs/);
            (function() { validateSuspension({ type: "timer", durationMs: NaN }); }).should.throw(/durationMs/);
        });

        it("rejects a non-numeric durationMs", function() {
            (function() { validateSuspension({ type: "timer", durationMs: "60000" }); }).should.throw(/durationMs/);
        });
    });

    describe("signal", function() {
        it("normalizes a valid signal descriptor", function() {
            var result = validateSuspension({ type: "signal", key: "approval:order-123", continuation: { b: 2 } });
            result.should.eql({ type: "signal", key: "approval:order-123", continuation: { b: 2 } });
        });

        it("rejects a missing key", function() {
            (function() { validateSuspension({ type: "signal" }); }).should.throw(/key/);
        });

        it("rejects an empty string key", function() {
            (function() { validateSuspension({ type: "signal", key: "" }); }).should.throw(/key/);
        });

        it("rejects a non-string key", function() {
            (function() { validateSuspension({ type: "signal", key: 123 }); }).should.throw(/key/);
        });
    });

    describe("generic malformed input", function() {
        it("rejects an unknown suspension type", function() {
            (function() { validateSuspension({ type: "approval" }); }).should.throw(/unknown suspension type/);
        });

        it("rejects a missing type", function() {
            (function() { validateSuspension({}); }).should.throw(/unknown suspension type/);
        });

        it("rejects null", function() {
            (function() { validateSuspension(null); }).should.throw(/plain object/);
        });

        it("rejects undefined", function() {
            (function() { validateSuspension(undefined); }).should.throw(/plain object/);
        });

        it("rejects a string", function() {
            (function() { validateSuspension("timer"); }).should.throw(/plain object/);
        });

        it("rejects an array", function() {
            (function() { validateSuspension(["timer"]); }).should.throw(/plain object/);
        });

        it("tags every thrown error with code SUSPENSION_INVALID", function() {
            try {
                validateSuspension({ type: "bogus" });
                should.fail("expected validateSuspension to throw");
            } catch (err) {
                err.code.should.equal("SUSPENSION_INVALID");
            }
        });
    });
});
