var should = require("should");
var net = require("net");
var { defaultPayloadConverter } = require("@temporalio/common");

// Issue #35: Define and prove the Temporal message serialization boundary.
//
// This spec exercises the REAL, installed Temporal SDK's default Data
// Converter directly (`@temporalio/common`'s `defaultPayloadConverter`),
// which is exactly what carries a Node-RED `msg` across every
// Workflow-input / Activity-input / Activity-result boundary in this
// runtime (see `lib/workflows.js`'s `proxyActivities` call, which relies on
// this same converter under the hood - no custom Data Converter is
// configured anywhere in this codebase).
//
// The goal is to OBSERVE and PIN DOWN actual SDK behavior for
// representative Node-RED msg value shapes, not to invent or enforce a
// parallel validator. Each `it` documents the category
// (SUPPORTED / SUPPORTED WITH DOCUMENTED REPRESENTATION / UNSUPPORTED)
// the observed behavior falls into; the README's compatibility table is a
// direct transcription of what these assertions prove.

function roundtrip(value) {
    var payload = defaultPayloadConverter.toPayload(value);
    return defaultPayloadConverter.fromPayload(payload);
}

describe("@tbrandenburg/node-red-temporal-runtime - Temporal message serialization boundary (issue #35)", function() {

    describe("SUPPORTED: plain JSON-like values round-trip with expected meaning", function() {
        it("round-trips strings, numbers, booleans and null unchanged", function() {
            roundtrip("hello").should.equal("hello");
            roundtrip(42).should.equal(42);
            roundtrip(true).should.equal(true);
            should(roundtrip(null)).be.null();
        });

        it("round-trips nested objects and arrays unchanged", function() {
            var msg = { payload: { a: [1, 2, 3], b: { c: "x" } }, topic: "t" };
            roundtrip(msg).should.eql(msg);
        });
    });

    describe("SUPPORTED WITH DOCUMENTED REPRESENTATION: Buffer/binary", function() {
        it("round-trips a Buffer's bytes but changes its runtime type to Uint8Array", function() {
            var original = Buffer.from("hello binary", "utf8");
            var result = roundtrip(original);

            // The exact bytes survive - this is the acceptance-critical
            // guarantee for binary payloads (issue #35 step 2).
            Buffer.from(result).toString("utf8").should.equal("hello binary");

            // But the representation is NOT a Buffer instance any more; the
            // default converter decodes binary payloads to a raw
            // Uint8Array. Any downstream node comparing `Buffer.isBuffer`
            // must account for this documented representation change.
            result.should.be.instanceof(Uint8Array);
            Buffer.isBuffer(result).should.be.false();
        });
    });

    describe("SUPPORTED WITH DOCUMENTED REPRESENTATION: Date", function() {
        it("round-trips a Date as an ISO-8601 string, not a Date instance", function() {
            var original = new Date("2024-01-01T00:00:00.000Z");
            var result = roundtrip(original);

            result.should.equal("2024-01-01T00:00:00.000Z");
            result.should.not.be.instanceof(Date);
        });
    });

    describe("SUPPORTED WITH DOCUMENTED REPRESENTATION: undefined", function() {
        it("round-trips a bare `undefined` value as `undefined`", function() {
            should(roundtrip(undefined)).be.undefined();
        });

        it("silently drops object properties whose value is `undefined`", function() {
            var result = roundtrip({ a: 1, b: undefined });
            result.should.eql({ a: 1 });
            result.should.not.have.property("b");
        });
    });

    describe("SUPPORTED WITH DOCUMENTED REPRESENTATION: Error objects", function() {
        it("round-trips an Error as a plain object, losing message/stack/name", function() {
            var original = new Error("boom");
            var result = roundtrip(original);

            // The underlying converter treats Error like any other object
            // and serializes it via JSON, which drops every own-enumerable-
            // property-less field an Error carries (message/stack/name are
            // non-enumerable). Nodes that pass `msg.error` as a live Error
            // instance must not rely on it staying an Error, or on
            // `.message`/`.stack` surviving the trip.
            result.should.eql({});
            result.should.not.be.instanceof(Error);
        });
    });

    describe("SUPPORTED WITH DOCUMENTED REPRESENTATION: functions are silently stripped", function() {
        it("drops a function-valued property from an otherwise plain object", function() {
            var result = roundtrip({ a: 1, fn: function() {} });
            result.should.eql({ a: 1 });
            result.should.not.have.property("fn");
        });

        it("fails to convert a bare function value (nothing left to encode)", function() {
            should(function() {
                roundtrip(function namedFn() {});
            }).throw();
        });
    });

    describe("UNSUPPORTED: circular structures fail visibly", function() {
        it("throws when converting an object with a circular reference, rather than silently corrupting it", function() {
            var circular = { a: 1 };
            circular.self = circular;

            should(function() {
                defaultPayloadConverter.toPayload(circular);
            }).throw();
        });
    });

    describe("UNSUPPORTED: live/native objects (sockets, streams) are outside the durable message boundary", function() {
        it("does not throw for a live net.Socket, but silently flattens it to an unusable internal-state snapshot", function() {
            // This is explicitly called out as OUTSIDE the normal durable
            // message boundary (issue #35 background: "HTTP In/HTTP
            // Response live req/res bridging is explicitly out of scope").
            // The converter does not reject it - it happily JSON-serializes
            // whatever enumerable internal fields the socket currently
            // exposes - but the result is neither a socket nor anything a
            // downstream node could meaningfully use as one. This is
            // documented as UNSUPPORTED for durable messaging purposes even
            // though it technically "round-trips" without throwing.
            var socket = new net.Socket();
            var result;

            should(function() {
                result = roundtrip(socket);
            }).not.throw();

            result.should.not.be.instanceof(net.Socket);
            result.should.not.have.property("write");
            socket.destroy();
        });
    });

    describe("Failure visibility: an unsupported Workflow/Activity boundary value must fail visibly", function() {
        it("propagates the underlying converter failure rather than swallowing it (proves the same failure Temporal's SDK surfaces when Workflow.start()/an Activity result attempts to convert a circular payload)", function() {
            var circular = { a: 1 };
            circular.self = circular;

            var thrown;
            try {
                defaultPayloadConverter.toPayload(circular);
            } catch (err) {
                thrown = err;
            }

            should.exist(thrown);
            // Confirms the error carries a diagnosable name/message - this
            // is the same class of error Temporal's Client/Worker surface
            // to the caller (as a start failure or a WorkflowExecutionFailed
            // / ActivityFailure) when this exact converter cannot encode a
            // Workflow input or an Activity result. No production wrapping
            // is needed: the native error is not opaque - it names the
            // unconvertible value and the reason.
            thrown.message.should.match(/[Uu]nable to convert/);
        });
    });
});
