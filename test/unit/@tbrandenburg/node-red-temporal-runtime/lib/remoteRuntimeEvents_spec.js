var should = require("should");
var sinon = require("sinon");
var EventEmitter = require("events");
var http = require("http");
var {
    isForwardableCommsEvent,
    isForwardableRuntimeEvent,
    isEmptyStatus,
    encodeRecord,
    decodeLine,
    createStatusSnapshot,
    createRuntimeStateSnapshot,
    createRuntimeEventsRoute,
    createRuntimeEventsReceiver,
    DEFAULT_PATH
} = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/remoteRuntimeEvents.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/remoteRuntimeEvents", function() {
    describe("isForwardableCommsEvent()", function() {
        it("accepts comms events with topic 'debug'", function() {
            isForwardableCommsEvent({ topic: "debug", data: { id: "n1", msg: "hi" } }).should.be.true();
        });
        it("rejects comms events with any other topic", function() {
            isForwardableCommsEvent({ topic: "status/n1", data: {} }).should.be.false();
            isForwardableCommsEvent({ topic: "notification/runtime-deploy", data: {} }).should.be.false();
        });
        it("rejects malformed input", function() {
            isForwardableCommsEvent(null).should.be.false();
            isForwardableCommsEvent(undefined).should.be.false();
            isForwardableCommsEvent("debug").should.be.false();
        });
    });

    describe("isForwardableRuntimeEvent()", function() {
        it("accepts native runtime-state runtime-events", function() {
            isForwardableRuntimeEvent({ id: "runtime-state", payload: { state: "start" }, retain: true }).should.be.true();
            isForwardableRuntimeEvent({ id: "runtime-state", retain: true }).should.be.true();
        });
        it("rejects any other runtime-event id", function() {
            isForwardableRuntimeEvent({ id: "runtime-deploy" }).should.be.false();
            isForwardableRuntimeEvent({ id: "node-status" }).should.be.false();
        });
        it("rejects malformed input", function() {
            isForwardableRuntimeEvent(null).should.be.false();
            isForwardableRuntimeEvent(undefined).should.be.false();
            isForwardableRuntimeEvent("runtime-state").should.be.false();
        });
    });

    describe("isEmptyStatus()", function() {
        it("treats missing/undefined status as empty", function() {
            isEmptyStatus(undefined).should.be.true();
            isEmptyStatus(null).should.be.true();
        });
        it("treats a status object with no text/fill/shape as empty", function() {
            isEmptyStatus({}).should.be.true();
        });
        it("treats a status object with any of text/fill/shape as non-empty", function() {
            isEmptyStatus({ text: "connected" }).should.be.false();
            isEmptyStatus({ fill: "green" }).should.be.false();
            isEmptyStatus({ shape: "dot" }).should.be.false();
        });
    });

    describe("encodeRecord() / decodeLine()", function() {
        it("round-trips a node-status record, preserving payload unchanged", function() {
            var event = { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } };
            var line = encodeRecord("node-status", event);
            line.should.endWith("\n");
            var decoded = decodeLine(line);
            decoded.should.eql({ type: "node-status", event: event });
        });

        it("round-trips a debug comms record, preserving payload unchanged", function() {
            var event = { topic: "debug", data: { id: "n2", msg: "hello" } };
            var line = encodeRecord("comms", event);
            var decoded = decodeLine(line);
            decoded.should.eql({ type: "comms", event: event });
        });

        it("rejects/ignores a comms record with a non-debug topic", function() {
            var line = encodeRecord("comms", { topic: "status/n1", data: {} });
            should(decodeLine(line)).be.null();
        });

        it("round-trips a runtime-state record, preserving payload unchanged", function() {
            var event = { id: "runtime-state", payload: { state: "start", deploy: true }, retain: true };
            var line = encodeRecord("runtime-state", event);
            line.should.endWith("\n");
            var decoded = decodeLine(line);
            decoded.should.eql({ type: "runtime-state", event: event });
        });

        it("rejects/ignores a runtime-state record whose event id is not runtime-state", function() {
            var line = encodeRecord("runtime-state", { id: "node-status" });
            should(decodeLine(line)).be.null();
        });

        it("rejects an unknown record type", function() {
            var line = JSON.stringify({ type: "runtime-event", event: { id: "n1" } }) + "\n";
            should(decodeLine(line)).be.null();
        });

        it("rejects a record missing its event object", function() {
            var line = JSON.stringify({ type: "node-status" }) + "\n";
            should(decodeLine(line)).be.null();
        });

        it("ignores blank lines", function() {
            should(decodeLine("")).be.null();
            should(decodeLine("   \n")).be.null();
        });

        it("ignores malformed JSON without throwing", function() {
            should(decodeLine("{not json")).be.null();
        });

        it("ignores non-string input without throwing", function() {
            should(decodeLine(undefined)).be.null();
            should(decodeLine(42)).be.null();
        });
    });

    describe("createStatusSnapshot()", function() {
        it("retains the latest status per node id", function() {
            var snapshot = createStatusSnapshot();
            snapshot.apply({ id: "n1", status: { fill: "blue", shape: "ring", text: "starting" } });
            snapshot.apply({ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
            snapshot.list().should.eql([{ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } }]);
        });

        it("keeps independent entries per node id", function() {
            var snapshot = createStatusSnapshot();
            snapshot.apply({ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
            snapshot.apply({ id: "n2", status: { fill: "red", shape: "ring", text: "error" } });
            snapshot.list().map((e) => e.id).sort().should.eql(["n1", "n2"]);
        });

        it("removes the retained entry when a clearing status arrives", function() {
            var snapshot = createStatusSnapshot();
            snapshot.apply({ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
            snapshot.apply({ id: "n1", status: {} });
            snapshot.list().should.eql([]);
        });

        it("removes the retained entry when status is entirely absent", function() {
            var snapshot = createStatusSnapshot();
            snapshot.apply({ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
            snapshot.apply({ id: "n1" });
            snapshot.list().should.eql([]);
        });

        it("ignores events without an id", function() {
            var snapshot = createStatusSnapshot();
            snapshot.apply(null);
            snapshot.apply({ status: { fill: "green" } });
            snapshot.list().should.eql([]);
        });
    });

    describe("createRuntimeStateSnapshot()", function() {
        it("starts with no retained state", function() {
            var snapshot = createRuntimeStateSnapshot();
            should(snapshot.get()).be.null();
        });

        it("retains only the latest runtime-state event", function() {
            var snapshot = createRuntimeStateSnapshot();
            snapshot.apply({ id: "runtime-state", payload: { state: "stop" }, retain: true });
            snapshot.apply({ id: "runtime-state", payload: { state: "start" }, retain: true });
            snapshot.get().should.eql({ id: "runtime-state", payload: { state: "start" }, retain: true });
        });

        it("ignores non-runtime-state events", function() {
            var snapshot = createRuntimeStateSnapshot();
            snapshot.apply({ id: "node-status", status: {} });
            snapshot.apply(null);
            should(snapshot.get()).be.null();
        });
    });

    describe("createRuntimeEventsRoute()", function() {
        it("requires options.events", function() {
            (function() {
                createRuntimeEventsRoute({});
            }).should.throw(/options\.events/);
        });

        function makeRes() {
            var res = new EventEmitter();
            res.chunks = [];
            res.writableEnded = false;
            res.destroyed = false;
            res.writeHead = sinon.stub();
            res.flushHeaders = sinon.stub();
            res.write = function(chunk) { res.chunks.push(chunk); };
            return res;
        }
        function makeReq() {
            return new EventEmitter();
        }
        function linesOf(res) {
            return res.chunks.join("").split("\n").filter(Boolean).map((l) => JSON.parse(l));
        }

        it("sends the retained status snapshot before live events", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            events.emit("node-status", { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });

            var res = makeRes();
            var req = makeReq();
            route.handler(req, res);

            linesOf(res).should.eql([
                { type: "node-status", event: { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } } }
            ]);

            events.emit("node-status", { id: "n2", status: { fill: "red", shape: "ring", text: "error" } });
            linesOf(res).should.have.length(2);
            linesOf(res)[1].should.eql({ type: "node-status", event: { id: "n2", status: { fill: "red", shape: "ring", text: "error" } } });

            route.close();
        });

        it("forwards only debug comms events, never other topics", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            var res = makeRes();
            var req = makeReq();
            route.handler(req, res);

            events.emit("comms", { topic: "debug", data: { id: "n1", msg: "hello" } });
            events.emit("comms", { topic: "status/n1", data: {} });
            events.emit("comms", { topic: "runtime-event", data: {} });

            var lines = linesOf(res);
            lines.should.have.length(1);
            lines[0].should.eql({ type: "comms", event: { topic: "debug", data: { id: "n1", msg: "hello" } } });

            route.close();
        });

        it("forwards only runtime-state runtime-events, never other ids", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            var res = makeRes();
            var req = makeReq();
            route.handler(req, res);

            events.emit("runtime-event", { id: "runtime-state", payload: { state: "start" }, retain: true });
            events.emit("runtime-event", { id: "runtime-deploy", payload: {} });
            events.emit("runtime-event", { id: "node-status" });

            var lines = linesOf(res);
            lines.should.have.length(1);
            lines[0].should.eql({ type: "runtime-state", event: { id: "runtime-state", payload: { state: "start" }, retain: true } });

            route.close();
        });

        it("sends the retained runtime-state snapshot before live events, to a newly-connected receiver", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            events.emit("runtime-event", { id: "runtime-state", payload: { state: "start" }, retain: true });

            var res = makeRes();
            var req = makeReq();
            route.handler(req, res);

            linesOf(res).should.eql([
                { type: "runtime-state", event: { id: "runtime-state", payload: { state: "start" }, retain: true } }
            ]);

            events.emit("runtime-event", { id: "runtime-state", payload: { state: "stop" }, retain: true });
            linesOf(res).should.have.length(2);
            linesOf(res)[1].should.eql({ type: "runtime-state", event: { id: "runtime-state", payload: { state: "stop" }, retain: true } });

            route.close();
        });

        it("does not replay any runtime-state when B has never emitted one", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            var res = makeRes();
            var req = makeReq();
            route.handler(req, res);

            linesOf(res).should.eql([]);
            route.close();
        });

        it("removes its per-connection listeners on request close (no leak)", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            var res = makeRes();
            var req = makeReq();
            route.handler(req, res);

            events.listenerCount("node-status").should.equal(2); // permanent snapshot tracker + this connection
            events.listenerCount("comms").should.equal(1);
            events.listenerCount("runtime-event").should.equal(2); // permanent snapshot tracker + this connection

            req.emit("close");

            events.listenerCount("node-status").should.equal(1); // only the permanent tracker remains
            events.listenerCount("comms").should.equal(0);
            events.listenerCount("runtime-event").should.equal(1); // only the permanent tracker remains

            route.close();
            events.listenerCount("node-status").should.equal(0);
            events.listenerCount("runtime-event").should.equal(0);
        });

        it("supports multiple independent connections, each receiving events", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            var resA = makeRes();
            var resB = makeRes();
            route.handler(makeReq(), resA);
            route.handler(makeReq(), resB);

            events.emit("node-status", { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });

            linesOf(resA).should.have.length(1);
            linesOf(resB).should.have.length(1);

            route.close();
        });

        it("repeated connect/disconnect cycles never leak listeners", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });

            for (var i = 0; i < 20; i++) {
                var res = makeRes();
                var req = makeReq();
                route.handler(req, res);
                req.emit("close");
            }

            events.listenerCount("node-status").should.equal(1); // just the permanent tracker
            events.listenerCount("comms").should.equal(0);

            route.close();
            events.listenerCount("node-status").should.equal(0);
        });

        it("a write failure on a dead client is swallowed, never thrown into the event emitter", function() {
            var events = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: events });
            var res = makeRes();
            res.write = function() { throw new Error("write after end"); };
            var req = makeReq();
            route.handler(req, res);

            (function() {
                events.emit("node-status", { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
            }).should.not.throw();

            route.close();
        });
    });

    describe("createRuntimeEventsReceiver()", function() {
        it("requires options.events and options.target", function() {
            (function() {
                createRuntimeEventsReceiver({ target: "http://localhost:1881" });
            }).should.throw(/options\.events/);
            (function() {
                createRuntimeEventsReceiver({ events: new EventEmitter() });
            }).should.throw(/options\.target/);
        });

        it("throws when no fetch implementation is available", function() {
            var originalFetch = global.fetch;
            delete global.fetch;
            try {
                (function() {
                    createRuntimeEventsReceiver({ events: new EventEmitter(), target: "http://localhost:1881" });
                }).should.throw(/fetch implementation/);
            } finally {
                global.fetch = originalFetch;
            }
        });

        function makeStreamResponse(lines) {
            var chunks = lines.slice();
            return {
                ok: true,
                status: 200,
                body: {
                    getReader() {
                        return {
                            read() {
                                if (chunks.length === 0) {
                                    return Promise.resolve({ value: undefined, done: true });
                                }
                                var next = chunks.shift();
                                return Promise.resolve({ value: Buffer.from(next), done: false });
                            },
                            releaseLock() {}
                        };
                    }
                }
            };
        }

        it("re-emits a node-status record exactly on A's events singleton", function(done) {
            var events = new EventEmitter();
            var line = JSON.stringify({ type: "node-status", event: { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } } }) + "\n";
            var fetchStub = sinon.stub().resolves(makeStreamResponse([line]));

            events.once("node-status", function(event) {
                event.should.eql({ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
                receiver.stop();
                done();
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();
        });

        it("re-emits a debug comms record exactly on A's events singleton", function(done) {
            var events = new EventEmitter();
            var line = JSON.stringify({ type: "comms", event: { topic: "debug", data: { id: "n2", msg: "hello" } } }) + "\n";
            var fetchStub = sinon.stub().resolves(makeStreamResponse([line]));

            events.once("comms", function(event) {
                event.should.eql({ topic: "debug", data: { id: "n2", msg: "hello" } });
                receiver.stop();
                done();
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();
        });

        it("ignores a malformed line without killing the receiver", function(done) {
            var events = new EventEmitter();
            var goodLine = JSON.stringify({ type: "node-status", event: { id: "n1", status: { fill: "green", shape: "dot", text: "ok" } } }) + "\n";
            var fetchStub = sinon.stub().resolves(makeStreamResponse(["{not json\n", goodLine]));

            events.once("node-status", function(event) {
                event.should.eql({ id: "n1", status: { fill: "green", shape: "dot", text: "ok" } });
                receiver.stop();
                done();
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();
        });

        it("ignores an unknown record type", function(done) {
            var events = new EventEmitter();
            var badLine = JSON.stringify({ type: "runtime-event", event: { id: "n1" } }) + "\n";
            var goodLine = JSON.stringify({ type: "node-status", event: { id: "n1", status: { fill: "green", shape: "dot", text: "ok" } } }) + "\n";
            var fetchStub = sinon.stub().resolves(makeStreamResponse([badLine, goodLine]));

            var sawUnknown = false;
            events.on("runtime-event", function() { sawUnknown = true; });
            events.once("node-status", function() {
                sawUnknown.should.be.false();
                receiver.stop();
                done();
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();
        });

        it("re-emits a runtime-state record as a native runtime-event on A's events singleton", function(done) {
            var events = new EventEmitter();
            var nativeEvent = { id: "runtime-state", payload: { state: "start", deploy: true }, retain: true };
            var line = JSON.stringify({ type: "runtime-state", event: nativeEvent }) + "\n";
            var fetchStub = sinon.stub().resolves(makeStreamResponse([line]));

            events.once("runtime-event", function(event) {
                event.should.eql(nativeEvent);
                receiver.stop();
                done();
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();
        });

        it("re-asserts B's last known runtime-state after A's own local runtime-state event fires (ordering safeguard)", function(done) {
            var events = new EventEmitter();
            var bState = { id: "runtime-state", payload: { state: "start", deploy: true }, retain: true };
            var line = JSON.stringify({ type: "runtime-state", event: bState }) + "\n";
            // never resolves further - simulates an open, live stream so the
            // receiver stays connected while we simulate A's own local emit.
            var fetchStub = sinon.stub().resolves(makeStreamResponse([line]));

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });

            var seenStates = [];
            events.on("runtime-event", function(event) {
                if (event && event.id === "runtime-state") {
                    seenStates.push(event.payload && event.payload.state);
                }
            });

            events.once("runtime-event", function() {
                // B's relayed "start" has now been fully applied (the outer
                // emit call that delivered it has returned). Simulate A's
                // own local runtime emitting its intentionally-always-
                // stopped state on a LATER tick, e.g. as part of a redeploy
                // - never synchronously nested inside the relay's own emit
                // call, matching how Node-RED's own flows/index.js would
                // actually emit it from a wholly separate call stack.
                setImmediate(function() {
                    events.emit("runtime-event", { id: "runtime-state", payload: { state: "stop", deploy: true }, retain: true });

                    seenStates.should.eql(["start", "stop", "start"]);
                    receiver.stop();
                    done();
                });
            });

            receiver.start();
        });

        it("does not re-assert anything when A's local runtime-state fires before B has ever sent one", function() {
            var events = new EventEmitter();
            var fetchStub = sinon.stub().callsFake(function() {
                return new Promise(function() {}); // never resolves: no B data has arrived yet
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();

            var seenStates = [];
            events.on("runtime-event", function(event) {
                seenStates.push(event.payload && event.payload.state);
            });
            events.emit("runtime-event", { id: "runtime-state", payload: { state: "stop" }, retain: true });

            seenStates.should.eql(["stop"]); // no B state known yet - nothing to correct with
            receiver.stop();
        });

        it("reconnects with a bounded backoff after the stream closes", function(done) {
            var events = new EventEmitter();
            var line = JSON.stringify({ type: "node-status", event: { id: "n1", status: { fill: "green", shape: "dot", text: "ok" } } }) + "\n";
            var callCount = 0;
            var fetchStub = sinon.stub().callsFake(function() {
                callCount += 1;
                return Promise.resolve(makeStreamResponse([line]));
            });

            var receiveCount = 0;
            events.on("node-status", function() {
                receiveCount += 1;
                if (receiveCount === 2) {
                    callCount.should.equal(2);
                    receiver.stop();
                    done();
                }
            });

            var receiver = createRuntimeEventsReceiver({
                events: events,
                target: "http://localhost:1881",
                fetch: fetchStub,
                log: { warn() {} },
                minBackoffMs: 5,
                maxBackoffMs: 20
            });
            receiver.start();
        });

        it("does not create parallel active streams on duplicate start() calls", function() {
            var events = new EventEmitter();
            var callCount = 0;
            var fetchStub = sinon.stub().callsFake(function() {
                callCount += 1;
                return new Promise(function() {}); // never resolves: simulates an open, live stream
            });

            var receiver = createRuntimeEventsReceiver({ events: events, target: "http://localhost:1881", fetch: fetchStub, log: { warn() {} } });
            receiver.start();
            receiver.start();
            receiver.start();

            callCount.should.equal(1);
            receiver.stop();
        });

        it("a connection refusal does not throw and schedules a retry", function(done) {
            var events = new EventEmitter();
            var fetchStub = sinon.stub().rejects(new Error("ECONNREFUSED"));
            var warnCalls = [];

            var receiver = createRuntimeEventsReceiver({
                events: events,
                target: "http://localhost:1881",
                fetch: fetchStub,
                log: { warn(msg) { warnCalls.push(msg); } },
                minBackoffMs: 5,
                maxBackoffMs: 10
            });

            receiver.start();
            setTimeout(function() {
                fetchStub.callCount.should.be.greaterThan(1);
                warnCalls.length.should.be.greaterThan(0);
                receiver.stop();
                done();
            }, 50);
        });
    });

    describe("live HTTP integration (server + receiver)", function() {
        it("streams the snapshot then live events end-to-end over a real HTTP server", function(done) {
            var serverEvents = new EventEmitter();
            var route = createRuntimeEventsRoute({ events: serverEvents });
            serverEvents.emit("node-status", { id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });

            var server = http.createServer(function(req, res) {
                if (req.url === DEFAULT_PATH) {
                    route.handler(req, res);
                } else {
                    res.writeHead(404);
                    res.end();
                }
            });

            server.listen(0, "127.0.0.1", function() {
                var port = server.address().port;
                var clientEvents = new EventEmitter();
                var receiver = createRuntimeEventsReceiver({
                    events: clientEvents,
                    target: `http://127.0.0.1:${port}`,
                    fetch: fetch,
                    log: { warn() {} }
                });

                clientEvents.once("node-status", function(event) {
                    event.should.eql({ id: "n1", status: { fill: "green", shape: "dot", text: "connected" } });
                    receiver.stop();
                    route.close();
                    server.close(done);
                });

                receiver.start();
            });
        });
    });
});
