var should = require("should");
var sinon = require("sinon");
var { createHeartbeatingExecuteNode } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/heartbeat.js");

describe("@tbrandenburg/node-red-temporal-runtime/lib/heartbeat", function() {
    /**
     * Fake timer harness so tests never sleep for real - `tick()` invokes
     * every currently-registered interval callback exactly once, mirroring
     * a single real interval firing, without depending on any global fake
     * timer library.
     */
    function createFakeTimers() {
        var callbacks = {};
        var nextId = 1;
        var setIntervalFn = function(fn) {
            var id = nextId++;
            callbacks[id] = fn;
            return id;
        };
        var clearIntervalFn = function(id) {
            delete callbacks[id];
        };
        return {
            setIntervalFn: setIntervalFn,
            clearIntervalFn: clearIntervalFn,
            tick: function() {
                Object.keys(callbacks).forEach(function(id) {
                    callbacks[id]();
                });
            },
            activeCount: function() {
                return Object.keys(callbacks).length;
            }
        };
    }

    it("heartbeats occur while executeNode is unresolved", function() {
        var timers = createFakeTimers();
        var heartbeat = sinon.stub();
        var resolveExecuteNode;
        var executeNode = function() {
            return new Promise(function(resolve) {
                resolveExecuteNode = resolve;
            });
        };
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: heartbeat
        });

        var pending = wrapped({ nodeId: "n1" });
        timers.tick();
        timers.tick();
        heartbeat.callCount.should.equal(2);

        resolveExecuteNode({ sends: [] });
        return pending;
    });

    it("heartbeat timer stops after resolve", function() {
        var timers = createFakeTimers();
        var executeNode = function() {
            return Promise.resolve({ sends: [] });
        };
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: sinon.stub()
        });

        return wrapped({ nodeId: "n1" }).then(function() {
            timers.activeCount().should.equal(0);
        });
    });

    it("heartbeat timer stops after reject", function() {
        var timers = createFakeTimers();
        var executeNode = function() {
            return Promise.reject(new Error("boom"));
        };
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: sinon.stub()
        });

        return wrapped({ nodeId: "n1" }).then(function() {
            throw new Error("expected wrapped executeNode to reject");
        }, function(err) {
            err.message.should.equal("boom");
            timers.activeCount().should.equal(0);
        });
    });

    it("executeNode input/output is unchanged (resolve path)", function() {
        var timers = createFakeTimers();
        var receivedInput;
        var executeNode = function(input) {
            receivedInput = input;
            return Promise.resolve({ sends: [{ port: 0, msg: { payload: 1 } }], destinationId: "n2" });
        };
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: sinon.stub()
        });

        var input = { flowVersion: "v1", nodeId: "n1", msg: { payload: 1 } };
        return wrapped(input).then(function(result) {
            receivedInput.should.equal(input);
            result.should.eql({ sends: [{ port: 0, msg: { payload: 1 } }], destinationId: "n2" });
        });
    });

    it("executeNode input/output is unchanged (reject path, thrown error passed through as-is)", function() {
        var timers = createFakeTimers();
        var thrown = new Error("infra failure");
        var executeNode = function() {
            return Promise.reject(thrown);
        };
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: sinon.stub()
        });

        return wrapped({ nodeId: "n1" }).then(function() {
            throw new Error("expected rejection");
        }, function(err) {
            err.should.equal(thrown);
        });
    });

    it("wrapper does not inspect node type: it is only ever given input.nodeId (for the heartbeat detail) and never reads input.type/msg", function() {
        var timers = createFakeTimers();
        var heartbeat = sinon.stub();
        var executeNode = sinon.stub().resolves({ sends: [] });
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: heartbeat
        });

        var input = { flowVersion: "v1", nodeId: "agent-node-1", msg: { payload: "secret-agent-prompt" } };
        var pending = wrapped(input);
        timers.tick();
        // The only heartbeat detail ever passed is { nodeId }, regardless of
        // node type/msg contents - proving the wrapper is generic and never
        // needs to know what kind of node it is heartbeating for.
        heartbeat.firstCall.args[0].should.eql({ nodeId: "agent-node-1" });
        return pending;
    });

    it("a heartbeat callback that throws does not affect the underlying executeNode result", function() {
        var timers = createFakeTimers();
        var executeNode = function() {
            return Promise.resolve({ sends: [] });
        };
        var throwingHeartbeat = sinon.stub().throws(new Error("heartbeat transport down"));
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: timers.setIntervalFn,
            clearIntervalFn: timers.clearIntervalFn,
            heartbeat: throwingHeartbeat
        });

        var pending = wrapped({ nodeId: "n1" });
        // Fire the heartbeat (and let it throw) BEFORE executeNode settles,
        // proving the thrown heartbeat error never becomes the Activity's
        // own rejection.
        timers.tick();
        return pending.then(function(result) {
            result.should.eql({ sends: [] });
        });
    });

    it("uses a default heartbeat interval when none is configured", function() {
        var setIntervalFn = sinon.stub().returns(1);
        var executeNode = function() {
            return Promise.resolve({ sends: [] });
        };
        var wrapped = createHeartbeatingExecuteNode(executeNode, {
            setIntervalFn: setIntervalFn,
            clearIntervalFn: sinon.stub(),
            heartbeat: sinon.stub()
        });

        return wrapped({ nodeId: "n1" }).then(function() {
            setIntervalFn.calledOnce.should.equal(true);
            setIntervalFn.firstCall.args[1].should.equal(20000);
        });
    });
});
