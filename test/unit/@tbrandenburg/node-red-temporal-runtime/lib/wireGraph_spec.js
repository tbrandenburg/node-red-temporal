var should = require("should");
var path = require("path");
var { extractWireGraph, extractNodeMeta } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/wireGraph.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FOUR_NODE_FLOW = require(path.join(FIXTURES, "four-node-flow.json"));

describe("@tbrandenburg/node-red-temporal-runtime/lib/wireGraph", function() {
    it("extracts a wire graph from the four-node demo flow, omitting the tab", function() {
        var graph = extractWireGraph(FOUR_NODE_FLOW);
        graph.should.eql({
            n1: [["n2"]],
            n2: [["n3"]],
            n3: [["n4"]],
            n4: []
        });
    });

    it("includes a node with wires:[[]] (zero destinations on its one port)", function() {
        var graph = extractWireGraph([
            { id: "a", type: "debug", wires: [[]] }
        ]);
        graph.should.eql({ a: [[]] });
    });

    it("omits a node with no wires property at all (e.g. a config-only node)", function() {
        var graph = extractWireGraph([
            { id: "cfg1", type: "mqtt-broker", broker: "localhost" }
        ]);
        graph.should.eql({});
    });

    it("supports multi-port wiring (a node with two output ports)", function() {
        var graph = extractWireGraph([
            { id: "switch1", type: "switch", wires: [["a"], ["b", "c"]] }
        ]);
        graph.should.eql({ switch1: [["a"], ["b", "c"]] });
    });
});

describe("@tbrandenburg/node-red-temporal-runtime/lib/wireGraph - extractNodeMeta", function() {
    it("extracts a name/type map from the four-node demo flow, omitting the tab", function() {
        var meta = extractNodeMeta(FOUR_NODE_FLOW);
        meta.should.eql({
            n1: { name: "start", type: "inject" },
            n2: { name: "double", type: "function" },
            n3: { name: "tag", type: "change" },
            n4: { name: "out", type: "debug" }
        });
    });

    it("omits a node with no wires property at all (e.g. a config-only node)", function() {
        var meta = extractNodeMeta([
            { id: "cfg1", type: "mqtt-broker", broker: "localhost" }
        ]);
        meta.should.eql({});
    });

    it("includes a node with an empty/undefined name (name is optional metadata)", function() {
        var meta = extractNodeMeta([
            { id: "a", type: "debug", wires: [[]] }
        ]);
        meta.should.eql({ a: { name: undefined, type: "debug" } });
    });
});
