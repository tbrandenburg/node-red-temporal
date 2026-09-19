var should = require("should");
var path = require("path");
var { extractWireGraph } = require("../../../../../packages/node_modules/@yourorg/node-red-temporal-runtime/lib/wireGraph.js");

var FIXTURES = path.join(__dirname, "..", "fixtures");
var FOUR_NODE_FLOW = require(path.join(FIXTURES, "four-node-flow.json"));

describe("@yourorg/node-red-temporal-runtime/lib/wireGraph", function() {
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
