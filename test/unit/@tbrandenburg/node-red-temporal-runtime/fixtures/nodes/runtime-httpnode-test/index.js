/**
 * Test-only Node-RED node package for issue #64: registers a route on
 * `RED.httpNode` (the node-owned, runtime-side "node" HTTP app - the same
 * seam stock `http in` nodes and FlowFuse Dashboard 2.0's `ui-base` use) so
 * a test can prove that seam is actually reachable through B's Admin API
 * server once `runtime.httpNode` is mounted alongside `runtime.httpAdmin`.
 */
"use strict";

module.exports = function (RED) {
    RED.httpNode.get("/fixture-runtime-httpnode", function (req, res) {
        res.status(200).json({ ok: true });
    });

    function RuntimeHttpNodeTestNode(config) {
        RED.nodes.createNode(this, config);
    }
    RED.nodes.registerType("runtime-httpnode-test-node", RuntimeHttpNodeTestNode);
};
