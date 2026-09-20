/**
 * Test-only Node-RED node package for issue #60: registers a route on
 * `RED.httpAdmin` (the node-owned, runtime-side Admin app) so a test can
 * prove that ANY node-owned Admin route - not just core Inject's
 * `POST /inject/:id` - is actually reachable through B's Admin API server
 * once `runtime.httpAdmin` is mounted onto `editorApi.httpAdmin`.
 */
"use strict";

module.exports = function (RED) {
    RED.httpAdmin.get("/fixture-runtime-admin", function (req, res) {
        res.status(200).json({ ok: true });
    });

    function RuntimeAdminTestNode(config) {
        RED.nodes.createNode(this, config);
    }
    RED.nodes.registerType("runtime-admin-test-node", RuntimeAdminTestNode);
};
