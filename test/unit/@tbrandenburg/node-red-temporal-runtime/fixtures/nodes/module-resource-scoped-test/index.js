/**
 * Test-only SCOPED Node-RED node package for issue #74 (see the sibling
 * `module-resource-test` package for the unscoped case).
 */
"use strict";

module.exports = function (RED) {
    function ModuleResourceScopedTestNode(config) {
        RED.nodes.createNode(this, config);
    }
    RED.nodes.registerType("module-resource-scoped-test-node", ModuleResourceScopedTestNode);
};
