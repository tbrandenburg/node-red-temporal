/**
 * Test-only Node-RED node package for issue #74. Registers no real
 * functionality - it only needs to exist as a valid node-red module so
 * @node-red/registry's localfilesystem loader picks up its sibling
 * `resources/` directory and exposes it via `getModuleResource`.
 */
"use strict";

module.exports = function (RED) {
    function ModuleResourceTestNode(config) {
        RED.nodes.createNode(this, config);
    }
    RED.nodes.registerType("module-resource-test-node", ModuleResourceTestNode);
};
