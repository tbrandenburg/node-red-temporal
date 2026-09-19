/**
 * Test-only Node-RED node package for issue #14: proves credentials,
 * config-node sharing, and node/flow/global context all work for real,
 * live nodes booted through the real @node-red/runtime - no
 * node-red-temporal-specific subsystem involved.
 */
"use strict";

module.exports = function (RED) {
    /**
     * Config node: holds one credential (`apiKey`, type "password") plus a
     * plain `label` property. Multiple `test-ctx-node` instances can
     * reference the SAME config node id to prove config-node sharing.
     */
    function TestConfigNode(config) {
        RED.nodes.createNode(this, config);
        this.label = config.label;
    }
    RED.nodes.registerType("test-config-node", TestConfigNode, {
        credentials: {
            apiKey: { type: "password" }
        }
    });

    /**
     * Regular node: on receiving a message, reads its config node's
     * credential and label, bumps a counter in node/flow/global context
     * (global context counter written through a NAMED context store so a
     * persistent store can be proven to survive a bootstrap restart), and
     * sends a single message summarizing everything it read.
     */
    function TestCtxNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;
        node.configNode = RED.nodes.getNode(config.config);
        node.contextStore = config.contextStore; // e.g. "file"; undefined = default memory store

        node.on("input", function (msg, send, done) {
            const nodeContext = node.context();
            const flowContext = node.context().flow;
            const globalContext = node.context().global;

            const nodeCount = (nodeContext.get("count") || 0) + 1;
            nodeContext.set("count", nodeCount);

            const flowCount = (flowContext.get("count") || 0) + 1;
            flowContext.set("count", flowCount);

            const storeArg = node.contextStore;
            const previousGlobalCount = storeArg ? globalContext.get("count", storeArg) : globalContext.get("count");
            const globalCount = (previousGlobalCount || 0) + 1;
            if (storeArg) {
                globalContext.set("count", globalCount, storeArg);
            } else {
                globalContext.set("count", globalCount);
            }

            const apiKey = node.configNode && node.configNode.credentials && node.configNode.credentials.apiKey;
            msg.payload = {
                // `redactCredential` (config property): when true, the raw
                // credential value is deliberately kept OUT of the outgoing
                // msg - used by the issue #14 manual E2E acceptance flow,
                // which routes this msg through real Temporal Activities/
                // Workflow history and must not leak the secret there. Only
                // proof-of-possession (hasApiKey/apiKeyLength) travels with
                // the msg in that mode; unit tests that call node.receive()
                // directly (never touching Temporal) use the default
                // (non-redacted) mode to assert on the actual value.
                apiKey: config.redactCredential ? undefined : apiKey,
                hasApiKey: !!apiKey,
                apiKeyLength: apiKey ? apiKey.length : 0,
                configLabel: node.configNode && node.configNode.label,
                nodeCount: nodeCount,
                flowCount: flowCount,
                globalCount: globalCount
            };
            send(msg);
            if (done) {
                done();
            }
        });
    }
    RED.nodes.registerType("test-ctx-node", TestCtxNode);
};
