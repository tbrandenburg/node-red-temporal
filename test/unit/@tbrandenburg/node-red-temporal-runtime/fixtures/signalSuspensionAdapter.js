/**
 * issue #91: minimal test/PoC-only #89 suspension adapter fixture proving
 * the SIGNAL (event-wait) half of the generic suspension/resume ABI - #90
 * already proved the TIMER half with a real stock Delay node.
 *
 * This is deliberately NOT a production node. It is kept under
 * `test/.../fixtures` (not `packages/node_modules/@tbrandenburg/...`) per
 * the issue's "no production approval node" requirement, and is only ever
 * `require()`d from this repo's own tests.
 *
 * Generic by MESSAGE SHAPE, not by node type (no `node.type === ...`
 * anywhere): `plan()` suspends any node invocation whose incoming
 * `msg.waitKey` is a non-empty string, regardless of what kind of node it
 * is. `continuation` is deliberately tiny/opaque plain data (`{ waitKey }`)
 * - it is never inspected by Workflow code (see workflows.js/suspension.js),
 * only round-tripped back to `resume()` unchanged.
 *
 * `resume()` receives the Temporal Signal's opaque `data` payload
 * (`resumeInput.signal`) and maps it into the resumed msg with exactly one
 * fixture-owned rule, documented here and nowhere else: `msg.approval =
 * data`. It never interprets what `data` itself means (no
 * `data.approved`/`data.reason` branching) - that stays entirely the
 * caller/downstream flow's business. The final send is produced via
 * `capture.routeSend()` (capture.js), so the resolved `destinationId`
 * always comes from Node-RED's own `preRoute` hook, never a raw wire-graph
 * lookup.
 */

"use strict";

function createSignalSuspensionAdapter({ capture }) {
    function plan(node, msg) {
        if (!msg || typeof msg.waitKey !== "string" || msg.waitKey.length === 0) {
            return undefined;
        }
        return { type: "signal", key: msg.waitKey, continuation: { waitKey: msg.waitKey } };
    }

    async function resume(node, msg, resumeInput) {
        // Strip `waitKey` from the resumed msg before it continues
        // downstream: this fixture's `plan()` suspends ANY node invocation
        // it sees `msg.waitKey` on, so leaving it in place would make the
        // very next downstream node (which receives this same msg object's
        // fields) get suspended again for the identical key - already
        // consumed, so it would block forever. Documented fixture-only
        // resume mapping: `msg.approval = data`, `msg.waitKey` removed.
        const resumedMsg = Object.assign({}, msg, { approval: resumeInput.signal });
        delete resumedMsg.waitKey;
        const routed = capture.routeSend(node, resumedMsg, function() {
            node.send(resumedMsg);
        });
        return { sends: routed.sends };
    }

    return { plan, resume };
}

module.exports = { createSignalSuspensionAdapter };
