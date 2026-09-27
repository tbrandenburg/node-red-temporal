var should = require("should");
var { createGenerationManager } = require("../../../../../packages/node_modules/@tbrandenburg/node-red-temporal-runtime/lib/generationManager.js");

function genA() {
    return { id: "gen-a", flowVersion: "hash-a", activityTaskQueue: "queue-a", target: "http://localhost:1881" };
}
function genB() {
    return { id: "gen-b", flowVersion: "hash-b", activityTaskQueue: "queue-b", target: "http://localhost:1882" };
}

describe("@tbrandenburg/node-red-temporal-runtime/lib/generationManager", function() {
    describe("promote() validation", function() {
        it("rejects a generation missing required fields", async function() {
            var manager = createGenerationManager();
            await manager.promote({ id: "x" }, async function() {}).should.be.rejectedWith(/flowVersion/);
        });

        it("requires a readinessCheck function", function() {
            var manager = createGenerationManager();
            (function() {
                manager.promote(genA());
            }).should.throw(/readinessCheck/);
        });
    });

    describe("first promotion (no prior active generation)", function() {
        it("becomes active once readinessCheck resolves", async function() {
            var manager = createGenerationManager();
            should.equal(manager.getActive(), null);

            var active = await manager.promote(genA(), async function() {});

            active.id.should.equal("gen-a");
            manager.getActive().id.should.equal("gen-a");
            manager.listDraining().should.eql([]);
        });

        it("leaves active null when readinessCheck rejects", async function() {
            var manager = createGenerationManager();

            await manager.promote(genA(), function() {
                return Promise.reject(new Error("runner unreachable"));
            }).should.be.rejectedWith(/runner unreachable/);

            should.equal(manager.getActive(), null);
            manager.listDraining().should.eql([]);
        });
    });

    describe("subsequent promotion (A active, promoting B)", function() {
        it("moves A into draining and makes B active on success", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});

            var active = await manager.promote(genB(), async function() {});

            active.id.should.equal("gen-b");
            manager.getActive().id.should.equal("gen-b");
            var draining = manager.listDraining();
            draining.length.should.equal(1);
            draining[0].id.should.equal("gen-a");
        });

        it("keeps A active and does not add anything to draining when B's readiness check fails", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});

            await manager.promote(genB(), function() {
                return Promise.reject(new Error("flowVersion mismatch on B"));
            }).should.be.rejectedWith(/flowVersion mismatch/);

            manager.getActive().id.should.equal("gen-a");
            manager.listDraining().should.eql([]);
        });

        it("rejects re-promoting the currently active generation", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});

            await manager.promote(genA(), async function() {}).should.be.rejectedWith(/already active/);
        });

        it("rejects re-promoting an already-draining generation", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});
            await manager.promote(genB(), async function() {});

            await manager.promote(genA(), async function() {}).should.be.rejectedWith(/already draining/);
        });
    });

    describe("serialized concurrent promotions", function() {
        it("never lets a second concurrent promote() interleave with the first's readiness check", async function() {
            var manager = createGenerationManager();
            var order = [];
            var releaseFirst;
            var firstGate = new Promise(function(resolve) { releaseFirst = resolve; });

            var first = manager.promote(genA(), async function() {
                order.push("first-readiness-start");
                await firstGate;
                order.push("first-readiness-end");
            });

            var second = manager.promote(genB(), async function() {
                order.push("second-readiness-start");
            });

            // Give the event loop a tick; second must NOT have started yet.
            await new Promise(function(r) { setTimeout(r, 10); });
            order.should.eql(["first-readiness-start"]);

            releaseFirst();
            await first;
            await second;

            order.should.eql(["first-readiness-start", "first-readiness-end", "second-readiness-start"]);
            manager.getActive().id.should.equal("gen-b");
            manager.listDraining()[0].id.should.equal("gen-a");
        });

        it("does not wedge the manager after one promotion attempt fails", async function() {
            var manager = createGenerationManager();

            await manager.promote(genA(), function() {
                return Promise.reject(new Error("boom"));
            }).should.be.rejectedWith(/boom/);

            var active = await manager.promote(genB(), async function() {});
            active.id.should.equal("gen-b");
        });
    });

    describe("retire()", function() {
        it("requires an isSafeToRetire function", function() {
            var manager = createGenerationManager();
            (function() {
                manager.retire("gen-a");
            }).should.throw(/isSafeToRetire/);
        });

        it("throws for an unknown generation id", async function() {
            var manager = createGenerationManager();
            await manager.retire("nope", async function() { return true; }).should.be.rejectedWith(/unknown draining generation/);
        });

        it("refuses to retire the currently active generation", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});

            await manager.retire("gen-a", async function() { return true; }).should.be.rejectedWith(/still active/);
        });

        it("removes a draining generation only once isSafeToRetire() resolves true", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});
            await manager.promote(genB(), async function() {});

            manager.listDraining().length.should.equal(1);

            await manager.retire("gen-a", async function() { return true; });

            manager.listDraining().should.eql([]);
        });

        it("leaves the generation draining when isSafeToRetire() resolves false", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});
            await manager.promote(genB(), async function() {});

            await manager.retire("gen-a", async function() { return false; })
                .should.be.rejectedWith(/refused/);

            manager.listDraining().length.should.equal(1);
            manager.listDraining()[0].id.should.equal("gen-a");
        });

        it("leaves the generation draining when isSafeToRetire() rejects", async function() {
            var manager = createGenerationManager();
            await manager.promote(genA(), async function() {});
            await manager.promote(genB(), async function() {});

            await manager.retire("gen-a", function() { return Promise.reject(new Error("workflow still running")); })
                .should.be.rejectedWith(/workflow still running/);

            manager.listDraining().length.should.equal(1);
        });
    });
});
