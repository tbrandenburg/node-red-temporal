# Convenience targets for the Temporal-backed Node-RED demo
# (packages/node_modules/@tbrandenburg/node-red-temporal-runtime).
#
# These do NOT replace the repo's own npm scripts (npm install/test/lint/
# build) - they only wrap the demo's Temporal dev server + worker lifecycle
# so it doesn't need 3 manually-managed terminals.

RUNTIME_DIR := packages/node_modules/@tbrandenburg/node-red-temporal-runtime
CLI         := $(RUNTIME_DIR)/bin/node-red-temporal
DEMO_FLOW   := $(RUNTIME_DIR)/demo/flows.json
RELEASE_TAG_PREFIX := temporal-v

PID_DIR        := /tmp/node-red-temporal-demo
TEMPORAL_PID   := $(PID_DIR)/temporal.pid
TEMPORAL_LOG   := $(PID_DIR)/temporal.log
TEMPORAL_DB    := $(PID_DIR)/temporal.db
WORKER_PID     := $(PID_DIR)/worker.pid
WORKER_LOG     := $(PID_DIR)/worker.log

.PHONY: publish release demo-run demo-start demo-status demo-stop

## publish: publish only the Temporal runtime package to npm
publish:
	@npm publish --access public $(RUNTIME_DIR)

## release: bump the Temporal runtime version and create its GitHub release
## Usage: make release BUMP=PATCH|MINOR|MAJOR
release:
	@set -eu; \
	case "$(BUMP)" in \
		MAJOR|MINOR|PATCH) ;; \
		*) echo "Usage: make release BUMP=MAJOR|MINOR|PATCH" >&2; exit 2 ;; \
	esac; \
	if [ -n "$$(git status --porcelain)" ]; then \
		echo "release requires a clean worktree" >&2; exit 1; \
	fi; \
	version=$$(cd "$(RUNTIME_DIR)" && npm version "$$(printf '%s' "$(BUMP)" | tr '[:upper:]' '[:lower:]')" --no-git-tag-version); \
	tag="$(RELEASE_TAG_PREFIX)$${version#v}"; \
	git add "$(RUNTIME_DIR)/package.json"; \
	git commit -m "release: Temporal runtime $${version#v}"; \
	git tag -a "$$tag" -m "Temporal runtime $${version#v}"; \
	gh release create "$$tag" --repo tbrandenburg/node-red-temporal --title "Temporal runtime $${version#v}" --generate-notes

## demo-run: start (or reuse) a Temporal dev server + the demo worker, both detached
demo-run:
	@mkdir -p $(PID_DIR)
	@if ss -tln 2>/dev/null | grep -q ':7233 '; then \
		echo "Temporal already listening on :7233 - reusing it"; \
	else \
		echo "Starting Temporal dev server..."; \
		setsid temporal server start-dev --db-filename $(TEMPORAL_DB) --ip 0.0.0.0 \
			> $(TEMPORAL_LOG) 2>&1 < /dev/null & echo $$! > $(TEMPORAL_PID); \
		sleep 3; \
	fi
	@if [ -f $(WORKER_PID) ] && kill -0 "$$(cat $(WORKER_PID))" 2>/dev/null; then \
		echo "Worker already running (pid $$(cat $(WORKER_PID)))"; \
	else \
		echo "Starting worker against $(DEMO_FLOW)..."; \
		setsid node $(CLI) --flow $(DEMO_FLOW) \
			> $(WORKER_LOG) 2>&1 < /dev/null & echo $$! > $(WORKER_PID); \
		sleep 2; \
	fi
	@echo ""
	@echo "Temporal Web UI : http://localhost:8233"
	@echo "Worker log      : $(WORKER_LOG)"
	@echo "Next: make demo-start"

## demo-start: trigger a new workflow execution against the running demo
demo-start:
	node $(CLI) start --flow $(DEMO_FLOW) --start-node n1 --input '{"payload":"hello"}'

## demo-status: check whether the Temporal server and the demo worker are up
demo-status:
	@echo "--- Temporal (127.0.0.1:7233) ---"
	@temporal operator cluster health --address 127.0.0.1:7233 2>&1 || true
	@echo "--- Worker ---"
	@if [ -f $(WORKER_PID) ] && kill -0 "$$(cat $(WORKER_PID))" 2>/dev/null; then \
		echo "running (pid $$(cat $(WORKER_PID)))"; \
	else \
		echo "not running"; \
	fi

## demo-stop: stop the worker and (only if we started it) the Temporal dev server
demo-stop:
	@if [ -f $(WORKER_PID) ]; then \
		kill "$$(cat $(WORKER_PID))" 2>/dev/null && echo "worker stopped" || echo "worker already stopped"; \
		rm -f $(WORKER_PID); \
	else \
		echo "no worker pidfile - nothing to stop"; \
	fi
	@if [ -f $(TEMPORAL_PID) ]; then \
		kill "$$(cat $(TEMPORAL_PID))" 2>/dev/null && echo "temporal dev server stopped" || echo "temporal already stopped"; \
		rm -f $(TEMPORAL_PID); \
	else \
		echo "no temporal pidfile (server was reused, not started by us) - leaving it running"; \
	fi
