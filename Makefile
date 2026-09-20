# Convenience targets for running this project's primary UX (issue #39): an
# ordinary Node-RED editor (A) whose Deploy button ships flows to a separate
# Temporal-backed runner (B) (packages/node_modules/@tbrandenburg/
# node-red-temporal-runtime).
#
# These do NOT replace the repo's own npm scripts (npm install/test/lint/
# build) - they only wrap the editor + runner + Temporal dev server
# lifecycle so it doesn't need 3 manually-managed terminals.
#
# issue #43: renamed from make demo-run/demo-start/demo-status/demo-stop.
# The old demo-* targets ran ONE combined worker against a static
# demo/flows.json and never exercised issue #39's editor-to-runner path.
# `make run` now boots TWO processes (editor A + runner B) and defaults to
# an EMPTY flow - the editor is the way you create/edit flows, exactly like
# a normal Node-RED install. `demo/flows.json` and friends are untouched and
# remain valid fixtures for manual/E2E testing; pass FLOW= to seed runner
# B's initial flow from one of them.

RUNTIME_DIR := packages/node_modules/@tbrandenburg/node-red-temporal-runtime
CLI         := $(RUNTIME_DIR)/bin/node-red-temporal
EDITOR      := packages/node_modules/node-red/red.js
SETTINGS_GEN := $(RUNTIME_DIR)/run/generate-editor-settings.js
RELEASE_TAG_PREFIX := temporal-v

# Editor A's userDir is the persistent "current flow" source of truth across
# restarts, exactly like a normal Node-RED install - kept in-repo (gitignored)
# rather than /tmp so it survives reboots the way ~/.node-red normally would.
EDITOR_USERDIR := .node-red-temporal/editor-userdir
# Runner B's userDir (issue #46): same rationale as EDITOR_USERDIR - a stable,
# gitignored install so installed node-red-contrib-* modules, credentials,
# and persisted context survive `make stop`/`make run` cycles instead of
# living in an ephemeral temp dir.
RUNNER_USERDIR := .node-red-temporal/runner-userdir
EDITOR_PORT    := 1880
ADMIN_PORT     := 1881
ADMIN_HOST     := 127.0.0.1

PID_DIR        := /tmp/node-red-temporal-run
TEMPORAL_PID   := $(PID_DIR)/temporal.pid
TEMPORAL_LOG   := $(PID_DIR)/temporal.log
TEMPORAL_DB    := $(PID_DIR)/temporal.db
RUNNER_PID     := $(PID_DIR)/runner.pid
RUNNER_LOG     := $(PID_DIR)/runner.log
EDITOR_PID     := $(PID_DIR)/editor.pid
EDITOR_LOG     := $(PID_DIR)/editor.log
EMPTY_FLOW     := $(PID_DIR)/empty-flows.json

.PHONY: publish release run start status stop demo-run demo-start demo-status demo-stop

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

## run: start (or reuse) Temporal + runner B + editor A, all detached.
## Usage: make run [FLOW=path/to/flows.json]
##   FLOW= seeds runner B's INITIAL flow on first boot only; omit it to boot
##   B with an empty flow and create everything from editor A's Deploy button.
run:
	@mkdir -p $(PID_DIR) $(EDITOR_USERDIR) $(RUNNER_USERDIR)
	@if ss -tln 2>/dev/null | grep -q ':7233 '; then \
		echo "Temporal already listening on :7233 - reusing it"; \
	else \
		echo "Starting Temporal dev server..."; \
		setsid temporal server start-dev --db-filename $(TEMPORAL_DB) --ip 0.0.0.0 \
			> $(TEMPORAL_LOG) 2>&1 < /dev/null & echo $$! > $(TEMPORAL_PID); \
		sleep 3; \
	fi
	@if [ -n "$(FLOW)" ]; then \
		seed_flow="$(FLOW)"; \
	else \
		[ -f $(EMPTY_FLOW) ] || echo '[]' > $(EMPTY_FLOW); \
		seed_flow="$(EMPTY_FLOW)"; \
	fi; \
	if [ -f $(RUNNER_PID) ] && kill -0 "$$(cat $(RUNNER_PID))" 2>/dev/null; then \
		echo "Runner B already running (pid $$(cat $(RUNNER_PID)))"; \
	else \
		echo "Starting runner B (admin API on $(ADMIN_HOST):$(ADMIN_PORT)) seeded from $$seed_flow..."; \
		setsid node $(CLI) worker --role activity --flow "$$seed_flow" \
			--admin-port $(ADMIN_PORT) --admin-host $(ADMIN_HOST) \
			--user-dir $(RUNNER_USERDIR) \
			> $(RUNNER_LOG) 2>&1 < /dev/null & echo $$! > $(RUNNER_PID); \
		sleep 2; \
	fi
	@node $(SETTINGS_GEN) --user-dir $(EDITOR_USERDIR) \
		--target http://$(ADMIN_HOST):$(ADMIN_PORT) --port $(EDITOR_PORT)
	@if [ -f $(EDITOR_PID) ] && kill -0 "$$(cat $(EDITOR_PID))" 2>/dev/null; then \
		echo "Editor A already running (pid $$(cat $(EDITOR_PID)))"; \
	else \
		echo "Starting editor A on :$(EDITOR_PORT)..."; \
		setsid node $(EDITOR) --userDir $(EDITOR_USERDIR) --settings $(EDITOR_USERDIR)/settings.js --port $(EDITOR_PORT) \
			> $(EDITOR_LOG) 2>&1 < /dev/null & echo $$! > $(EDITOR_PID); \
		sleep 2; \
	fi
	@echo ""
	@echo "Editor A (design time, press Deploy) : http://localhost:$(EDITOR_PORT)"
	@echo "Runner B admin API                   : http://$(ADMIN_HOST):$(ADMIN_PORT)"
	@echo "Temporal Web UI                       : http://localhost:8233"
	@echo "Editor log                           : $(EDITOR_LOG)"
	@echo "Runner log                           : $(RUNNER_LOG)"
	@echo "Next: open the editor, build/edit a flow, press Deploy."

## status: report Temporal, runner B, and editor A's process/port state
status:
	@echo "--- Temporal (127.0.0.1:7233) ---"
	@temporal operator cluster health --address 127.0.0.1:7233 2>&1 || true
	@echo "--- Runner B (pid + admin API :$(ADMIN_PORT)) ---"
	@if [ -f $(RUNNER_PID) ] && kill -0 "$$(cat $(RUNNER_PID))" 2>/dev/null; then \
		echo "process running (pid $$(cat $(RUNNER_PID)))"; \
	else \
		echo "process not running"; \
	fi
	@curl -sf -o /dev/null http://$(ADMIN_HOST):$(ADMIN_PORT)/flows && echo "admin API reachable" || echo "admin API not reachable"
	@echo "--- Editor A (pid + editor :$(EDITOR_PORT)) ---"
	@if [ -f $(EDITOR_PID) ] && kill -0 "$$(cat $(EDITOR_PID))" 2>/dev/null; then \
		echo "process running (pid $$(cat $(EDITOR_PID)))"; \
	else \
		echo "process not running"; \
	fi
	@curl -sf -o /dev/null http://localhost:$(EDITOR_PORT) && echo "editor reachable" || echo "editor not reachable"

## stop: stop editor A and runner B, and (only if we started it) Temporal
stop:
	@if [ -f $(EDITOR_PID) ]; then \
		kill "$$(cat $(EDITOR_PID))" 2>/dev/null && echo "editor A stopped" || echo "editor A already stopped"; \
		rm -f $(EDITOR_PID); \
	else \
		echo "no editor pidfile - nothing to stop"; \
	fi
	@if [ -f $(RUNNER_PID) ]; then \
		kill "$$(cat $(RUNNER_PID))" 2>/dev/null && echo "runner B stopped" || echo "runner B already stopped"; \
		rm -f $(RUNNER_PID); \
	else \
		echo "no runner pidfile - nothing to stop"; \
	fi
	@if [ -f $(TEMPORAL_PID) ]; then \
		kill "$$(cat $(TEMPORAL_PID))" 2>/dev/null && echo "temporal dev server stopped" || echo "temporal already stopped"; \
		rm -f $(TEMPORAL_PID); \
	else \
		echo "no temporal pidfile (server was reused, not started by us) - leaving it running"; \
	fi

## start: REMOVED (issue #43) - the old demo-start CLI trigger no longer
## applies now that `make run` boots a real editor. Open the editor and
## press Deploy/Inject instead.
start:
	@echo "make start was removed (issue #43): open the editor at http://localhost:$(EDITOR_PORT)" >&2
	@echo "and use its Deploy button / Inject nodes instead of a CLI trigger." >&2
	@exit 1

## demo-run/demo-start/demo-status/demo-stop: renamed (issue #43)
demo-run demo-start demo-status demo-stop:
	@echo "'make $@' was renamed to 'make $(subst demo-,,$@)' (issue #43): see AGENTS.md/README.md." >&2
	@exit 1

# --- Docker dev onboarding (issue #52) ---------------------------------
#
# These targets run the SAME editor A + runner B lifecycle as `make run`/
# `make status`/`make stop` above, just inside the `dev` container defined
# in compose.yaml, via `docker compose exec ... make run` etc. They do not
# reimplement or duplicate that lifecycle - they only add the container
# start/stop/wait plumbing around it.
#
# ADMIN_HOST decision (verified live during S3): the Makefile's `run` target
# binds runner B's admin API to `ADMIN_HOST := 127.0.0.1` (hardcoded).
# Inside the `dev` container this is fine and unchanged - editor A and
# runner B both run inside the SAME container and talk to each other over
# its own loopback, exactly like the non-Docker lifecycle. Docker's
# `1881:1881` port publish (compose.yaml) cannot reach a loopback-only bind
# from the host, but the issue's acceptance criteria only requires the
# editor (1880) and Temporal UI (8233) to be host-reachable, and Deploy
# works end-to-end through 1880 without host access to 1881. ADMIN_HOST is
# therefore left untouched - making it host-reachable is unneeded scope.

COMPOSE := docker compose

# Default namespace for `make docker-run-external` when the caller doesn't
# set TEMPORAL_NAMESPACE explicitly (?= leaves an env/CLI-supplied value
# untouched).
TEMPORAL_NAMESPACE ?= default

.PHONY: docker-run docker-run-external docker-stop docker-status docker-shell docker-logs

## docker-run: start the local Docker stack and boot editor A + runner B
## inside the `dev` container via the existing `make run` lifecycle.
docker-run:
	@$(COMPOSE) up -d --build postgres temporal temporal-ui dev
	@echo "Waiting for temporal and dev services..."
	@i=0; \
	while [ $$i -lt 60 ]; do \
		temporal_health=$$($(COMPOSE) ps --format json temporal 2>/dev/null | grep -o '"Health":"[a-z]*"' | head -1); \
		dev_state=$$($(COMPOSE) ps --format json dev 2>/dev/null | grep -o '"State":"[a-z]*"' | head -1); \
		if echo "$$temporal_health" | grep -q healthy && echo "$$dev_state" | grep -q running; then \
			echo "temporal healthy, dev running"; \
			break; \
		fi; \
		i=$$((i+1)); \
		sleep 2; \
	done
	@$(COMPOSE) exec -T dev make run
	@echo ""
	@echo "Node-RED editor : http://localhost:$(EDITOR_PORT)"
	@echo "Temporal UI     : http://localhost:8233"

## docker-run-external: start ONLY the `dev` service (never postgres/temporal/
## temporal-ui) and boot editor A + runner B inside it against an externally
## supplied Temporal server (Mode B, issue #52). Requires TEMPORAL_ADDRESS.
##
## `--no-deps` stops compose from auto-starting `dev`'s `depends_on: temporal`
## (verified live during S4: `docker compose up -d --no-deps dev` creates
## only the `dev` container, `temporal`/`postgres`/`temporal-ui` are never
## created). The external TEMPORAL_ADDRESS/TEMPORAL_NAMESPACE are passed via
## `docker compose exec -e ...`, which overrides the `dev` service's own
## `environment:` block (TEMPORAL_ADDRESS: temporal:7233) for this exec only
## - compose.yaml's own defaults are untouched for `make docker-run`.
## For host.docker.internal:<port> external addresses on Linux, compose.yaml's
## `dev` service carries a matching `extra_hosts: host.docker.internal:
## host-gateway` (verified live during S4).
docker-run-external:
	@if [ -z "$(TEMPORAL_ADDRESS)" ]; then \
		echo "ERROR: TEMPORAL_ADDRESS must be set, e.g.:" >&2; \
		echo "  TEMPORAL_ADDRESS=host.docker.internal:7233 make docker-run-external" >&2; \
		exit 1; \
	fi
	@$(COMPOSE) up -d --build --no-deps dev
	@echo "Waiting for dev service..."
	@i=0; \
	while [ $$i -lt 60 ]; do \
		dev_state=$$($(COMPOSE) ps --format json dev 2>/dev/null | grep -o '"State":"[a-z]*"' | head -1); \
		if echo "$$dev_state" | grep -q running; then \
			echo "dev running"; \
			break; \
		fi; \
		i=$$((i+1)); \
		sleep 2; \
	done
	@$(COMPOSE) exec -T \
		-e TEMPORAL_ADDRESS="$(TEMPORAL_ADDRESS)" \
		-e TEMPORAL_NAMESPACE="$(TEMPORAL_NAMESPACE)" \
		dev make run
	@echo ""
	@echo "Connected to external Temporal : $(TEMPORAL_ADDRESS) (namespace: $(TEMPORAL_NAMESPACE))"
	@echo "Node-RED editor                : http://localhost:$(EDITOR_PORT)"

## docker-stop: stop editor A + runner B inside the dev container, then
## stop/remove the compose stack (volumes preserved - never passes -v).
docker-stop:
	@if $(COMPOSE) ps --format json dev 2>/dev/null | grep -q '"State":"running"'; then \
		$(COMPOSE) exec -T dev make stop || true; \
	else \
		echo "dev service not running - skipping in-container stop"; \
	fi
	@$(COMPOSE) down

## docker-status: report compose service state, plus editor A/runner B
## state inside the dev container if it is up.
docker-status:
	@$(COMPOSE) ps
	@if $(COMPOSE) ps --format json dev 2>/dev/null | grep -q '"State":"running"'; then \
		echo ""; \
		$(COMPOSE) exec -T dev make status; \
	fi

## docker-shell: open an interactive shell inside the dev container.
docker-shell:
	@$(COMPOSE) exec dev bash

## docker-logs: follow logs for the dev and temporal services (the two
## most relevant for onboarding); use `docker compose logs -f` directly for
## the full multi-service stream.
docker-logs:
	@$(COMPOSE) logs -f dev temporal
