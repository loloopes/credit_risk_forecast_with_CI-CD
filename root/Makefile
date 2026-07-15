.PHONY: setup setup-fresh local-up local-up-fresh local-down local-status local-logs-llm local-grafana

# One command: prerequisites, env files, start full stack, health checks.
setup:
	bash scripts/local-platform-setup.sh

# Recreate every container (compose edits, env changes, new volume mounts).
setup-fresh:
	LOCAL_FORCE_RECREATE=1 bash scripts/local-platform-setup.sh

local-up:
	bash scripts/local-platform-up.sh

local-up-fresh:
	LOCAL_FORCE_RECREATE=1 bash scripts/local-platform-up.sh

local-down:
	bash scripts/local-platform-down.sh

local-status:
	bash scripts/local-platform-status.sh

local-logs-llm:
	cd llm && docker compose logs -f llm-api llm-langgraph-api

local-grafana:
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"
