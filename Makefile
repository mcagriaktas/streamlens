.PHONY: test test-server test-client test-integration typecheck install-server install-client install ci clean \
       docker-up docker-down

## Run unit tests (server + client)
test: test-server test-client

## Server unit tests
test-server:
	cd server && uv run pytest tests/unit -v

## Client tests
test-client:
	cd client && npm test

## Start Kafka + Schema Registry + Kafka Connect for integration tests
docker-up:
	docker compose -f container/docker-compose.yml up -d
	@echo "Waiting for services to be healthy..."
	@for i in $$(seq 1 90); do \
		healthy=$$(docker compose -f container/docker-compose.yml ps --format json 2>/dev/null | grep -o '"healthy"' | wc -l | tr -d ' '); \
		[ "$$healthy" -ge 3 ] && break || sleep 2; \
	done
	docker compose -f container/docker-compose.yml ps

## Stop Kafka + Schema Registry
docker-down:
	docker compose -f container/docker-compose.yml down -v

## Server integration tests (requires docker-up)
test-integration: docker-up
	cd server && uv run pytest tests/integration -v
	$(MAKE) docker-down

## TypeScript type check
typecheck:
	cd client && npx tsc --noEmit

## Full CI check (matches GitHub Actions)
ci: test typecheck test-integration
	cd client && npm run build

## Install dependencies
install: install-server install-client

install-server:
	cd server && uv sync --extra dev

install-client:
	cd client && npm install

## Clean build artifacts
clean: docker-down
	rm -rf client/dist server/.pytest_cache server/__pycache__ server/lib/__pycache__ server/tests/__pycache__
