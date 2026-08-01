.PHONY: help sync lint fmt fmt-check type test coverage contract demo docker-build docker-smoke site-install site-dev site-build site-deploy gate

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

sync: ## Install/sync Python dependencies (uv)
	uv sync --dev

lint: ## Ruff lint (src + tests)
	uv run ruff check src tests

fmt: ## Auto-format code
	uv run ruff format src tests

fmt-check: ## Verify formatting without changing files
	uv run ruff format --check src tests

type: ## mypy strict type check
	uv run mypy src/eurostream

test: ## Run test suite
	uv run pytest -q

coverage: ## Run tests with line coverage report
	uv run pytest --cov=eurostream --cov-report=term-missing

contract: ## Schema contract drift check vs committed baseline
	uv run eurostream contracts --baseline governance/contracts.json

demo: ## Run the end-to-end pipeline demo
	uv run eurostream demo

docker-build: ## Build the production container image
	docker build -t eurostream .

docker-smoke: ## Build image and run the demo inside a container
	docker build -t eurostream . && docker run --rm eurostream eurostream demo

site-install: ## Install docs-site dependencies
	cd site && npm install

site-dev: ## Docs-site dev server (http://localhost:4321)
	cd site && npm run dev

site-build: ## Production build of the docs site
	cd site && npm run build

site-preview: ## Preview the built docs site locally
	cd site && npm run preview

site-deploy: ## Deploy docs site to Cloudflare Pages
	cd site && npm run build && ./deploy.sh

gate: lint fmt-check type test contract ## Run the full CI quality gate locally
	@echo "✅ quality gate passed"
