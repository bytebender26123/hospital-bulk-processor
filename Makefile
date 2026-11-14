.PHONY: install run lint typecheck docker-up docker-down

install:
	python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

run:
	.venv/bin/uvicorn app.main:app --reload --port 8000

lint:
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

typecheck:
	.venv/bin/mypy app

docker-up:
	docker compose up --build

docker-down:
	docker compose down
