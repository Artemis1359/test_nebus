.DEFAULT_GOAL := help

UV ?= uv
COMPOSE := docker compose

.PHONY: help env install up down ps logs lint typecheck format check test migrate migration-check api consumer

help:
	@printf '%s\n' \
	  'make install          Install locked development dependencies' \
	  'make up / down        Start / stop the Docker stack (keep data)' \
	  'make ps / logs        Show containers / follow application logs' \
	  'make check            Check lint, formatting and types' \
	  'make typecheck        Check application and migration types' \
	  'make format           Format Python code' \
	  'make test             Run all tests (Testcontainers manages Docker)' \
	  'make migrate          Apply migrations locally' \
	  'make migration-check  Check model/schema parity locally' \
	  'make api / consumer   Run API / consumer locally'

env:
	@if [ ! -f .env ]; then cp .env.example .env; fi

install:
	$(UV) sync --frozen

up: env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f api consumer

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .

typecheck:
	$(UV) run mypy

check: lint typecheck
	$(UV) run ruff format --check .

test:
	$(UV) run pytest .

migrate: env
	$(UV) run alembic upgrade head

migration-check: env
	$(UV) run alembic check

api: env
	$(UV) run uvicorn app.main:app --reload

consumer: env
	$(UV) run faststream run app.workers.consumer:app
