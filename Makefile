.PHONY: install deps run test build docs clean lint

install:
	uv sync

deps:
	dbt deps

run:
	dbt run

test:
	dbt test

build:
	dbt build

docs:
	dbt docs generate
	dbt docs serve

lint:
	sqlfluff lint models/