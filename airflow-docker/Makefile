PYTHON_IMAGE = python:3.11-slim
AIRFLOW_VERSION = 2.9.2

build:
	docker compose build
	
up:
	docker compose up -d

down:
	docker compose down

purge-database:
	docker compose down -v db && docker compose build db && docker compose up -d db

compile-requirements:
	docker run --rm -v $(shell pwd)/requirements:/requirements -w /requirements $(PYTHON_IMAGE) sh -c "pip install pip-tools && pip-compile -U --upgrade-package apache-airflow==$(AIRFLOW_VERSION) --resolver=backtracking"