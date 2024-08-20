PYTHON_IMAGE = python:3.11-slim
AIRFLOW_VERSION = 2.9.2

build:
	docker compose build
	
run:
	docker compose up -d

compile-requirements:
	docker run --rm -v $(shell pwd)/requirements:/requirements -w /requirements $(PYTHON_IMAGE) sh -c "pip install pip-tools && pip-compile -U --upgrade-package apache-airflow==$(AIRFLOW_VERSION) --resolver=backtracking"
