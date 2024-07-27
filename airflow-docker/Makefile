run-airflow:
	docker compose up -d --no-attach python

run-python:
	docker compose run --rm python

regenerate-requirements:
	docker compose run python sh -c "cd ../requirements && pip-compile -U --resolver=backtracking"