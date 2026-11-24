PY = .venv/bin/python
export PYTHONPATH = src
DATA_URL = https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip

.PHONY: setup data up down ingest run run-local bench test lint clean

setup:            ## create venv and install deps (needs Python 3.11 + Java 17)
	python3.11 -m venv .venv && $(PY) -m pip install -q -r requirements.txt

data:             ## download Synthea sample CSVs into data/raw/csv
	mkdir -p data/raw/csv && curl -sL -o data/raw/synthea.zip $(DATA_URL)
	cd data/raw && unzip -oj synthea.zip '*patients.csv' '*encounters.csv' '*conditions.csv' '*medications.csv' -d csv

up:               ## start LocalStack, create buckets, deploy the upload-validator Lambda
	docker compose up -d --wait
	$(PY) infra/setup_localstack.py

down:
	docker compose down

ingest:           ## upload raw CSVs to S3 bronze
	$(PY) -m clinicalflow.ingest

run:              ## full pipeline with LocalStack S3, exports the dashboard data to web/data
	$(PY) -m clinicalflow.pipeline

run-local:        ## full pipeline without Docker, on the committed sample data
	RAW_DIR=data/sample $(PY) -m clinicalflow.pipeline --no-s3

bench:            ## Spark performance experiments -> docs/performance.md
	$(PY) benchmarks/benchmark.py

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests benchmarks lambdas infra

clean:
	rm -rf lake warehouse.duckdb spark-warehouse
