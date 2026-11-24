# ClinicalFlow

An end-to-end healthcare data pipeline on synthetic patient records. Raw CSVs land in S3, PySpark cleans
and models them through bronze → silver → gold layers, data quality checks gate every load, and a DuckDB
warehouse answers clinical analytics questions in SQL. Everything runs locally for $0: AWS services are
emulated with [LocalStack](https://github.com/localstack/localstack).

## Architecture

```
Synthea CSVs ──► S3 bronze bucket (LocalStack) ──► lake/bronze/<table>/ingest_date=YYYY-MM-DD/
                                                        │
                                   PySpark: type, standardise, de-duplicate, drop identifiers
                                                        │
                    ┌───────────────────────────────────┴──────────────────┐
                    ▼                                                      ▼
   lake/silver/<table>  (parquet, partitioned by year)     lake/quarantine/<table>  (failed rows + reason)
                    │
        PySpark: star schema
                    ▼
   lake/gold/  dim_patient · dim_date · dim_condition · fact_encounter · fact_medication · fact_condition
                    │
        data quality gate (lake/dq_report.json) ── fails ──► stop, warehouse not loaded
                    ▼
   warehouse.duckdb ──► SQL analytics (readmission, medication trends, cost per encounter)
```

## Dataset

[Synthea](https://github.com/synthetichealth/synthea) synthetic patient data (no real patients):
[sample CSV download](https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip).
The pipeline uses `patients`, `encounters`, `conditions` and `medications`. A small, referentially consistent
slice (15 patients) is committed in `data/sample/` for tests and CI.

## Run it

Needs Python 3.11, Java 17 (for Spark) and Docker.

```bash
make setup        # venv + dependencies
make data         # download the Synthea CSVs
make up           # start LocalStack
make run          # ingest to S3 → silver → gold → quality checks → DuckDB + queries
make test         # pytest
```

No Docker? `make run-local` runs the same pipeline on `data/sample/` without S3.

## What each layer does

| Layer | Code | What happens |
|---|---|---|
| Bronze | `ingest.py` | Creates the S3 buckets, uploads raw CSVs under `ingest_date=` prefixes, mirrors them to the lake |
| Silver | `transforms/bronze_to_silver.py` | Explicit types, trimmed and standardised codes, de-duplication on business keys, SSN/name/address columns dropped, rule-breaking rows quarantined with a reason |
| Gold | `transforms/silver_to_gold.py` | Star schema with surrogate `date_key`, age groups, encounter duration |
| Quality | `quality/checks.py` | Not-null, uniqueness, non-negative cost, referential integrity, and bronze = silver + quarantined + duplicates row reconciliation |
| Warehouse | `warehouse/load_duckdb.py`, `sql/` | Loads gold into DuckDB and runs the analytics SQL (window functions, CTEs) |

## Tests and CI

`tests/` covers each transform on hand-built rows (typing, quarantine reasons, de-duplication, durations,
date keys) and checks that the quality gate catches orphan records and lost rows. GitHub Actions runs ruff,
pytest and an end-to-end pipeline run on the sample data on every push.

## Roadmap

Phase 1
- [x] S3 landing zone on LocalStack, partitioned bronze layer
- [x] PySpark silver layer with quarantine and de-duplication
- [x] Gold star schema and DuckDB warehouse with analytics SQL
- [x] Data quality gate and row-count reconciliation
- [x] pytest suite and GitHub Actions CI
- [ ] S3-triggered Lambda that validates uploads before processing
- [ ] Performance notes: partition pruning, broadcast joins, CSV vs parquet
- [ ] Dashboard on Vercel with in-browser SQL (DuckDB-WASM)

Phase 2: Step Functions orchestration, Airflow DAG, Terraform, Java parity validator, Snowflake/Snowpark.
