# ClinicalFlow — Phase 1 Plan (7 days)

End-to-end healthcare data pipeline on synthetic patient data. Runs fully local at $0
(PySpark + LocalStack), with a live dashboard on Vercel.

## Scope

**Phase 1 covers:** Python, SQL, PySpark, Spark SQL partitioned tables (Hive-style), S3 + Lambda
(LocalStack), bronze/silver/gold ETL, star-schema data modeling, data quality checks, pytest,
Git + GitHub Actions CI, DuckDB (Snowflake-style warehouse), query performance tuning, Vercel dashboard.

**Phase 2 (later):** Step Functions, Airflow, Terraform, Java validator, Snowflake/Snowpark,
full Hive metastore. Running Step Functions with Spark on LocalStack is fragile, so it waits.

---

## Datasets (free, synthetic, safe to publish)

| What | Link | Use |
|---|---|---|
| Synthea sample CSV (~1k patients), main dataset | https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip | Day 1 |
| Synthea generator (Java jar) | https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar | 10k–50k patients for performance numbers |
| Synthea docs (column meanings) | https://github.com/synthetichealth/synthea | Reference |
| CMS DE-SynPUF (synthetic Medicare claims) | https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files | Phase 2 second source |

Generate more data:
```bash
java -jar synthea-with-dependencies.jar -p 20000 --exporter.csv.export true
```

Phase 1 tables: `patients.csv`, `encounters.csv`, `conditions.csv`, `medications.csv`.

---

## Architecture

```
Synthea CSV → ingest.py → S3 bronze (LocalStack)
                 └─ S3 event → Lambda validate_upload → manifest OR quarantine
bronze → [Spark] → silver (typed, deduped, partitioned parquet + Spark SQL tables)
       → [DQ checks] → [Spark] → gold (star schema + metrics) → [DQ checks]
       → DuckDB warehouse → export → web/public/data → Vercel (DuckDB-WASM)
GitHub Actions: CI (lint + pytest) and scheduled pipeline run that commits gold data
```

---

## Folder structure

```
clinicalflow/
├── PLAN.md
├── README.md                  # architecture diagram, how to run, results
├── Makefile                   # make up / ingest / run / test / export / down
├── docker-compose.yml         # LocalStack (S3 + Lambda + logs)
├── requirements.txt           # pyspark, boto3, pandera, duckdb, pytest, ruff
├── .env.example               # AWS_ENDPOINT_URL=http://localhost:4566, fake keys
├── .github/workflows/
│   ├── ci.yml                 # lint + pytest on every push
│   └── pipeline.yml           # weekly/manual: run pipeline, export gold, commit
├── data/
│   ├── raw/                   # downloaded Synthea CSVs (gitignored)
│   └── sample/                # tiny CSVs for tests + CI (committed)
├── infra/
│   └── setup_localstack.sh    # buckets, deploy Lambda, S3 trigger
├── lambdas/
│   └── validate_upload/
│       └── handler.py         # header/schema check → manifest or quarantine
├── src/clinicalflow/
│   ├── config.py              # paths, bucket names, Spark session builder
│   ├── ingest.py              # raw CSV → s3://clinicalflow-bronze
│   ├── transforms/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   ├── quality/
│   │   └── checks.py          # pandera + Spark checks → dq_report.json
│   ├── warehouse/
│   │   └── load_duckdb.py     # gold parquet → warehouse.duckdb
│   ├── sql/
│   │   ├── readmission_30d.sql
│   │   ├── med_trends.sql
│   │   └── cost_per_encounter.sql
│   ├── export.py              # gold → web/public/data/*.parquet + run_report.json
│   └── pipeline.py            # CLI: runs all stages with logging + timing
├── tests/
│   ├── conftest.py            # local SparkSession fixture
│   ├── test_bronze_to_silver.py
│   ├── test_silver_to_gold.py
│   ├── test_quality.py
│   └── test_lambda.py
├── docs/
│   ├── architecture.png
│   └── performance.md         # EXPLAIN before/after, timings
└── web/                       # Next.js on Vercel
    ├── public/data/           # gold parquet + run_report.json (committed by CI)
    └── app/
        ├── page.tsx           # overview + last pipeline run
        ├── insights/page.tsx  # charts
        ├── quality/page.tsx   # DQ results
        └── sql/page.tsx       # DuckDB-WASM SQL playground
```

---

## Day-by-day

### Day 1 — Setup + ingestion to S3
- [ ] Install Java 17, Python 3.11, Docker
- [ ] Repo, `requirements.txt`, `docker-compose.yml` (LocalStack `SERVICES=s3,lambda,logs`)
- [ ] Download Synthea CSVs → `data/raw/`; copy 50 rows of each → `data/sample/`
- [ ] `infra/setup_localstack.sh`: buckets `clinicalflow-bronze`, `-silver`, `-gold`, `-quarantine`
- [ ] `ingest.py`: boto3 with `endpoint_url` → `bronze/<table>/ingest_date=YYYY-MM-DD/`

**Done:** `make up && make ingest` shows files in LocalStack S3.
**Learn:** S3 key layout, partition prefixes, boto3, LocalStack vs real AWS.

### Day 2 — Bronze → Silver (PySpark)
- [ ] Spark session reading `s3a://` via LocalStack (2-hour limit; else use local paths)
- [ ] Per table: cast types, trim/standardize codes, dedupe on ID, handle nulls, add `_ingested_at`, `_source_file`
- [ ] Write parquet partitioned by `year`; register Spark SQL tables (`saveAsTable`)

**Done:** `spark.sql("select count(*) from silver.encounters")` works.
**Learn:** DataFrames, lazy evaluation, partitioning, parquet, schema-on-read, Spark tables ≈ Hive tables.

### Day 3 — Silver → Gold (modeling + SQL)
- [ ] Star schema: `dim_patient`, `dim_date`, `dim_condition`, `fact_encounter`, `fact_medication`
- [ ] Metrics: 30-day readmission rate (`LEAD` window), medication trends by month, avg cost per encounter class
- [ ] `load_duckdb.py` → `warehouse.duckdb`

**Done:** all 3 queries return sensible numbers in DuckDB.
**Learn:** facts vs dimensions, window functions, CTEs, warehouse vs lake.

### Day 4 — Data quality + pytest
- [ ] Checks: non-null PKs, unique IDs, valid date ranges, `cost >= 0`, referential integrity, row-count reconciliation bronze→silver→gold
- [ ] Failed rows → quarantine with `reason`; write `dq_report.json`
- [ ] Unit tests on 5 hand-made rows per transform; DQ tests on deliberately bad input

**Done:** `make test` passes; a broken CSV is quarantined.
**Learn:** data quality dimensions, pytest fixtures, testing Spark, reconciliation (= migration validation).

### Day 5 — Lambda trigger + pipeline runner
- [ ] `validate_upload/handler.py`: on S3 PUT, check header → `_manifest.json` or quarantine; logs to CloudWatch (LocalStack)
- [ ] Setup script zips + deploys Lambda, adds S3 notification
- [ ] `pipeline.py`: ingest → silver → DQ → gold → DQ → load → export, timing per stage, stop on failure
- [ ] pytest for handler with a fake S3 event

**Done:** bad upload → quarantined; `make run` runs end to end.
**Learn:** event-driven design, Lambda, S3 notifications, CloudWatch logs, idempotent pipelines.

### Day 6 — CI/CD + performance tuning
- [ ] `ci.yml`: ruff + pytest (setup-java in Actions)
- [ ] `pipeline.yml`: LocalStack service, run pipeline on sample data, `export.py`, commit `web/public/data/`
- [ ] `docs/performance.md`: unpartitioned vs partitioned (EXPLAIN + timing), broadcast join, CSV vs parquet

**Done:** CI green; pipeline workflow commits data; real before/after numbers.
**Learn:** GitHub Actions, CI/CD for data, Spark query plans, broadcast joins, file formats.

### Day 7 — Vercel dashboard + README
- [ ] `npx create-next-app web`; Recharts on exported data
- [ ] SQL page with `@duckdb/duckdb-wasm` + 3 example query buttons
- [ ] Vercel: root dir `web/`; add subdomain (CNAME → `cname.vercel-dns.com`)
- [ ] README: diagram, one-command run, results, Phase 2 roadmap; record 2-min demo

**Done:** subdomain live, repo + demo video ready.
**Learn:** static data apps, DuckDB-WASM, presenting technical work.

---

## Phase 2 (~1 day each)
1. Step Functions on LocalStack (Lambda steps with Retry/Catch/Choice)
2. Airflow DAG of the same stages (`airflow standalone`)
3. Terraform with `tflocal` replacing `setup_localstack.sh`
4. Java parity validator (DuckDB vs parquet counts + checksums)
5. Snowflake free trial: Stages + Snowpark transform
6. CMS DE-SynPUF claims as second source

## Risks
- Spark ↔ LocalStack s3a (Day 2): 2-hour limit, then local paths.
- Apple Silicon: `localstack/localstack` image supports ARM.
- Keep committed web data under ~10 MB (export the 1k-patient run).
