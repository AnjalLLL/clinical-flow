"""Load gold parquet into a DuckDB warehouse file and run the analytics queries."""
from pathlib import Path

import duckdb

from clinicalflow.config import GOLD, WAREHOUSE

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
GOLD_TABLES = ("dim_patient", "dim_date", "dim_condition", "fact_encounter", "fact_medication", "fact_condition")


def load(con):
    for name in GOLD_TABLES:
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS "
            f"SELECT * FROM read_parquet('{GOLD / name}/**/*.parquet', hive_partitioning = true)"
        )
        print(f"warehouse.{name}: {con.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]} rows")


def run_queries(con):
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        print(f"\n-- {sql_file.stem}")
        print(con.sql(sql_file.read_text()))


def run():
    with duckdb.connect(str(WAREHOUSE)) as con:
        load(con)
        run_queries(con)


if __name__ == "__main__":
    run()
