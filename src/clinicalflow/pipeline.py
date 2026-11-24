"""Run the pipeline end to end: ingest -> silver -> gold -> data quality -> warehouse -> export."""
import argparse
import sys
import time

from clinicalflow import export, ingest
from clinicalflow.config import spark_session
from clinicalflow.quality import checks
from clinicalflow.transforms import bronze_to_silver, silver_to_gold
from clinicalflow.warehouse import load_duckdb

TIMINGS = {}


def stage(name, fn, *args, **kwargs):
    start = time.perf_counter()
    print(f"\n=== {name} ===")
    result = fn(*args, **kwargs)
    TIMINGS[name] = round(time.perf_counter() - start, 2)
    print(f"--- {name} done in {TIMINGS[name]}s")
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--no-s3", action="store_true", help="skip LocalStack, copy raw files locally")
    args = p.parse_args()

    stage("ingest", ingest.run, use_s3=not args.no_s3)
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    stats = stage("silver", bronze_to_silver.run, spark)
    stage("gold", silver_to_gold.run, spark)
    failed = stage("quality", checks.run, spark, stats)
    spark.stop()
    if failed:
        print("data quality failed, warehouse not loaded:\n  " + "\n  ".join(failed))
        sys.exit(1)
    stage("warehouse", load_duckdb.run)
    export.run(TIMINGS)


if __name__ == "__main__":
    main()
