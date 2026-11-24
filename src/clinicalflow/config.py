import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = Path(os.getenv("RAW_DIR", ROOT / "data" / "raw" / "csv"))
LAKE = Path(os.getenv("LAKE_DIR", ROOT / "lake"))
BRONZE, SILVER, GOLD, QUARANTINE = (LAKE / n for n in ("bronze", "silver", "gold", "quarantine"))
WAREHOUSE = Path(os.getenv("WAREHOUSE_PATH", ROOT / "warehouse.duckdb"))
DQ_REPORT = LAKE / "dq_report.json"

TABLES = ("patients", "encounters", "conditions", "medications")
BUCKETS = {n: f"clinicalflow-{n}" for n in ("bronze", "silver", "gold", "quarantine")}
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")


def s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def spark_session(app="clinicalflow"):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers must use the same Python as the driver

    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
