"""Bronze layer: land raw CSVs in S3 (LocalStack), wait for the Lambda to validate them, mirror them for Spark."""
import argparse
import shutil
import time
from datetime import date

from clinicalflow.config import BRONZE, BUCKETS, RAW_DIR, TABLES, s3_client


def create_buckets(s3):
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for name in BUCKETS.values():
        if name not in existing:
            s3.create_bucket(Bucket=name)


def upload(s3, ingest_date):
    keys = []
    for table in TABLES:
        key = f"{table}/ingest_date={ingest_date}/{table}.csv"
        # a re-run on the same day must not pass on the previous run's validation result
        s3.delete_object(Bucket=BUCKETS["bronze"], Key=f"{key}.manifest.json")
        s3.delete_object(Bucket=BUCKETS["quarantine"], Key=key)
        s3.upload_file(str(RAW_DIR / f"{table}.csv"), BUCKETS["bronze"], key)
        keys.append(key)
    return keys


def wait_for_validation(s3, keys, timeout=120):
    """Block until the validate_upload Lambda has written a manifest for every file, or quarantined it."""
    pending, deadline = set(keys), time.time() + timeout
    while pending and time.time() < deadline:
        for key in list(pending):
            if exists(s3, BUCKETS["bronze"], f"{key}.manifest.json"):
                pending.discard(key)
            elif exists(s3, BUCKETS["quarantine"], key):
                reason = s3.head_object(Bucket=BUCKETS["quarantine"], Key=key)["Metadata"].get("reason")
                raise ValueError(f"{key} was quarantined by the upload validator: {reason}")
        time.sleep(1)
    if pending:
        raise TimeoutError(f"no manifest for {sorted(pending)}; is the Lambda deployed? run `make up`")


def exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def mirror_from_s3(s3, ingest_date):
    # shortcut: Spark reads a local mirror instead of s3a://, add hadoop-aws when moving to EMR
    for table in TABLES:
        dest = BRONZE / table / f"ingest_date={ingest_date}"
        dest.mkdir(parents=True, exist_ok=True)
        key = f"{table}/ingest_date={ingest_date}/{table}.csv"
        s3.download_file(BUCKETS["bronze"], key, str(dest / f"{table}.csv"))


def copy_local(ingest_date):
    for table in TABLES:
        dest = BRONZE / table / f"ingest_date={ingest_date}"
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy(RAW_DIR / f"{table}.csv", dest / f"{table}.csv")


def run(use_s3=True, ingest_date=None):
    ingest_date = ingest_date or date.today().isoformat()
    for table in TABLES:
        if not (RAW_DIR / f"{table}.csv").exists():
            raise FileNotFoundError(f"missing {RAW_DIR / table}.csv, run `make data` first")
    if use_s3:
        s3 = s3_client()
        create_buckets(s3)
        keys = upload(s3, ingest_date)
        print(f"uploaded {len(keys)} files to s3://{BUCKETS['bronze']}, waiting for the upload validator")
        wait_for_validation(s3, keys)
        mirror_from_s3(s3, ingest_date)
        print("all files validated and mirrored to the lake")
    else:
        copy_local(ingest_date)
    return ingest_date


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--no-s3", action="store_true", help="skip LocalStack, copy files locally")
    run(use_s3=not p.parse_args().no_s3)
