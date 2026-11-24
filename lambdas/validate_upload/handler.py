"""S3-triggered Lambda: checks the header of every CSV landing in the bronze bucket.

Valid file   -> writes <key>.manifest.json next to it (the pipeline only processes files that have one)
Invalid file -> moves it to the quarantine bucket with the reason in the object metadata
"""
import csv
import io
import json
import os
from urllib.parse import unquote_plus

import boto3

REQUIRED = {
    "patients": {"Id", "BIRTHDATE", "DEATHDATE", "GENDER", "RACE", "ETHNICITY", "STATE", "COUNTY", "INCOME"},
    "encounters": {"Id", "START", "STOP", "PATIENT", "ENCOUNTERCLASS", "CODE", "DESCRIPTION",
                   "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE", "REASONCODE", "REASONDESCRIPTION"},
    "conditions": {"START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION"},
    "medications": {"START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST", "DISPENSES",
                    "TOTALCOST"},
}
QUARANTINE_BUCKET = os.getenv("QUARANTINE_BUCKET", "clinicalflow-quarantine")


def validate(key, head_bytes):
    """Return None if the file is valid, otherwise the reason it is not."""
    if not key.endswith(".csv"):
        return None if key.endswith(".manifest.json") else "not_a_csv"
    table = key.split("/", 1)[0]
    if table not in REQUIRED:
        return f"unknown_table:{table}"
    header = next(csv.reader(io.StringIO(head_bytes.decode("utf-8", errors="replace"))), [])
    missing = REQUIRED[table] - set(header)
    return f"missing_columns:{','.join(sorted(missing))}" if missing else None


def handler(event, context, s3=None):
    s3 = s3 or boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        if key.endswith(".manifest.json"):
            continue
        head = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-4095")["Body"].read()
        reason = validate(key, head)
        if reason:
            s3.copy_object(Bucket=QUARANTINE_BUCKET, Key=key, CopySource={"Bucket": bucket, "Key": key},
                           Metadata={"reason": reason}, MetadataDirective="REPLACE")
            s3.delete_object(Bucket=bucket, Key=key)
        else:
            size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
            s3.put_object(Bucket=bucket, Key=f"{key}.manifest.json",
                          Body=json.dumps({"key": key, "bytes": size, "status": "valid"}))
        print(json.dumps({"key": key, "valid": reason is None, "reason": reason}))  # goes to CloudWatch Logs
        results.append({"key": key, "valid": reason is None, "reason": reason})
    return {"results": results}
