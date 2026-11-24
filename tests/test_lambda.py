import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas" / "validate_upload"))
from handler import handler, validate  # noqa: E402

GOOD_HEADER = b"START,STOP,PATIENT,ENCOUNTER,SYSTEM,CODE,DESCRIPTION\n2020-01-01,,p1,e1,SNOMED,1,x\n"


class FakeS3:
    """Just enough of the S3 client for the handler: an in-memory {(bucket, key): bytes} store."""

    def __init__(self, objects):
        self.objects = dict(objects)

    def get_object(self, Bucket, Key, Range=None):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body.encode()

    def copy_object(self, Bucket, Key, CopySource, **_):
        self.objects[(Bucket, Key)] = self.objects[(CopySource["Bucket"], CopySource["Key"])]

    def delete_object(self, Bucket, Key):
        del self.objects[(Bucket, Key)]


def s3_event(key, bucket="clinicalflow-bronze"):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def test_valid_file_gets_manifest():
    key = "conditions/ingest_date=2026-11-24/conditions.csv"
    s3 = FakeS3({("clinicalflow-bronze", key): GOOD_HEADER})
    out = handler(s3_event(key), None, s3=s3)
    assert out["results"][0]["valid"]
    manifest = json.loads(s3.objects[("clinicalflow-bronze", key + ".manifest.json")])
    assert manifest == {"key": key, "bytes": len(GOOD_HEADER), "status": "valid"}


def test_file_with_missing_columns_is_moved_to_quarantine():
    key = "conditions/ingest_date=2026-11-24/conditions.csv"
    s3 = FakeS3({("clinicalflow-bronze", key): b"START,PATIENT\n2020-01-01,p1\n"})
    out = handler(s3_event(key), None, s3=s3)
    assert out["results"][0]["reason"] == "missing_columns:CODE,DESCRIPTION,ENCOUNTER,STOP"
    assert ("clinicalflow-bronze", key) not in s3.objects
    assert ("clinicalflow-quarantine", key) in s3.objects


def test_unknown_table_and_wrong_extension():
    assert validate("billing/x.csv", GOOD_HEADER) == "unknown_table:billing"
    assert validate("conditions/x.txt", GOOD_HEADER) == "not_a_csv"
    assert validate("conditions/x.csv", GOOD_HEADER) is None
