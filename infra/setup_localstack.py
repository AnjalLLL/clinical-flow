"""Create the S3 buckets, deploy the upload-validator Lambda and wire the S3 -> Lambda trigger on LocalStack."""
import io
import zipfile
from pathlib import Path

import boto3

from clinicalflow.config import BUCKETS, S3_ENDPOINT, s3_client

FUNCTION = "clinicalflow-validate-upload"
HANDLER_FILE = Path(__file__).resolve().parents[1] / "lambdas" / "validate_upload" / "handler.py"


def lambda_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.write(HANDLER_FILE, "handler.py")
    return buf.getvalue()


def main():
    s3 = s3_client()
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for name in BUCKETS.values():
        if name not in existing:
            s3.create_bucket(Bucket=name)
    print("buckets:", ", ".join(BUCKETS.values()))

    lam = boto3.client("lambda", endpoint_url=S3_ENDPOINT, region_name=s3.meta.region_name,
                       aws_access_key_id="test", aws_secret_access_key="test")
    code = lambda_zip()
    try:
        lam.update_function_code(FunctionName=FUNCTION, ZipFile=code)
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FUNCTION, Runtime="python3.11", Handler="handler.handler", Timeout=30,
            Role="arn:aws:iam::000000000000:role/lambda-role",  # LocalStack does not enforce IAM
            Code={"ZipFile": code}, Environment={"Variables": {"QUARANTINE_BUCKET": BUCKETS["quarantine"]}},
        )
    lam.get_waiter("function_active_v2").wait(FunctionName=FUNCTION)
    arn = lam.get_function(FunctionName=FUNCTION)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=FUNCTION, StatementId="s3-invoke", Action="lambda:InvokeFunction",
                           Principal="s3.amazonaws.com", SourceArn=f"arn:aws:s3:::{BUCKETS['bronze']}")
    except lam.exceptions.ResourceConflictException:
        pass
    s3.put_bucket_notification_configuration(
        Bucket=BUCKETS["bronze"],
        NotificationConfiguration={"LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": arn, "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".csv"}]}},
        }]},
    )
    print(f"lambda {FUNCTION} deployed and triggered by s3://{BUCKETS['bronze']}/*.csv")


if __name__ == "__main__":
    main()
