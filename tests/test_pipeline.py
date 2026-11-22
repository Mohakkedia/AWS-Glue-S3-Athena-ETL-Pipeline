import os
import sys
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "local_demo" / "src"))
sys.path.insert(0, str(ROOT / "lambda"))

from run_local_pipeline import transform  # noqa: E402
import notify_completion  # noqa: E402


def test_transform_deduplicates_by_claim_id():
    df = pd.DataFrame({
        "claim_id": ["A", "A", "B"],
        "payer_code": ["P01", "P01", "P02"],
        "service_date": ["2026-08-01", "2026-08-01", "2026-08-02"],
        "billed_amount": [100, 100, 200],
        "paid_amount": [90, 90, 180],
        "claim_status": ["Paid", "Paid", "Paid"],
    })
    result = transform(df)
    assert len(result) == 2
    assert set(result["claim_id"]) == {"A", "B"}


def test_transform_drops_rows_with_missing_billed_amount():
    df = pd.DataFrame({
        "claim_id": ["A", "B"],
        "payer_code": ["P01", "P02"],
        "service_date": ["2026-08-01", "2026-08-02"],
        "billed_amount": [100, None],
        "paid_amount": [90, 50],
        "claim_status": ["Paid", "Paid"],
    })
    result = transform(df)
    assert len(result) == 1
    assert result.iloc[0]["claim_id"] == "A"


def test_transform_computes_outstanding_amount():
    df = pd.DataFrame({
        "claim_id": ["A"], "payer_code": ["P01"], "service_date": ["2026-08-01"],
        "billed_amount": [100.0], "paid_amount": [60.0], "claim_status": ["Paid"],
    })
    result = transform(df)
    assert result.iloc[0]["outstanding_amount"] == pytest.approx(40.0)


def test_transform_handles_missing_paid_amount_as_zero():
    df = pd.DataFrame({
        "claim_id": ["A"], "payer_code": ["P01"], "service_date": ["2026-08-01"],
        "billed_amount": [100.0], "paid_amount": [None], "claim_status": ["Pending"],
    })
    result = transform(df)
    assert result.iloc[0]["outstanding_amount"] == pytest.approx(100.0)


def test_transform_derives_service_year_month():
    df = pd.DataFrame({
        "claim_id": ["A"], "payer_code": ["P01"], "service_date": ["2026-08-15"],
        "billed_amount": [100.0], "paid_amount": [50.0], "claim_status": ["Paid"],
    })
    result = transform(df)
    assert result.iloc[0]["service_year_month"] == "2026-08"


@mock_aws
def test_s3_and_glue_catalog_integration():
    s3 = boto3.client("s3", region_name="us-east-1")
    glue = boto3.client("glue", region_name="us-east-1")

    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="raw/test.csv", Body=b"a,b\n1,2\n")

    glue.create_database(DatabaseInput={"Name": "test_db"})
    glue.create_table(DatabaseName="test_db", TableInput={
        "Name": "test_table",
        "StorageDescriptor": {"Columns": [{"Name": "a", "Type": "string"}], "Location": "s3://test-bucket/raw/"},
        "TableType": "EXTERNAL_TABLE",
    })

    tables = glue.get_tables(DatabaseName="test_db")["TableList"]
    assert len(tables) == 1
    assert tables[0]["Name"] == "test_table"

    obj = s3.get_object(Bucket="test-bucket", Key="raw/test.csv")
    assert obj["Body"].read() == b"a,b\n1,2\n"


@mock_aws
def test_lambda_handler_writes_completion_marker_to_s3():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="marker-bucket")

    os.environ["COMPLETION_MARKER_BUCKET"] = "marker-bucket"
    os.environ["COMPLETION_MARKER_PREFIX"] = "status/"
    os.environ.pop("SNS_TOPIC_ARN", None)

    event = {"JobRun": {"JobName": "test-job", "JobRunState": "SUCCEEDED", "Id": "jr_test_001"}}
    result = notify_completion.handler(event, None)

    assert result["statusCode"] == 200
    assert "marker_location" in result["body"]

    objects = s3.list_objects_v2(Bucket="marker-bucket", Prefix="status/")
    assert objects["KeyCount"] == 1


@mock_aws
def test_lambda_handler_reports_failure_status_code():
    os.environ.pop("SNS_TOPIC_ARN", None)
    os.environ.pop("COMPLETION_MARKER_BUCKET", None)
    event = {"JobRun": {"JobName": "test-job", "JobRunState": "FAILED", "Id": "jr_test_002"}}
    result = notify_completion.handler(event, None)
    assert result["statusCode"] == 500


if __name__ == "__main__":
    test_transform_deduplicates_by_claim_id()
    test_transform_drops_rows_with_missing_billed_amount()
    test_transform_computes_outstanding_amount()
    test_transform_handles_missing_paid_amount_as_zero()
    test_transform_derives_service_year_month()
    test_s3_and_glue_catalog_integration()
    test_lambda_handler_writes_completion_marker_to_s3()
    test_lambda_handler_reports_failure_status_code()
    print("All tests passed.")
