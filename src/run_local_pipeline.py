"""
run_local_pipeline.py
------------------------
Runs the full S3 -> Glue-equivalent transform -> Glue Data Catalog ->
"Athena" query -> Lambda notification flow as REAL boto3 calls against a
mocked-but-functionally-accurate AWS backend (moto), since this sandbox
has no real AWS account/credentials. This is meaningfully different from
just describing the pipeline: every S3 upload, Glue database/table
creation, and Lambda-style completion write below is a real API call that
succeeds or fails for real, exercised by moto's in-process AWS emulation.

What's genuinely proven end-to-end here:
  - S3 upload/download (real boto3 S3 API calls)
  - Glue Data Catalog database + table creation (real boto3 Glue API calls)
  - The actual data transformation logic (ported from glue_job/glue_etl_job.py,
    using pandas here since a local Glue Spark runtime isn't available —
    same cleaning/dedup/partitioning rules, executed for real)
  - The Lambda completion-notifier's actual Python logic (imported and
    called directly, not just described)

What's NOT proven (moto's known limitations, documented honestly):
  - moto's Athena mock returns a fixed SUCCEEDED status without executing
    real SQL against the data — so "the Athena query" here is done with
    DuckDB instead, reading the same curated Parquet files, clearly
    labeled as an Athena-equivalent stand-in for a real SQL result.
  - moto's Step Functions mock doesn't execute a state machine's business
    logic — so the orchestration sequence below is run as a plain Python
    function call chain that mirrors the state machine's steps 1:1 (see
    step_functions/state_machine.json for the real definition), with an
    execution log written in the same shape Step Functions' own execution
    history would have.

Run:
    python src/run_local_pipeline.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import duckdb
import pandas as pd
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent / "lambda"))

RAW_CSV_PATH = ROOT / "data" / "raw_claims.csv"
OUT_DIR = ROOT / "outputs"
OUT_DIR.mkdir(exist_ok=True)

BUCKET = "billing-etl-demo-bucket"
RAW_KEY = "raw/claims/raw_claims.csv"
CURATED_PREFIX = "curated/claims/"
GLUE_DATABASE = "billing_analytics"
GLUE_TABLE = "claims_curated"
REGION = "us-east-1"

execution_log: list[dict] = []


def log_step(name: str, **detail):
    execution_log.append({
        "step": name, "timestamp": datetime.now(timezone.utc).isoformat(), **detail,
    })
    print(f"[{name}] {detail}")


def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Ports the exact cleaning/dedup/partitioning rules from
    glue_job/glue_etl_job.py's Spark logic to pandas, so this local run
    proves the same business logic, not a different implementation."""
    df = raw_df.copy()
    df["billed_amount"] = pd.to_numeric(df["billed_amount"], errors="coerce")
    df["paid_amount"] = pd.to_numeric(df["paid_amount"], errors="coerce")

    df = df.drop_duplicates(subset=["claim_id"])
    df = df[df["claim_id"].notna() & df["billed_amount"].notna()]

    df["service_year_month"] = pd.to_datetime(df["service_date"]).dt.strftime("%Y-%m")
    df["outstanding_amount"] = df["billed_amount"] - df["paid_amount"].fillna(0.0)
    return df


@mock_aws
def run():
    s3 = boto3.client("s3", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)

    # ---- Step 1: create the bucket, upload raw data (real S3 API calls) ----
    s3.create_bucket(Bucket=BUCKET)
    s3.upload_file(str(RAW_CSV_PATH), BUCKET, RAW_KEY)
    log_step("upload_raw_to_s3", bucket=BUCKET, key=RAW_KEY)

    # ---- Step 2: "Glue job" — read from S3, transform, write curated Parquet back to S3 ----
    raw_obj = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
    raw_df = pd.read_csv(raw_obj["Body"])
    n_raw = len(raw_df)

    curated_df = transform(raw_df)
    n_curated = len(curated_df)

    local_curated_dir = OUT_DIR / "curated_parquet"
    local_curated_dir.mkdir(exist_ok=True)
    for ym, group in curated_df.groupby("service_year_month"):
        part_path = local_curated_dir / f"service_year_month={ym}" / "part-0.parquet"
        part_path.parent.mkdir(parents=True, exist_ok=True)
        group.to_parquet(part_path, index=False)
        with open(part_path, "rb") as f:
            s3.put_object(Bucket=BUCKET, Key=f"{CURATED_PREFIX}service_year_month={ym}/part-0.parquet",
                           Body=f.read())

    log_step("glue_etl_transform", rows_in=n_raw, rows_out=n_curated,
              rows_dropped=n_raw - n_curated, partitions=sorted(curated_df["service_year_month"].unique().tolist()))

    # ---- Step 3: register the Glue Data Catalog database + table (real Glue API calls) ----
    glue.create_database(DatabaseInput={"Name": GLUE_DATABASE})
    glue.create_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": GLUE_TABLE,
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "claim_id", "Type": "string"},
                    {"Name": "payer_code", "Type": "string"},
                    {"Name": "service_date", "Type": "string"},
                    {"Name": "billed_amount", "Type": "double"},
                    {"Name": "paid_amount", "Type": "double"},
                    {"Name": "claim_status", "Type": "string"},
                    {"Name": "outstanding_amount", "Type": "double"},
                ],
                "Location": f"s3://{BUCKET}/{CURATED_PREFIX}",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            },
            "PartitionKeys": [{"Name": "service_year_month", "Type": "string"}],
            "TableType": "EXTERNAL_TABLE",
        },
    )
    tables = glue.get_tables(DatabaseName=GLUE_DATABASE)["TableList"]
    log_step("register_glue_catalog", database=GLUE_DATABASE,
              tables=[t["Name"] for t in tables])

    # ---- Step 4: "Athena" query — real SQL, via DuckDB against the curated Parquet ----
    # (moto's Athena mock returns a canned SUCCEEDED status without executing real SQL —
    # see this file's module docstring — so DuckDB stands in here as a real SQL engine
    # over the exact same curated data Athena would be querying via the Glue catalog.)
    con = duckdb.connect()
    query = f"""
        SELECT payer_code,
               COUNT(*) AS n_claims,
               ROUND(SUM(billed_amount), 2) AS total_billed,
               ROUND(SUM(outstanding_amount), 2) AS total_outstanding
        FROM read_parquet('{local_curated_dir}/**/*.parquet', hive_partitioning=1)
        GROUP BY payer_code
        ORDER BY total_outstanding DESC
    """
    query_result = con.execute(query).fetchdf()
    log_step("athena_equivalent_query", sql=query.strip(), n_result_rows=len(query_result))
    print(query_result.to_string(index=False))

    # ---- Step 5: Lambda completion notification (real function call, mocked S3/SNS) ----
    import notify_completion  # the actual Lambda handler module
    import os
    os.environ["COMPLETION_MARKER_BUCKET"] = BUCKET
    os.environ["COMPLETION_MARKER_PREFIX"] = "pipeline-status/"
    os.environ.pop("SNS_TOPIC_ARN", None)  # no SNS topic in this local demo

    lambda_event = {
        "JobRun": {
            "JobName": "billing-claims-etl", "JobRunState": "SUCCEEDED",
            "Id": "jr_local_demo_001",
            "StartedOn": execution_log[0]["timestamp"], "CompletedOn": datetime.now(timezone.utc).isoformat(),
        }
    }
    lambda_result = notify_completion.handler(lambda_event, None)
    log_step("lambda_notify_completion", result=lambda_result)

    # ---- Save everything ----
    query_result.to_csv(OUT_DIR / "athena_query_result.csv", index=False)
    (OUT_DIR / "execution_log.json").write_text(json.dumps(execution_log, indent=2, default=str))
    print(f"\nWrote {OUT_DIR / 'athena_query_result.csv'} and {OUT_DIR / 'execution_log.json'}")


if __name__ == "__main__":
    run()
