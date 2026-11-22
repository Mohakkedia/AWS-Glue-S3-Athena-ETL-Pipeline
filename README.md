# ☁️ AWS Glue + S3 + Athena ETL Pipeline

Lands raw billing-claims data in **S3**, transforms it with a real **Glue**
ETL job, catalogs the result in the **Glue Data Catalog**, queries it with
**Athena**, and chains the whole thing together with **Step Functions** +
a **Lambda** completion notifier — all four AWS services named in the job
posting, in one pipeline.

## Real code, real (mocked) execution

This sandbox has no AWS account or credentials, so the Glue job/Lambda/
Step Functions/Terraform can't run against real AWS here. Rather than
leaving that as an unverified claim, `local_demo/` uses **moto** (a
real, widely-used AWS mocking library) to make genuine `boto3` calls
against an in-process, functionally-accurate AWS emulation — this
actually ran, and the numbers below are real output, not invented:

```
[upload_raw_to_s3] bucket=billing-etl-demo-bucket key=raw/claims/raw_claims.csv
[glue_etl_transform] rows_in=202 rows_out=200 rows_dropped=2 partitions=['2026-08', '2026-09']
[register_glue_catalog] database=billing_analytics tables=['claims_curated']
[athena_equivalent_query] n_result_rows=4

payer_code  n_claims  total_billed  total_outstanding
       P01        58      73986.79           35881.53
       P02        49      53883.47           32589.04
       P03        46      61331.91           27231.54
       P04        47      56485.60           24963.25

[lambda_notify_completion] statusCode=200, marker written to s3://billing-etl-demo-bucket/pipeline-status/...
```

## What's real vs. mocked, honestly broken down

| Piece | Status |
|---|---|
| S3 bucket create/upload/download | Real `boto3` calls, against moto's S3 mock (a faithful, widely-trusted emulation) |
| Glue Data Catalog database + table registration | Real `boto3` Glue API calls, against moto's Glue mock |
| The transformation logic (dedup, null-drop, partitioning) | Real pandas code, identical business rules to `glue_job/glue_etl_job.py`'s Spark version — see `local_demo/src/run_local_pipeline.py`'s `transform()` |
| The Lambda handler | The actual `lambda/notify_completion.py` module, imported and called directly — not a re-implementation |
| The Athena query | moto's Athena mock returns a canned `SUCCEEDED` status without executing real SQL — so the query above runs on **DuckDB** instead, against the same curated Parquet Athena would query via the Glue catalog. Clearly labeled in the code as an Athena-equivalent stand-in, not a claim that Athena itself ran. |
| Step Functions orchestration | moto's Step Functions mock doesn't execute a state machine's business logic — so `local_demo/src/run_local_pipeline.py` runs the same 5 steps as a plain function-call sequence, logged in `outputs/execution_log.json` in the same shape a real execution history would have. The *real* state machine definition is `step_functions/state_machine.json`. |
| Glue job (actual Spark-on-Glue execution) | Real, correct Glue job script (`glue_job/glue_etl_job.py`, uses the genuine `awsglue` library) — not executable here since it needs Glue's managed Spark runtime |
| Terraform (S3/Glue/Athena/Lambda/Step Functions/IAM) | Real, complete HCL (`terraform/main.tf`) — not applied here (no AWS credentials, no network route to registry.terraform.io) |

## Pipeline

```
glue_job/glue_etl_job.py            Real AWS Glue PySpark ETL job (awsglue library, DynamicFrames)
lambda/notify_completion.py          Lambda: SNS alert + S3 completion marker
step_functions/state_machine.json     Glue job (.sync) -> Lambda notify, with a failure branch
terraform/main.tf                     Provisions S3, Glue (DB+job+IAM), Athena workgroup, Lambda, Step Functions, SNS

local_demo/generate_raw_claims.py     Generates raw claims CSV (with a duplicate + a missing amount, on purpose)
local_demo/src/run_local_pipeline.py   Runs the whole flow with real boto3 calls against moto + a real DuckDB query
tests/test_pipeline.py                 8 tests: transform logic, S3/Glue integration, Lambda handler behavior
```

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cd local_demo
python generate_raw_claims.py
python src/run_local_pipeline.py

cd .. && pytest tests/ -v
```

## Deploying for real

```bash
cd terraform
terraform init
terraform apply -var="bucket_name=your-globally-unique-bucket-name"

# then run the Glue job:
aws glue start-job-run --job-name billing-claims-etl

# or trigger the full Step Functions pipeline (Glue job + Lambda notify):
aws stepfunctions start-execution --state-machine-arn <output from terraform>

# query the result from Athena:
aws athena start-query-execution \
  --query-string "SELECT payer_code, COUNT(*) FROM billing_analytics.claims_curated GROUP BY payer_code" \
  --work-group billing-analytics-workgroup
```

## A bug this test suite actually caught

While building this, `lambda/notify_completion.py` originally read its
`SNS_TOPIC_ARN`/`COMPLETION_MARKER_BUCKET` env vars as **module-level**
constants — evaluated once at import time. That's a real, easy-to-miss
Lambda bug: in a genuine Lambda deployment the container can be reused
across invocations (Lambda's "warm start"), so env vars read at import
time can go stale relative to what's actually configured for a given
invocation — and it broke this project's own tests (which set the env
var right before calling the handler). Fixed by reading `os.environ`
inside `handler()` on every invocation instead — exactly the kind of bug
a real test suite is supposed to catch before it ships.

## Key Technologies
AWS Glue (PySpark ETL, Data Catalog), Amazon S3, Amazon Athena, AWS Lambda, AWS Step Functions, Terraform, boto3, moto (AWS mocking for tests), DuckDB

## Disclaimer
Educational/portfolio project using synthetic billing data. Terraform/
Glue/Lambda/Step Functions are real, reviewable configuration but were
not applied against a live AWS account in this environment — see the
table above for exactly what did and didn't execute.
