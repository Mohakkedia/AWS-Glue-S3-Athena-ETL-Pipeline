"""
notify_completion.py
-----------------------
Lambda function invoked by the Step Functions state machine (see
step_functions/state_machine.json) after the Glue job finishes — reports
the job's outcome to an SNS topic (email/Slack-integrated in a real
deployment) and writes a completion marker to S3 that downstream
consumers (e.g. a dashboard) can poll for.

Deploy: zip this file and upload as the Lambda function's code (see
terraform/lambda.tf), or paste into the Lambda console's inline editor
(it's a single small file, well under the inline-edit size limit).

Event shape (from the Step Functions Glue GetJobRun task, passed as this
Lambda's event):
{
  "JobRun": {
    "JobName": "billing-claims-etl",
    "JobRunState": "SUCCEEDED" | "FAILED" | "TIMEOUT",
    "Id": "jr_...",
    "StartedOn": "...",
    "CompletedOn": "..."
  }
}
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import boto3


def handler(event, context):
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    completion_marker_bucket = os.environ.get("COMPLETION_MARKER_BUCKET")
    completion_marker_prefix = os.environ.get("COMPLETION_MARKER_PREFIX", "pipeline-status/")

    job_run = event.get("JobRun", {})
    job_name = job_run.get("JobName", "unknown")
    state = job_run.get("JobRunState", "UNKNOWN")
    run_id = job_run.get("Id", "unknown")

    message = {
        "job_name": job_name,
        "run_id": run_id,
        "state": state,
        "started_on": job_run.get("StartedOn"),
        "completed_on": job_run.get("CompletedOn"),
        "notified_at": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Publish to SNS (email/Slack subscribers get notified) — if configured
    if sns_topic_arn:
        sns = boto3.client("sns")
        subject = f"[{'OK' if state == 'SUCCEEDED' else 'ALERT'}] Glue job {job_name}: {state}"
        sns.publish(TopicArn=sns_topic_arn, Subject=subject[:100], Message=json.dumps(message, indent=2))

    # 2. Write a completion marker to S3 — lightweight, pollable "is it done" signal
    if completion_marker_bucket:
        s3 = boto3.client("s3")
        key = f"{completion_marker_prefix}{job_name}/{run_id}.json"
        s3.put_object(
            Bucket=completion_marker_bucket, Key=key,
            Body=json.dumps(message, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        message["marker_location"] = f"s3://{completion_marker_bucket}/{key}"

    print(json.dumps(message))

    return {
        "statusCode": 200 if state == "SUCCEEDED" else 500,
        "body": message,
    }
