# main.tf
# --------
# Provisions the full AWS ETL stack: S3 (raw + curated zones + Athena
# results + Glue scripts), Glue (Data Catalog database + ETL job + IAM
# role), Athena (workgroup), a Lambda completion-notifier, and a Step
# Functions state machine chaining the Glue job to the Lambda.
#
# Usage (on a machine with AWS credentials configured and network access
# to registry.terraform.io — neither available in this sandbox, see
# README.md "What actually ran here"):
#   terraform init
#   terraform plan -var="bucket_name=my-unique-billing-etl-bucket"
#   terraform apply -var="bucket_name=my-unique-billing-etl-bucket"

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for raw/curated/scripts/results zones"
  type        = string
}

variable "glue_database_name" {
  type    = string
  default = "billing_analytics"
}

# ---------------------------------------------------------------------------
# S3 — one bucket, four logical zones via key prefixes
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/glue_etl_job.py"
  source = "${path.module}/../glue_job/glue_etl_job.py"
  etag   = filemd5("${path.module}/../glue_job/glue_etl_job.py")
}

# ---------------------------------------------------------------------------
# IAM — least-privilege role for the Glue job
# ---------------------------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "billing-claims-etl-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
    }]
  })
}

# ---------------------------------------------------------------------------
# Glue Data Catalog + ETL job
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "billing_analytics" {
  name = var.glue_database_name
}

resource "aws_glue_job" "billing_claims_etl" {
  name         = "billing-claims-etl"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_S3_PATH"      = "s3://${aws_s3_bucket.data_lake.id}/raw/claims/"
    "--CURATED_S3_PATH"  = "s3://${aws_s3_bucket.data_lake.id}/curated/claims/"
    "--GLUE_DATABASE"    = var.glue_database_name
    "--job-language"     = "python"
    "--enable-metrics"   = "true"
  }
}

# ---------------------------------------------------------------------------
# Athena — workgroup pointed at a dedicated results prefix
# ---------------------------------------------------------------------------
resource "aws_athena_workgroup" "billing_analytics" {
  name = "billing-analytics-workgroup"
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"
    }
  }
}

# ---------------------------------------------------------------------------
# Lambda — completion notifier
# ---------------------------------------------------------------------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/notify_completion.py"
  output_path = "${path.module}/notify_completion.zip"
}

resource "aws_iam_role" "lambda_role" {
  name = "notify-pipeline-completion-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3_sns" {
  name = "lambda-s3-sns-access"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.data_lake.arn}/pipeline-status/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.pipeline_alerts.arn
      },
    ]
  })
}

resource "aws_sns_topic" "pipeline_alerts" {
  name = "billing-etl-pipeline-alerts"
}

resource "aws_lambda_function" "notify_completion" {
  function_name    = "notify-pipeline-completion"
  role             = aws_iam_role.lambda_role.arn
  handler          = "notify_completion.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      SNS_TOPIC_ARN             = aws_sns_topic.pipeline_alerts.arn
      COMPLETION_MARKER_BUCKET  = aws_s3_bucket.data_lake.id
      COMPLETION_MARKER_PREFIX  = "pipeline-status/"
    }
  }
}

# ---------------------------------------------------------------------------
# Step Functions — orchestrates Glue job -> Lambda notification
# ---------------------------------------------------------------------------
resource "aws_iam_role" "step_functions_role" {
  name = "billing-etl-step-functions-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "step_functions_permissions" {
  name = "step-functions-glue-lambda-access"
  role = aws_iam_role.step_functions_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = aws_lambda_function.notify_completion.arn
      },
    ]
  })
}

resource "aws_sfn_state_machine" "billing_etl_pipeline" {
  name     = "billing-claims-etl-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn
  definition = templatefile("${path.module}/../step_functions/state_machine.json", {
    bucket_name = aws_s3_bucket.data_lake.id
  })
}

# ---------------------------------------------------------------------------
output "bucket_name" {
  value = aws_s3_bucket.data_lake.id
}
output "glue_job_name" {
  value = aws_glue_job.billing_claims_etl.name
}
output "athena_workgroup" {
  value = aws_athena_workgroup.billing_analytics.name
}
output "step_function_arn" {
  value = aws_sfn_state_machine.billing_etl_pipeline.arn
}
