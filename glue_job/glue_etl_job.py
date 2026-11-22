"""
glue_etl_job.py
------------------
AWS Glue ETL job (PySpark) — reads raw claims/billing CSVs from the S3
"raw" zone, cleans and transforms them, writes partitioned Parquet to the
S3 "curated" zone, and updates the Glue Data Catalog so the result is
immediately queryable from Athena.

This is a genuine Glue job script (uses the `awsglue` library exactly as
AWS Glue's job runtime provides it) meant to be uploaded as a Glue job's
script location and run on Glue's managed Spark infrastructure — see
terraform/glue.tf for how the job itself is provisioned, and
local_demo/ for a locally-executable equivalent (this sandbox has no AWS
account/credentials, so this script's Glue-specific parts can't run here
— see local_demo/README section in the top-level README for what did).

Deploy: upload this file to the S3 script location Terraform's
`aws_glue_job` resource points at (see terraform/glue.tf), or paste it
into the Glue Studio script editor.
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping, DropNullFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3_PATH", "CURATED_S3_PATH", "GLUE_DATABASE"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_S3_PATH = args["RAW_S3_PATH"]          # e.g. s3://my-bucket/raw/claims/
CURATED_S3_PATH = args["CURATED_S3_PATH"]  # e.g. s3://my-bucket/curated/claims/
GLUE_DATABASE = args["GLUE_DATABASE"]      # e.g. billing_analytics
CURATED_TABLE = "claims_curated"

# ---------------------------------------------------------------------------
# 1. Read raw CSVs from S3 via a Glue dynamic frame (schema-flexible read,
#    the standard Glue pattern — falls back gracefully on ragged/evolving CSVs)
# ---------------------------------------------------------------------------
raw_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [RAW_S3_PATH], "recurse": True},
    transformation_ctx="raw_dyf",
)

# ---------------------------------------------------------------------------
# 2. Clean: drop fully-null fields, enforce a stable output schema
# ---------------------------------------------------------------------------
mapped_dyf = ApplyMapping.apply(
    frame=raw_dyf,
    mappings=[
        ("claim_id", "string", "claim_id", "string"),
        ("payer_code", "string", "payer_code", "string"),
        ("service_date", "string", "service_date", "string"),
        ("billed_amount", "string", "billed_amount", "double"),
        ("paid_amount", "string", "paid_amount", "double"),
        ("claim_status", "string", "claim_status", "string"),
    ],
    transformation_ctx="mapped_dyf",
)
cleaned_dyf = DropNullFields.apply(frame=mapped_dyf, transformation_ctx="cleaned_dyf")

# ---------------------------------------------------------------------------
# 3. Transform with the Spark DataFrame API for anything ApplyMapping can't
#    express cleanly — deduplication, derived columns, partitioning key
# ---------------------------------------------------------------------------
df = cleaned_dyf.toDF()

df = df.dropDuplicates(["claim_id"])
df = df.withColumn("service_year_month", F.date_format(F.to_date("service_date"), "yyyy-MM"))
df = df.withColumn(
    "outstanding_amount",
    F.col("billed_amount") - F.coalesce(F.col("paid_amount"), F.lit(0.0)),
)
df = df.filter(F.col("claim_id").isNotNull() & F.col("billed_amount").isNotNull())

# ---------------------------------------------------------------------------
# 4. Write partitioned Parquet to the curated S3 zone
# ---------------------------------------------------------------------------
(
    df.write
    .mode("overwrite")
    .partitionBy("service_year_month")
    .parquet(CURATED_S3_PATH)
)

# ---------------------------------------------------------------------------
# 5. Register/refresh the table in the Glue Data Catalog so it's
#    immediately queryable from Athena with no manual crawler run needed
# ---------------------------------------------------------------------------
curated_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [CURATED_S3_PATH]},
    transformation_ctx="curated_dyf",
)
sink = glueContext.getSink(
    connection_type="s3",
    path=CURATED_S3_PATH,
    enableUpdateCatalog=True,
    partitionKeys=["service_year_month"],
    transformation_ctx="catalog_sink",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName=CURATED_TABLE)
sink.writeFrame(curated_dyf)

print(f"Wrote {df.count()} curated rows to {CURATED_S3_PATH}, "
      f"cataloged as {GLUE_DATABASE}.{CURATED_TABLE}")

job.commit()
