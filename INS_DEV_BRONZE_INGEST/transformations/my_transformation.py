import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ============================================================
# POLICY MASTER - BRONZE
# ============================================================

policy_master_schema = StructType([
    StructField("policy_number", StringType()),
    StructField("product_line", StringType()),
    StructField("insured_id", StringType()),
    StructField("premium_amount", DoubleType()),
    StructField("issue_date", DateType())
])

@dlt.table(
    name="policy_master",
    comment="Raw Policy Master Data - Bronze Layer",
    table_properties={"quality": "bronze"}
)
def policy_master():

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(policy_master_schema)
        .load("/Volumes/ins_dev/bronze/policy_master")
    )

    business_cols = df.columns

    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_batch_id", expr("uuid()"))
        .withColumn(
            "_record_hash",
            sha2(to_json(struct(*[col(c) for c in business_cols])), 256)
        )
    )


# ============================================================
# POLICY EVENTS - BRONZE
# ============================================================

event_schema = StructType([
    StructField("policy_number", StringType()),
    StructField("event_type", StringType()),
    StructField("event_effective_dt", DateType()),
    StructField("transactions", ArrayType(
        StructType([
            StructField("txn_id", StringType()),
            StructField("txn_type", StringType()),
            StructField("amount", DoubleType())
        ])
    )),
    StructField("coverages", ArrayType(
        StructType([
            StructField("coverage_code", StringType()),
            StructField("limit", DoubleType()),
            StructField("deductible", DoubleType())
        ])
    )),
    StructField("party_roles", ArrayType(
        StructType([
            StructField("party_id", StringType()),
            StructField("role", StringType())
        ])
    ))
])

@dlt.table(
    name="policy_events",
    comment="Raw Policy Events Data - Bronze Layer",
    table_properties={"quality": "bronze"}
)
def policy_events():

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(event_schema)
        .load("/Volumes/ins_dev/bronze/policy_events")
    )

    business_cols = df.columns

    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_batch_id", expr("uuid()"))
        .withColumn(
            "_record_hash",
            sha2(to_json(struct(*[col(c) for c in business_cols])), 256)
        )
    )
