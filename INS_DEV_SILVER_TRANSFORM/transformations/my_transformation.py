import dlt
from pyspark.sql.functions import *

# ============================================================
# POLICY MASTER - SILVER (SCD TYPE 2)
# ============================================================

@dlt.view
def policy_master_bronze_view():
    return (
        spark.readStream.table("ins_dev.bronze.policy_master")
        .dropDuplicates(["policy_number", "_record_hash"])
    )


dlt.create_streaming_table(
    name="policy_master_silver_scd2",
    comment="Policy master with historical tracking (SCD Type 2)",
    table_properties={"quality": "silver"}
)


dlt.apply_changes(
    target="policy_master_silver_scd2",
    source="policy_master_bronze_view",
    keys=["policy_number"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type=2
)


# ============================================================
# POLICY EVENTS - SILVER (FLATTENED)
# ============================================================

@dlt.table(
    name="policy_events_silver",
    comment="Flattened policy events - Silver Layer",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_policy_number", "policy_number IS NOT NULL")
def policy_events_silver():

    bronze_df = spark.readStream.table("ins_dev.bronze.policy_events")

    return (
        bronze_df
        .withColumn("txn", explode_outer("transactions"))
        .withColumn("coverage", explode_outer("coverages"))
        .withColumn("party", explode_outer("party_roles"))
        .select(
            "policy_number",
            "event_type",
            "event_effective_dt",
            col("txn.txn_id").alias("txn_id"),
            col("txn.txn_type").alias("txn_type"),
            col("txn.amount").alias("txn_amount"),
            col("coverage.coverage_code").alias("coverage_code"),
            col("coverage.limit").alias("coverage_limit"),
            col("coverage.deductible").alias("coverage_deductible"),
            col("party.party_id").alias("party_id"),
            col("party.role").alias("party_role"),
            "_ingestion_timestamp",
            "_record_hash"
        )
    )
