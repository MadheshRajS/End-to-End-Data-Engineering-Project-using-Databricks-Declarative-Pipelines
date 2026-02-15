import dlt
from pyspark.sql.functions import *

# ============================================================
# GOLD LAYER - POLICY SUMMARY
# ============================================================

@dlt.table(
    name="policy_master_gold",
    comment="Latest policy details with aggregates - Gold Layer",
    table_properties={"quality": "gold"}
)
def policy_master_gold():
    
    # Read Silver SCD2
    policy_silver = dlt.read("policy_master_silver_scd2")#.filter(col("__CURRENT") == True)
    
    # Read Events Silver
    events_silver = dlt.read("policy_events_silver")
    
    # Aggregate policy events
    events_agg = (
        events_silver
        .groupBy("policy_number")
        .agg(
            count("*").alias("total_events"),
            countDistinct("txn_id").alias("total_transactions"),
            sum("txn_amount").alias("total_premium")
        )
    )
    
    # Join latest policy info with event aggregates
    return (
        policy_silver.join(events_agg, on="policy_number", how="left")
        .select(
            "policy_number",
            "product_line",
            "insured_id",
            "premium_amount",
            "total_events",
            "total_transactions",
            "total_premium",
            "_ingestion_timestamp",
            "_record_hash"
        )
    )
