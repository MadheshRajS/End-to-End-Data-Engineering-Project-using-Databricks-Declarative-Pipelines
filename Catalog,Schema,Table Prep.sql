-- Databricks notebook source
SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create DEV catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ins_dev
COMMENT 'Insurance Data Platform - DEV Environment';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema creation

-- COMMAND ----------

USE CATALOG ins_dev;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS security;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Volume creation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS ins_dev.bronze.policy_landing;
CREATE VOLUME IF NOT EXISTS ins_dev.bronze.checkpoints;
CREATE VOLUME IF NOT EXISTS ins_dev.bronze.quarantine;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS ins_dev.raw.policy_landing;
CREATE VOLUME IF NOT EXISTS ins_dev.raw.checkpoints;
CREATE VOLUME IF NOT EXISTS ins_dev.raw.quarantine;

-- COMMAND ----------

SHOW VOLUMES IN ins_dev.raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Source data creation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC policy_count = 500000
-- MAGIC
-- MAGIC df_master = (
-- MAGIC     spark.range(1, policy_count + 1)
-- MAGIC     .withColumn("policy_number", concat(lit("POL"), lpad(col("id"), 8, "0")))
-- MAGIC     .withColumn("product_line", expr("""
-- MAGIC         CASE 
-- MAGIC             WHEN id % 3 = 0 THEN 'AUTO'
-- MAGIC             WHEN id % 3 = 1 THEN 'HOME'
-- MAGIC             ELSE 'LIFE'
-- MAGIC         END
-- MAGIC     """))
-- MAGIC     .withColumn("insured_id", concat(lit("CUST"), lpad(col("id"), 8, "0")))
-- MAGIC     .withColumn("premium_amount", round(rand()*5000 + 500, 2))
-- MAGIC     .withColumn(
-- MAGIC         "issue_date",
-- MAGIC         expr("date_add(to_date('2023-01-01'), cast(id % 365 as int))")
-- MAGIC     )
-- MAGIC     .drop("id")
-- MAGIC )
-- MAGIC
-- MAGIC # Introduce 2% duplicates
-- MAGIC df_duplicates = df_master.sample(0.02)
-- MAGIC
-- MAGIC df_master_final = df_master.union(df_duplicates)
-- MAGIC
-- MAGIC df_master_final.write \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .json("/Volumes/ins_dev/bronze/policy_master/batch1/")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC events_per_policy = 3
-- MAGIC
-- MAGIC df_events = (
-- MAGIC     df_master
-- MAGIC     .withColumn("event_sequence", explode(sequence(lit(1), lit(events_per_policy))))
-- MAGIC     .withColumn("event_type", expr("""
-- MAGIC         CASE 
-- MAGIC             WHEN event_sequence = 1 THEN 'ISSUE'
-- MAGIC             WHEN event_sequence = 2 THEN 'ENDORSE'
-- MAGIC             ELSE 'RENEW'
-- MAGIC         END
-- MAGIC     """))
-- MAGIC     .withColumn("event_effective_dt", expr("date_add(issue_date, event_sequence*30)"))
-- MAGIC )
-- MAGIC
-- MAGIC df_nested = (
-- MAGIC     df_events
-- MAGIC     .withColumn("transactions", array(
-- MAGIC         struct(
-- MAGIC             concat(lit("TXN"), monotonically_increasing_id()).alias("txn_id"),
-- MAGIC             lit("PREMIUM").alias("txn_type"),
-- MAGIC             col("premium_amount").alias("amount")
-- MAGIC         )
-- MAGIC     ))
-- MAGIC     .withColumn("coverages", array(
-- MAGIC         struct(
-- MAGIC             lit("COV1").alias("coverage_code"),
-- MAGIC             lit(100000).alias("limit"),
-- MAGIC             lit(500).alias("deductible")
-- MAGIC         )
-- MAGIC     ))
-- MAGIC     .withColumn("party_roles", array(
-- MAGIC         struct(
-- MAGIC             col("insured_id").alias("party_id"),
-- MAGIC             lit("INSURED").alias("role")
-- MAGIC         )
-- MAGIC     ))
-- MAGIC     .select(
-- MAGIC         "policy_number",
-- MAGIC         "event_type",
-- MAGIC         "event_effective_dt",
-- MAGIC         "transactions",
-- MAGIC         "coverages",
-- MAGIC         "party_roles"
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC df_nested.write \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .json("/Volumes/ins_dev/bronze/policy_events/batch1/")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("header","true").csv("/Volumes/ins_dev/raw/policy_landing/policy_master/batch1/").count()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("header","true").csv("/Volumes/ins_dev/raw/policy_landing/policy_master/batch1/").show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("header","true").csv("/Volumes/ins_dev/raw/policy_landing/policy_events/batch1/").show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("header","true").json("/Volumes/ins_dev/raw/policy_landing/policy_events/batch1/").select('coverages','transactions','party_roles').show(6,truncate=False)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ins_dev.bronze;
CREATE SCHEMA IF NOT EXISTS ins_dev.silver;
CREATE SCHEMA IF NOT EXISTS ins_dev.gold;


-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS ins_dev.bronze.policy_master;
CREATE VOLUME IF NOT EXISTS ins_dev.bronze.policy_events;


-- COMMAND ----------

SELECT * FROM ins_dev.silver.policy_master_gold
