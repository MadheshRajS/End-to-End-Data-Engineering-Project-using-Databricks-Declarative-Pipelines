-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Post data ingestion into Raw

-- COMMAND ----------

SELECT COUNT(*) FROM ins_dev.raw.policy_master_bronze;

-- COMMAND ----------

SELECT COUNT(*) FROM ins_dev.raw.policy_events_bronze;

-- COMMAND ----------

DESCRIBE TABLE ins_dev.raw.policy_master_bronze;


-- COMMAND ----------

SELECT * FROM ins_dev.raw.policy_master_bronze LIMIT 5;

-- COMMAND ----------

SELECT * FROM ins_dev.raw.policy_events_bronze LIMIT 5;

-- COMMAND ----------

UNDROP TABLE ins_dev.raw.policy_master_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Post silver tables load

-- COMMAND ----------

SELECT COUNT(*) FROM ins_dev.silver.policy_master_silver_scd2


-- COMMAND ----------

SELECT COUNT(DISTINCT policy_number) FROM ins_dev.raw.policy_master_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Bronze – Policy Master

-- COMMAND ----------

-- Check historical records
SELECT *
FROM ins_dev.silver.policy_master_silver_scd2
WHERE policy_number = 'POL00000001'
ORDER BY __START_AT;


-- COMMAND ----------

SHOW GRANTS ON SCHEMA ins_dev.gold;


-- COMMAND ----------

SELECT *
FROM ins_dev.bronze.policy_master
WHERE policy_number = 'POL00000001';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Bronze – Policy Events

-- COMMAND ----------

SELECT *
FROM ins_dev.bronze.policy_events
WHERE policy_number = 'POL00000001';


-- COMMAND ----------

SELECT *
FROM ins_dev.silver.policy_master_silver_scd2
WHERE policy_number = 'POL00000001';


-- COMMAND ----------

SELECT *
FROM ins_dev.silver.policy_master_gold
WHERE policy_number = 'POL00000001';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compare counts between Silver and Gold: For a policy

-- COMMAND ----------

SELECT s.policy_number,
       COUNT(DISTINCT e.txn_id) AS silver_transactions,
       g.total_transactions AS gold_transactions
FROM ins_dev.silver.policy_master_silver_scd2 s
LEFT JOIN ins_dev.silver.policy_events_silver e
  ON s.policy_number = e.policy_number
LEFT JOIN ins_dev.silver.policy_master_gold g
  ON s.policy_number = g.policy_number
WHERE s.policy_number = 'POL00000001'
GROUP BY s.policy_number, g.total_transactions;
