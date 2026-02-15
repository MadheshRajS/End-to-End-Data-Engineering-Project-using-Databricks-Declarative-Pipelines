# Insurance Data Engineering Project

## Overview

This project demonstrates a **high-level insurance data engineering pipeline** using **Databricks**, **Unity Catalog**, and **Declarative Delta Live Tables (DLT)**. The pipeline ingests raw policy and policy event data, transforms it through multiple layers (Bronze, Silver, Gold), applies **SCD Type 2** on policy master, flattens nested events, and produces aggregated insights.  

The main goals of this project are:

- Learn **Databricks Delta Live Tables** for declarative pipeline development.
- Use **Unity Catalog** to manage schema and table permissions.
- Implement **Bronze, Silver, Gold** architecture for structured data flow.
- Apply **SCD Type 2** to maintain historical policy master records.
- Flatten nested JSON policy events.
- Aggregate policy data at the Gold layer for analytics readiness.

---

## Architecture

![Insurance Data Pipeline Architecture](A_flowchart_diagram_titled_"Insurance_Data_Pipelin.png)

**Description:**

1. **Raw Data Sources**  
   - `policy_master` → CSV file  
   - `policy_events` → JSON file  

2. **Autoloader**  
   - Ingests raw files automatically into the **Bronze layer**.
   - Ensures incremental ingestion of new files.

3. **Bronze Layer**  
   - Stores raw data as Delta tables.
   - Adds metadata columns: `_ingestion_timestamp`, `_load_batch_id`, `_record_hash`.
   - Handles duplicates and prepares raw data for transformations.

4. **Silver Layer**  
   - **Policy Master Table (SCD Type 2)**: Maintains historical changes using `apply_changes()`.  
   - **Policy Events Table**: Flattens nested JSON arrays (transactions, coverages, party roles).  
   - Data quality checks applied using `expect_or_drop` for critical columns.

5. **Gold Layer**  
   - Aggregates policy data for analysis:  
     - Total events per policy.  
     - Total transactions and total premium.  
   - Joins policy master SCD2 table with flattened policy events.

---


---

## Step-by-Step Implementation

### 1. Raw Data Generation
- `policy_master` (CSV) and `policy_events` (JSON) are simulated using **PySpark**.  
- Introduced **2% duplicates** in the policy master table.  
- Policy events are nested JSON arrays with transactions, coverages, and party roles.  

---

### 2. Bronze Layer
- Read raw files using **Autoloader** (`cloudFiles`).  
- Added metadata columns:  
  - `_ingestion_timestamp`  
  - `_load_batch_id`  
  - `_record_hash`  
- Stored as Delta tables in **Unity Catalog** under `ins_dev.bronze`.  

```python
dlt.table(
    name="policy_master",
    table_properties={"quality": "bronze"}
)

---

## 3. Silver Layer: Policy Master & Flattening

The Silver layer transforms raw Bronze data into a cleaned, queryable state.

### Policy Master (SCD Type 2)
* **Ingestion:** Uses `dlt.read_stream()` to consume incremental data from the Bronze layer.
* **Logic:** Applied `dlt.apply_changes()` with `stored_as_scd_type=2`.
* **Purpose:** This ensures historical records are preserved. When a policy is updated, the pipeline creates a new version instead of overwriting, allowing for point-in-time analysis.

### Policy Events (Flattened)
* **Logic:** Flattened nested JSON arrays including `transactions`, `coverages`, and `party_roles`.
* **Data Quality:** Applied `expect_or_drop` constraints to enforce schema integrity and filter out invalid records before they reach the Silver tier.

---

## 🥇 Gold Layer: Analytics & Aggregation

The Gold layer provides the final consumption-ready table located at `ins_dev.gold.policy_master_gold`.

* **Aggregations:** Calculates key metrics per policy:
  * `total_events`
  * `total_transactions`
  * `total_premium`
* **Join Logic:** Aggregated event data is joined with the **latest SCD2 policy master records** to provide a unified view of the current policy state alongside its historical performance.

---

## ✅ How to Verify SCD2 Works

To verify that the Slowly Changing Dimension logic is correctly tracking history, run the following query in a Databricks SQL Warehouse:

```sql
-- Query the Silver policy master table
SELECT * FROM ins_dev.silver.policy_master_scd2 
WHERE policy_number = 'POL00000001';
