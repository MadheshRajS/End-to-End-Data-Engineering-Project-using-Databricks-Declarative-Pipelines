# Insurance Data Engineering Project

## Overview

This project demonstrates a **high-level insurance data engineering pipeline** using **Databricks**, **Unity Catalog**, and **Declarative Delta Live Tables (DLT)**. The pipeline ingests raw policy and policy event data, transforms it through multiple layers (Bronze, Silver, Gold), applies **SCD Type 2** on policy master, flattens nested events, and produces aggregated insights.  

**Main Goals:**

- Learn **Databricks Delta Live Tables** for declarative pipeline development.
- Use **Unity Catalog** to manage schema and table permissions.
- Implement **Bronze, Silver, Gold** architecture for structured data flow.
- Apply **SCD Type 2** to maintain historical policy master records.
- Flatten nested JSON policy events.
- Aggregate policy data at the Gold layer for analytics readiness.

---

## Architecture

![Insurance Data Pipeline Architecture]([A_flowchart_diagram_titled_"Insurance_Data_Pipelin.png](https://github.com/MadheshRajS/End-to-End-Data-Engineering-Project-using-Databricks-Declarative-Pipelines/blob/main/Architecture%20diagram.png?raw=true))

**Pipeline Components:**

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
     - Total events per policy  
     - Total transactions and total premium  
   - Joins policy master SCD2 table with flattened policy events.

---

## Step-by-Step Implementation

### 1. Raw Data Generation
- `policy_master` (CSV) and `policy_events` (JSON) are simulated using **PySpark**.  
- Introduced **2% duplicates** in the policy master table.  
- Policy events are nested JSON arrays with transactions, coverages, and party roles.  

---

### 2. Bronze Layer
- Read raw files using **Autoloader (`cloudFiles`)**.  
- Added metadata columns:  
  - `_ingestion_timestamp`  
  - `_load_batch_id`  
  - `_record_hash`  
- Stored as Delta tables in **Unity Catalog** under `ins_dev.bronze`.  

```python
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="policy_master",
    table_properties={"quality": "bronze"}
)
def policy_master_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(source_path_policy_master)
        .select("*", "_metadata.file_path", current_timestamp().alias("_ingestion_timestamp"))
    )
```

## 3. Silver Layer

### Policy Master (SCD Type 2)
- Used `dlt.read_stream()` to ingest from Bronze.
- Applied `dlt.apply_changes()` with `stored_as_scd_type=2`.
- Ensures historical records are preserved while new/updated records are inserted.

### Policy Events (Flattened)
- Flattened nested JSON arrays: `transactions`, `coverages`, `party_roles`.
- Applied `expect_or_drop` to enforce data quality.

---

## 4. Gold Layer
- Aggregated policy events:
  - `total_events`
  - `total_transactions`
  - `total_premium`
- Joined aggregated data with latest SCD2 policy master records.
- Stored final aggregated Delta table under `ins_dev.gold.policy_master_gold`.

---

## How to Verify SCD2 Works
1. Query the Silver policy master table:
```sql
SELECT * FROM ins_dev.silver.policy_master_scd2
WHERE policy_number = 'POL00000001';
```
- Insert or update policy master data and re-run the pipeline.  
  Older records will automatically have `__CURRENT = False` while the latest version will have `__CURRENT = True`.

---

## Notes

- All layers are managed using **Unity Catalog**.
- **Declarative Delta Live Tables (DLT)** ensures reproducibility and proper data lineage.
- **Autoloader** correctly handles incremental ingestion of raw CSV/JSON files.

---

## Future Improvements

- Implement additional data quality rules using `dlt.expect` for Silver tables.
- Extend the Gold layer to include customer analytics, claims, or reporting.
- Integrate with **DBT** for modeling transformations in the Silver/Gold layers.

---

## References

- [Databricks Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/index.html)  
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)  
- [Autoloader](https://docs.databricks.com/ingestion/auto-loader/index.html)
