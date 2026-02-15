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

# Insurance Data Engineering Project

This project demonstrates an end-to-end insurance data engineering pipeline using **Databricks**, **Delta Live Tables (DLT)**, **Unity Catalog**, and **DBT** for modeling. The pipeline is designed to handle incremental ingestion, historical data management, and aggregated analytics.

---

## Project Layers

### 1. Bronze Layer
- Raw CSV/JSON files ingested using **Autoloader**.
- Stored as Delta tables for incremental processing.
- Serves as the source for Silver layer transformations.

### 2. Silver Layer

#### **Policy Master (SCD Type 2)**
- Ingested from Bronze using `dlt.read_stream()`.
- Applied `dlt.apply_changes()` with `stored_as_scd_type=2`.
- Maintains historical records; new/updated records are inserted with versioning.
- Key columns:
  - `__CURRENT` – indicates the latest record version
  - `__START_DATE` – record effective start date
  - `__END_DATE` – record effective end date

#### **Policy Events (Flattened)**
- Flattened nested JSON arrays such as `transactions`, `coverages`, `party_roles`.
- Applied `dlt.expect_or_drop` to enforce data quality rules.

### 3. Gold Layer

#### **Aggregated Policy Events**
- Compute metrics:
  - `total_events`
  - `total_transactions`
  - `total_premium`
- Joined aggregated metrics with the latest SCD Type 2 policy master records.
- Stored final aggregated table as:  
  `ins_dev.gold.policy_master_gold`

---

## Verifying SCD Type 2 Functionality

1. Query the Silver layer policy master table:

```sql
SELECT * 
FROM ins_dev.silver.policy_master_scd2 
WHERE policy_number = 'POL00000001';
