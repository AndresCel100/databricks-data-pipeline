# Medallion Fraud Detection & Loss Prevention Pipeline

An end-to-end, production-grade **Lakeflow Declarative Pipeline** (`beestats_medallion_pipeline`) built on Databricks Unity Catalog (`workspace` catalog) and Serverless compute to incrementally ingest, clean, quantify, and visualize point-of-sale (POS) transactional fraud and anomalies across retail and restaurant store locations.

## Architecture Overview

The pipeline implements a three-tier Medallion architecture using declarative Python (`pyspark.pipelines`), transforming raw, nested POS transaction logs landed in Unity Catalog Volumes into actionable loss-prevention intelligence.

```text
┌─────────────────────────────────────────────────────────────────────────┐
│         LAKEFLOW DECLARATIVE PIPELINE (beestats_medallion_pipeline)     │
│                    Serverless Compute · Unity Catalog                   │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
    ┌────────────────────────────────┼────────────────────────────────┐
    │                                │                                │
    ▼                                ▼                                ▼
┌───────────────┐            ┌───────────────┐            ┌───────────────┐
│    BRONZE     │ ─────────► │    SILVER     │ ─────────► │     GOLD      │
│ Auto Loader & │            │ Streaming Tbl │            │ Materialized  │
│ Streaming Tbl │            │ & @dp.expect  │            │     Views     │
└───────────────┘            └───────────────┘            └───────────────┘
        ▲                            │                            │
        │                            ▼                            ▼
┌───────────────┐            ┌───────────────┐            ┌───────────────┐
│  UC Volumes   │            │  DQ Metrics & │            │     AI/BI     │
│ (/rawlanding) │            │ Dropped Rows  │            │   Dashboard   │
└───────────────┘            └───────────────┘            └───────────────┘
```

### Incremental Processing & Daily Execution Flow

```text
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│   5:00 AM Job    │──►│   Auto Loader    │──►│  Silver Quality  │──►│ Gold Materialized│
│ (America/Chicago TZ) │   │  (New JSON Only) │   │ (@dp.expect_or_  │   │  Views & BI Sync │
│                  │   │   Checkpointed   │   │      drop)       │   │ (workspace.gold) │
└──────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────────┘
```

## Repository Structure

```text
databricks-data-pipeline/
├── .gitignore
├── README.md
└── src/
    ├── dashboards/
    │   └── Applebees Fraud Analytics Dashboard.lvdash.json
    ├── docs/
    │   ├── Applebees_Pipeline_bronze_documentation.ipynb
    │   ├── Applebees_Pipeline_silver_documentation.ipynb
    │   └── Applebees_Pipeline_gold_documentation.ipynb
    └── transformations/
        ├── bronze/
        │   └── bronze_ingestion.py
        ├── silver/
        │   └── silver_transformations.py
        └── gold/
            └── gold_aggregations.py
```

## Data Layers & Logic

### 1. Bronze Layer (`src/transformations/bronze/bronze_ingestion.py` → `workspace.bronze`)
- **Incremental Auto Loader Ingestion (`cloudFiles`):** Streams raw multi-format POS JSON logs and reference datasets incrementally from Unity Catalog Volumes (`/Volumes/workspace/bronze/rawlanding/`) into **Streaming Tables (`@dp.table`)**, preserving raw data lineage, source file metadata, and checkpoint state so only newly landed files are processed on each run.
- **Memory-Safe Parsing & Schema Normalization:** Uses text-based streaming ingestion (`format="text"`) paired with `from_json()` schema parsing and `cloudFiles.maxFilesPerTrigger` micro-batching to handle large `checks` payloads reliably on Serverless compute, flattening nested JSON arrays into structured relational tables for downstream transformations.

### 2. Silver Layer (`src/transformations/silver/silver_transformations.py` → `workspace.silver`)
- **Incremental Streaming Transformations:** Reads upstream tables from `workspace.bronze` as **Streaming Tables (`@dp.table`)**, processing only new incremental records.
- **Missing Value Handling:**
  - Evaluates missingness across dataset attributes.
  - Systematically drops columns with 100% missing values.
  - Populates remaining null values in key numerical fields with a default baseline value of `0`.
- **Deduplication:** Enforces composite key deduplication rules on `(Id, locationId, BusinessDate)` to guarantee unique transactional records.
- **Type Casting & Normalization:** Standardizes string timestamps and date attributes into strict Spark `TimestampType` and `DateType` schemas.
- **Outlier & Fraud Hypothesis Engine:**
  - Evaluates records against **6 distinct loss prevention hypotheses** (e.g., post-payment edits, excessive discounts, void anomalies, unusual operating hours).
  - Consolidates all flagged anomaly instances into a central `suspect_fraud_checks` table for detailed aggregation in the Gold layer.
- **Declarative Data Quality Constraints (`@dp.expect` & `@dp.expect_or_drop`):**
  - Enforces mandatory non-null constraints on critical primary identifiers (`locationId`, `BusinessDate`, `Id`).
  - Structurally flawed records failing primary key expectations are automatically dropped/quarantined by Lakeflow while pass/drop metrics are logged in the Data Quality observability panel.

### 3. Gold Layer (`src/transformations/gold/gold_aggregations.py` → `workspace.gold`)
Consolidates transaction-level suspect indicators from `workspace.silver` using **Materialized Views (`@dp.materialized_view`)** so Lakeflow automatically manages multi-table joins, aggregations, and incremental refreshes across four business-ready deliverables:

| Table / View Name | Purpose & Business Value |
| :--- | :--- |
| **`employee_fraud_scorecard`** | Aggregates total fraud flags and risk scores by employee/cashier ID to highlight high-risk operators for management review. |
| **`location_loss_prevention_summary`** | Summarizes store-level anomaly counts and financial exposure metrics across store locations, enabling regional managers to spot geographic outliers. |
| **`hourly_anomaly_trends`** | Groups flag frequencies by hour of day to identify operational windows (e.g., late-night shifts or closing hours) particularly susceptible to fraudulent activity. |
| **`suspect_check_details`** | A detailed transactional view providing complete check context (check ID, line items, monetary amounts, timestamps) for granular forensic investigation. |

## Loss Prevention AI/BI Dashboard

An interactive Databricks AI/BI Dashboard (`src/dashboards/Applebees Fraud Analytics Dashboard.lvdash.json`) connects directly to the `workspace.gold` Materialized Views via a Serverless SQL Warehouse to present key metrics to store managers and loss prevention teams:
- **Key Metrics:** Total suspect checks, total employees flagged, and calculated dollar exposure at risk.
- **Trend Analysis:** Daily and hourly anomaly distributions.
- **Geographic Ranking:** Risk exposure summarized by store location.
- **Investigation Grid:** Filterable audit detail table linked to specific suspect transaction IDs for in-depth forensic investigation.

## Orchestration & Automated Operations

The end-to-end pipeline is managed declaratively via **`beestats_medallion_pipeline`** and triggered by a scheduled **Lakeflow Job**:
- **Execution Schedule:** Automated daily run at **5:00 AM (`America/Mexico_City`)** using a Lakeflow Job trigger.
- **Declarative Dependency DAG:** Automatically resolved lineage across all 28 tables (`workspace.bronze` → `workspace.silver` → `workspace.gold`) with incremental checkpointing.
- **Failure & Quality Alerts:** Email notifications configured in the pipeline/job settings to immediately alert engineering teams if any run fails, alongside real-time `@dp.expect` data quality metrics in the Lakeflow UI.
- **Compute & Governance:** Runs on **Serverless Compute** with **Unity Catalog** (`workspace` catalog) enabled.

## Setup & Deployment

### Prerequisites
- Databricks Workspace with **Unity Catalog** (`workspace` catalog) and **Serverless Compute** enabled.
- Unity Catalog Volume `/Volumes/workspace/bronze/rawlanding/` populated with raw POS subfolders (`checks`, `employees`, `items_table`, `locations_with_coordinates`, `revenue_centers`).
- Databricks Git Folder integration linked to your GitHub account.

### Installation Steps
1. **Clone Repository:** In your Databricks workspace, import this repository via **Workspace** → **Create** → **Git folder**.
2. **Create / Configure Lakeflow Declarative Pipeline:**
   - Go to **Jobs & Pipelines** → **Create** → **ETL Pipeline** (or open `beestats_medallion_pipeline`).
   - Set **Source code paths** to `src/transformations` (covering `bronze_ingestion.py`, `silver_transformations.py`, and `gold_aggregations.py`).
   - Set **Catalog** to `workspace` and default **Schema** to `bronze`, with **Serverless** compute enabled.
   - Click **Validate** and **Run pipeline** to materialize all Bronze, Silver, and Gold datasets.
3. **Publish AI/BI Dashboard:**
   - Open `src/dashboards/Applebees Fraud Analytics Dashboard`, verify all datasets query `workspace.gold`, attach a Serverless SQL Warehouse, and click **Publish**.
4. **Configure Daily Schedule & Alerts:**
   - Open the orchestrating **Lakeflow Job** under **Jobs & Pipelines**, verify the **5:00 AM (`America/Mexico_City`)** daily schedule trigger, and add your alerting email address under **Notifications**.