# Medallion Fraud Detection & Loss Prevention Pipeline.

An end-to-end, production-grade Medallion Lakehouse architecture built on Databricks to detect, quantify, and visualize point-of-sale (POS) transactional fraud and anomalies across retail and restaurant store locations.

##  Architecture Overview

The pipeline implements a three-tier Medallion architecture orchestrated via Databricks Workflows, transforming raw, nested POS transaction logs into actionable loss-prevention intelligence.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATABRICKS WORKFLOW                           │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
    ┌────────────────────────────────┼────────────────────────────────┐
    │                                │                                │
    ▼                                ▼                                ▼
┌───────────┐                ┌───────────────┐                ┌──────────────┐
│  BRONZE   │ ────────────►  │    SILVER     │ ────────────►  │     GOLD     │
│ Ingestion │                │ Hygiene & DQ  │                │  Analytics & │
└───────────┘                └───────────────┘                │  Dashboards  │
                                     │                        └──────────────┘
                                     ▼                                │
                             ┌───────────────┐                        ▼
                             │  QUARANTINE   │                ┌──────────────┐
                             │   Bad Data    │                │    AI/BI     │
                             └───────────────┘                │  Dashboard   │
                                                              └──────────────┘
```

##  Repository Structure

```text
databricks-data-pipeline/
├── .gitignore
├── README.md
├── resources/
│   └── jobs/
│       └── daily_orchestration_job.yml
└── src/
    └── transformations/
        ├── bronze/
        │   └── Applebees_Pipeline_bronzelayer.ipynb
        ├── silver/
        │   └── Applebees_Pipeline_silverlayer.ipynb
        └── gold/
            ├── Applebees_Pipeline_goldlayer.ipynb
            └── Applebees Fraud Analytics Dashboard.lvdash.json
```

##  Data Layers & Logic

### 1. Bronze Layer (`src/transformations/bronze/Applebees_Pipeline_bronzelayer.ipynb`)

* **Raw Ingestion:** Ingests raw multi-format POS JSON logs into Delta Lake format, preserving raw data lineage and audit metadata.
* **Schema Normalization:** Flattens and normalizes complex nested JSON arrays by exploding array data types into distinct, structured relational tables for downstream transformations.

### 2. Silver Layer (`src/transformations/silver/Applebees_Pipeline_silverlayer.ipynb`)

* **Missing Value Handling:**
  * Evaluates missingness across all dataset attributes.
  * Columns with **100% missing values** are systematically dropped.
  * Remaining null values in key numerical fields are populated with a default baseline value of `0`.
* **Deduplication:** Enforces a composite key deduplication rule on `(Id, locationId, BusinessDate)` to guarantee unique transactional records.
* **Type Casting & Normalization:** Standardizes string timestamps and date attributes into strict Spark `TimestampType` and `DateType` schemas.
* **Outlier & Fraud Hypothesis Engine:**
  * Evaluates records against **6 distinct loss prevention hypotheses** (e.g., post-payment edits, excessive discounts, void anomalies, unusual operating hours).
  * Consolidates all flagged anomaly instances into a central `suspect_fraud_checks` table for detailed study in the Gold layer.
* **Data Quality Constraints & Quarantine:**
  * Enforces mandatory non-null constraints on critical primary identifiers (`locationId`, `BusinessDate`, `Id`).
  * Structurally flawed records failing primary key expectations are automatically isolated and routed to a `quarantine_bad_data` table for auditing.

### 3. Gold Layer (`src/transformations/gold/Applebees_Pipeline_goldlayer.ipynb` & Dashboard)

Consolidates transaction-level suspect indicators from `suspect_fraud_checks` into four specialized, business-ready aggregate deliverables alongside an integrated AI/BI dashboard:

| Table / View Name | Purpose & Business Value | 
| ----- | ----- | 
| `employee_fraud_scorecard` | Aggregates total fraud flags and risk scores by employee/cashier ID to highlight high-risk operators for management review. | 
| `location_loss_prevention_summary` | Summarizes store-level anomaly counts and financial exposure metrics across store locations, enabling regional managers to spot geographic outliers. | 
| `hourly_anomaly_trends` | Groups flag frequencies by hour of day to identify operational windows (e.g., late-night shifts or closing hours) particularly susceptible to fraudulent activity. | 
| `suspect_check_details` | A detailed transactional view providing complete check context (check ID, line items, monetary amounts, timestamps) for granular forensic investigation. | 

---

## Loss Prevention AI/BI Dashboard

An interactive Databricks AI/BI Dashboard (`src/transformations/gold/Applebees Fraud Analytics Dashboard.lvdash.json`) connects directly to the Gold layer tables to present key metrics to store managers and loss prevention teams:

* **Key Metrics:** Total flagged checks, calculated dollar exposure, and top high-risk cashiers.
* **Trend Analysis:** Daily and hourly anomaly distributions.
* **Geographic Ranking:** Risk exposure summarized by store location.
* **Investigation Grid:** Filterable audit detail table linked to specific suspect transaction IDs for in-depth forensic investigation.

---

##  Orchestration & Automated Operations

The entire end-to-end pipeline is orchestrated via Databricks Workflows defined in `resources/jobs/daily_orchestration_job.yml`:

* **Execution Schedule:** Automated daily run using a Quartz Cron schedule expression.
* **Dependency DAG:** Strict task dependencies (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold` $\rightarrow$ `Dashboard Refresh`).
* **Failure Alerts:** Email notification triggers configured to immediately alert on-call engineering teams if any task fails or times out.
* **Environment Parameters:** Managed globally using Databricks Job Parameters (`env`, `process_date`).

---

##  Setup & Local Deployment

### Prerequisites
* Databricks Workspace with Unity Catalog or Hive Metastore enabled.
* Databricks Repos / Git Integration linked to your GitHub account.

### Installation Steps
1. **Clone Repository:** In your Databricks workspace, import this repository via **Workspace** $\rightarrow$ **Repos** $\rightarrow$ **Add Repo**.
2. **Execute Transformations:** Open and run the notebooks in sequential order inside `src/transformations/`:
   1. `bronze/Applebees_Pipeline_bronzelayer.ipynb`
   2. `silver/Applebees_Pipeline_silverlayer.ipynb`
   3. `gold/Applebees_Pipeline_goldlayer.ipynb`
3. **Deploy Workflow:**
   * Go to **Workflows** $\rightarrow$ **Jobs** $\rightarrow$ **Create Job**.
   * Open the context menu (`...`) at top-right $\rightarrow$ select **Edit as YAML** / **View as code**.
   * Replace contents with `resources/jobs/daily_orchestration_job.yml` and save.
4. **Configure Email Notifications:** Update the destination email address under `email_notifications` in the job configuration to point to your team's alerting endpoint.[readme_documentation.md](https://github.com/user-attachments/files/34113671/readme_documentation.md)
