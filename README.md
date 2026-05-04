# Azure Databricks Insurance Claims Data Engineering Pipeline

A production-pattern implementation of a Guidewire ClaimCenter Data (CDA) ingestion and transformation pipeline built on Azure Databricks, Azure Data Factory, and Azure Data Lake Storage Gen2.

This project replicates the architecture of a real-world insurance claims data platform at a scaled-down scope: 5 tables instead of 813, simulated source data instead of live AWS S3, with every production pattern faithfully reproduced.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Pipeline Stages](#pipeline-stages)
- [Key Patterns and Techniques](#key-patterns-and-techniques)
- [Repository Structure](#repository-structure)
- [Data Model](#data-model)
- [Setup and Configuration](#setup-and-configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Production vs Mini Project Comparison](#production-vs-mini-project-comparison)

---

## Architecture Overview

```
ADLS client_data/          (simulated Guidewire CDA S3 delivery)
        |
        v
[Stage 1] Manifest Notebooks (x4)
        Copy manifest.json, parse table metadata,
        detect new files, register in abc.CDA_FILE_LOADS
        |
        v
ADLS raw/                  (parquet files, append-only per timestamp subfolder)
        |
        v
[Stage 2] Source to Raw
        Copy parquet from client_data/ to raw/ table-by-table
        |
        v
[Stage 3] Raw to Replica
ADLS replica/ + Unity Catalog: insurance_claims_domain.replica.*
        (External Delta tables, CDC MERGE + ROW_NUMBER dedup)
        |
        v
[Stage 4] Replica to Refined
ADLS refined/ + Unity Catalog: insurance_claims_domain.refined.*
        (External Delta tables, SCD Type 2 MERGE with hash-based change detection)
        |
        v
[Stage 5] Analytics Layer
Unity Catalog: insurance_claims_domain.analytics.*
        (Aggregated views and snapshot tables for reporting)
```

**Orchestration:** Azure Data Factory pipelines drive every stage using `ExecutePipeline`, `ForEach`, and `DatabricksNotebook` activity chains. Azure SQL (ABC metadata framework) tracks every cycle, step, and job execution. A master pipeline (`pl_m_source_to_refined`) coordinates the full end-to-end run.

See [docs/screenshots.md](docs/screenshots.md) for pipeline canvas diagrams and workspace screenshots.

---

## Technology Stack

| Component | Service |
|-----------|---------|
| Compute | Azure Databricks (Spark + Delta Lake) |
| Storage | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory |
| Metadata store | Azure SQL Database |
| Table governance | Unity Catalog |
| Secrets | Azure Key Vault (production) |
| Source system | Guidewire ClaimCenter CDA (simulated) |
| Language | PySpark, SQL, Python |

---

## Pipeline Stages

### Stage 0: Data Generator
`notebooks/00 Code to simulate s3 data loads (Here we use ADLS).py`

Simulates a Guidewire CDA delivery into ADLS `client_data/`. On the first run it generates 100% INSERT rows. On subsequent runs it generates a realistic CDC mix (60% INSERT, 30% UPDATE, 10% DELETE). Every UPDATE intentionally includes a stale duplicate row (60 seconds older `gwcbi___payload_ts_ms`) to exercise the deduplication logic in the Raw-to-Replica stage. State is persisted in a Unity Catalog table so each run builds on the previous one.

### Stage 1: Manifest Loading (4 notebooks)

Mirrors the production 4-notebook split for independent retry and ADF visibility.

| Notebook | Responsibility |
|----------|---------------|
| `01 Copy Manifest File from Source to ADLS.py` | Copy `manifest.json` from `client_data/` to `raw/claims/` |
| `02 Load Manifest Data to Azure SQL.py` | Parse manifest, load into `abc.MANIFEST`, call `GET_GW_FILE_METADATA_MASTER` to update `abc.TABLE_LOAD_METADATA` |
| `03 Detect and List Files to Load.py` | Determine full vs incremental load; list parquet paths; stage in global temp view `TEMP_FILE_INFO_TABLE` |
| `04 Populate CDA_FILE_LOADS Table.py` | Write one row per new parquet file into `abc.CDA_FILE_LOADS` (`IS_LOADED=0`) |

### Stage 2: Source to Raw
`notebooks/06 Copy Parquet Files from Source to ADLS Raw.py`

Reads `abc.CDA_FILE_LOADS` to find all unloaded files. Copies each parquet file from `client_data/` to the corresponding `raw/` path and marks `IS_LOADED=1` on completion.

### Stage 3: Raw to Replica
`notebooks/07 Copy ADLS Raw Parquet to Replica External Delta Tables.py`

For each table, reads parquet files from `raw/`, deduplicates within the batch using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY gwcbi___payload_ts_ms DESC)`, and MERGEs into an external Delta table in Unity Catalog. Handles CDC operation codes (0=INSERT, 1=soft-delete, 2=after-image UPDATE, 4=UPDATE) and propagates `Soft_Delete='Y'` flags for deletes.

Includes balancing and reconciliation notebooks (`08`, `09`, `10`) that verify row counts match between the parquet source and the replica Delta table.

### Stage 4: Replica to Refined (SCD Type 2)
`notebooks/12 Load Replica to Refined - SCD Type 2.py`

The most complex stage. For each refined table:

1. Executes a configurable transformation SQL query (stored in `abc.JOB_PARM_TBL`) that JOINs replica tables
2. Computes `ETL_Key_Hash` (MD5 over primary key) and `ETL_SCD2_Hash` (MD5 over business columns, excluding delivery metadata)
3. On initial load: writes Delta table and registers as Unity Catalog external table
4. On incremental load: runs a SCD Type 2 MERGE
   - Rows where `ETL_SCD2_Hash` changed: old active row is expired (`ETL_ActiveRow_Flag='N'`, `ETL_RecordExpiry_Date` set)
   - New/changed rows: inserted as new active versions
5. Assigns sequential `SurrogateKey` to all new rows post-MERGE
6. Propagates soft-deletes and retired records from replica to refined

**SCD2 columns on every refined table:**

| Column | Purpose |
|--------|---------|
| `ETL_Key_Hash` | MD5 of primary key - identifies the entity across all versions |
| `ETL_SCD2_Hash` | MD5 of business columns - detects data changes |
| `ETL_ActiveRow_Flag` | `Y` = current version, `N` = expired version |
| `ETL_RecordEffective_Date` | When this version became active |
| `ETL_RecordExpiry_Date` | When this version was superseded (`9999-12-31` if still active) |
| `ETL_Ins_Cyc_SK` | Cycle run when this row was first created |
| `ETL_Lst_Updt_Cyc_Sk` | Cycle run when this row was last expired or updated |

### Validation
`notebooks/11 Replica Tables Data Validation.py`

Runs configurable validation rules from `abc.VALIDATION_CTRL_TBL` and logs results to `abc.VALIDATION_ERROR_LOG`.

---

## Key Patterns and Techniques

**Metadata-driven execution:** All pipeline parameters (table names, transformation queries, SCD2 keys, balancing thresholds) are stored in Azure SQL (`abc` schema) and read at runtime. Adding a new table requires only a database row insert, not a code change.

**ABC metadata framework:** A set of Azure SQL tables and stored procedures tracks every cycle (`CYC_RUN_TBL`), step (`STEP_RUN_TBL`), and job (`JOB_RUN_TBL`) execution with start time, end time, row counts, and status. `PROC_UPDATE_CYC_START/END`, `PROC_UPDATE_STEP_START/END`, `PROC_UPDATE_JOB_START/END` are called by ADF activities (not by notebooks) to maintain a clean audit trail.

**Idempotent notebooks:** Every notebook is safe to rerun. Manifest loading deletes before re-inserting. File copy marks `IS_LOADED` before returning. SCD2 MERGE is naturally idempotent (unchanged rows produce no change in the target).

**External Delta tables with Unity Catalog:** All Delta tables are registered as external (`USING DELTA LOCATION`) so the Unity Catalog metadata pointer and the ADLS data files have independent lifecycles. Dropping the UC table does not delete the underlying files.

**CDC deduplication:** The Guidewire CDA feed can deliver multiple rows for the same entity ID within a single batch (stale duplicates from the update pipeline). The Raw-to-Replica notebook discards stale rows using `ROW_NUMBER OVER (PARTITION BY id ORDER BY gwcbi___payload_ts_ms DESC)`.

**ADF ForEach parallelism:** Source-to-Raw and Raw-to-Replica stages use a ForEach activity to run one Databricks notebook per table in parallel. In production this drives 805 parallel jobs per step.

**Cycle guard with Until loop:** The master pipeline opens with an ADF `Until` activity that polls `abc.CYC_CTRL_TBL` and waits for any previous cycle run to reach a terminal status (`C` or `F`) before starting a new cycle. This prevents overlapping runs.

---

## Repository Structure

```
.
├── README.md
├── .gitignore
│
├── notebooks/                              Databricks notebooks (.py source export)
│   ├── README.md                           Notebook inventory and credential setup guide
│   ├── 00 Code to simulate s3 data loads   Simulates Guidewire CDA S3 data delivery
│   ├── 01 Copy Manifest File ...           Stage 1a: Copy manifest.json to raw/
│   ├── 02 Load Manifest Data ...           Stage 1b: Parse manifest, load abc.MANIFEST
│   ├── 03 Detect and List Files ...        Stage 1c: Full vs incremental file detection
│   ├── 04 Populate CDA_FILE_LOADS ...      Stage 1d: Register files in abc.CDA_FILE_LOADS
│   ├── 05 Check Run Status ...             Pre-run guard: check/set restart flags
│   ├── 06 Copy Parquet Files ...           Stage 2: Copy parquet to ADLS raw/
│   ├── 07 Copy ADLS Raw Parquet ...        Stage 3: CDC MERGE into replica Delta tables
│   ├── 08 Replica Table Balancing Part 1   Stage 3 validation: balancing pass 1
│   ├── 09 Replica Table Balancing Part 2   Stage 3 validation: balancing pass 2
│   ├── 10 Replica Table Balancing ...      Stage 3 validation: final reconciliation
│   ├── 11 Replica Tables Data Validation   Stage 3 validation: rule-based data quality
│   ├── 12 Load Replica to Refined ...      Stage 4: SCD Type 2 MERGE into refined tables
│   └── 13 Refined Table Balancing ...      Stage 4 validation: row count reconciliation
│
├── sql/                                    Azure SQL DDLs and stored procedures
│   ├── README.md                           Schema setup guide and table inventory
│   ├── abc_tables_ddl.sql                  ABC metadata framework: 19 tables + seed data
│   ├── abc_stored_procedures_ddl.sql       Stored procedures for cycle/step/job tracking
│   ├── abc_tables_backup_ddl.sql           End-of-cycle backup shadow tables
│   └── refined_tables_ddl.sql             Refined layer external Delta table registrations
│
├── adf_pipelines/                          ADF pipeline definitions (JSON export)
│   ├── README.md                           Pipeline inventory and activity sequence guide
│   ├── pl_m_source_to_refined.json         Master pipeline: end-to-end orchestration
│   ├── pl_c_source_to_raw.json             Child pipeline: Manifest + Source to Raw
│   ├── pl_c_raw_to_replica.json            Child pipeline: Raw to Replica
│   ├── pl_c_replica_to_refined.json        Child pipeline: Replica to Refined (SCD2)
│   └── pl_gc_relica_data_validation.json   Sub-pipeline: Replica data validation
│
└── docs/
    └── screenshots.md                      Pipeline diagrams and workspace screenshots
```

---

## Data Model

### Source Tables (5 Guidewire ClaimCenter entities)

| Table | Description | Rows per run |
|-------|-------------|-------------|
| `cc_policy` | Insurance policy master | 50 |
| `cc_claim` | Claim header | 100 |
| `cc_exposure` | Claim line/coverage exposure | 150 |
| `cc_contact` | Claimant and contact records | 80 |
| `cc_transaction` | Financial transactions (payments, reserves) | 200 |

### ABC Metadata Tables (Azure SQL, `abc` schema)

| Table | Purpose |
|-------|---------|
| `CYC_CTRL_TBL` | One row per pipeline cycle. Tracks status (C/I/F) and current run SK |
| `CYC_RUN_TBL` | One row per cycle execution. Full run history |
| `STEP_CTRL_TBL` | Step definitions (Manifest, Src-Raw, Raw-Replica, Replica-Refined) |
| `STEP_RUN_TBL` | One row per step execution |
| `JOB_CTRL_TBL` | One row per table-level job definition |
| `JOB_RUN_TBL` | One row per job execution (full audit log) |
| `JOB_PARM_TBL` | Job parameters: transformation SQL, SCD2 keys, partition columns |
| `MANIFEST` | Current manifest entries per XCENTER |
| `TABLE_LOAD_METADATA` | Latest/previous fingerprints and timestamps per table |
| `CDA_FILE_LOADS` | One row per parquet file: IS_LOADED flag, path, fingerprint, timestamp |
| `TABLES_TO_LOAD` | Staging: ForEach input list for Source-to-Raw |
| `TABLES_TO_LOAD_RPL` | Staging: ForEach input list for Raw-to-Replica |
| `TABLES_TO_LOAD_RFN` | Staging: ForEach input list for Replica-to-Refined |
| `VALIDATION_CTRL_TBL` | Configurable validation rule definitions |
| `VALIDATION_ERROR_LOG` | Failed validation rule log per cycle |
| `BALANCE_METRICS` | Row count metrics per table per cycle for reconciliation |

---

## Setup and Configuration

### Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- Azure Data Lake Storage Gen2 account
- Azure SQL Database
- Azure Data Factory

### Step 1: Azure SQL Setup

Run the DDL scripts in order against your Azure SQL database (see `sql/README.md` for full details):

```
1. sql/abc_tables_ddl.sql
2. sql/abc_stored_procedures_ddl.sql
3. sql/abc_tables_backup_ddl.sql
4. sql/refined_tables_ddl.sql
```

### Step 2: Configure Notebook Credentials

Each notebook that connects to Azure SQL contains placeholder values:

```python
AsqldbServer   = "<your-sql-server>.database.windows.net"
AsqldbName     = "<your-database-name>"
AsqldbUserName = "<your-sql-username>"
AsqldbPassword = "<your-sql-password>"
```

Notebook 00 contains a placeholder for the ADLS storage key:

```python
STORAGE_KEY = "<your-adls-storage-key>"
```

In a production environment, all secrets are stored in Azure Key Vault and fetched via:

```python
AsqldbPassword = dbutils.secrets.get("your-kv-scope", "AsqldbPassword")
```

### Step 3: ADF Pipeline Import

Import the JSON files from `adf_pipelines/` into your Azure Data Factory instance (see `adf_pipelines/README.md` for full details). Update linked service and dataset references to match your environment.

---

## Running the Pipeline

### Option 1: Full Pipeline via ADF Master Pipeline

Trigger `pl_m_source_to_refined` from ADF. This runs the complete sequence from data generation through to the refined layer.

### Option 2: Run Individual Stages

Each child pipeline (`pl_c_*`) can be triggered independently for testing or debugging a specific stage.

### Option 3: Run Notebooks Directly

Each notebook accepts Databricks widget parameters. Set the widget values in the notebook UI and run all cells.

---

## Production vs Mini Project Comparison

| Aspect | Production (Slide Insurance) | This Project |
|--------|------------------------------|-------------|
| Source data | AWS S3 (live Guidewire CDA feed) | ADLS `client_data/` (simulated by notebook 00) |
| Tables | 813 (cc_*, cctl_*, ccx_*) | 5 (cc_policy, cc_claim, cc_exposure, cc_contact, cc_transaction) |
| Refined tables | 35 | 2 (claim_detail, claim_financial) |
| Analytics objects | 52 (15 tables + 37 views) | Subset of views |
| ADF pipelines | 99 | 5 (core pipeline stages) |
| Parallel jobs | 805 (Src-to-Raw), 35 (Replica-to-Refined) | Sequential (5 tables) |
| Secrets | Azure Key Vault | Placeholder values (practice only) |
| Schedule | Hourly, 7AM-midnight ET, 7 days/week | Manual trigger |
| Run duration | 30-43 minutes | Under 5 minutes |
| Unity Catalog | `dbw_claims_prod_eastus_001` | `insurance_claims_domain` |

---

## What This Demonstrates

- End-to-end pipeline engineering on Azure Databricks with Unity Catalog and Delta Lake
- Metadata-driven architecture where no code changes are needed to add new tables
- CDC processing with Guidewire operation code handling and ROW_NUMBER deduplication
- SCD Type 2 implementation in PySpark/Delta using MD5 hash-based change detection and MERGE
- ADF orchestration patterns including ForEach parallelism, Until loops, cycle guards, and pipeline chaining
- Data quality framework with configurable validation rules and multi-pass reconciliation checks
- Audit trail design with a full run history metadata schema tracking every cycle, step, and job
- Insurance domain knowledge with Guidewire ClaimCenter data model (policy, claim, exposure, contact, transaction)
