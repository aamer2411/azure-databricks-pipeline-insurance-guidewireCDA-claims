# SQL

Azure SQL DDL scripts and stored procedure definitions for the ABC metadata framework. Run these scripts once against a fresh Azure SQL database to set up the complete metadata infrastructure that drives pipeline execution.

All scripts are idempotent (`IF NOT EXISTS` guarded) and can be safely re-run.

---

## Files

### `abc_tables_ddl.sql`

DDL for the ABC metadata framework schema (`abc`) and all 19 tables, including seed data for control tables.

The ABC framework is the operational backbone of the pipeline. Every cycle, step, and job execution is tracked here. ADF reads from these tables to build its ForEach item lists and writes back status updates via stored procedures.

**Run order matters** (foreign key and dependency chain):

| Order | Table | Purpose |
|-------|-------|---------|
| 1 | Schema `abc` | Container for all metadata objects |
| 2 | `CYC_CTRL_TBL` | One row per pipeline cycle. Tracks current status (C/I/F) and the running `CYC_RUN_SK`. |
| 3 | `CYC_RUN_TBL` | One row per cycle execution. Full audit history of every run. |
| 4 | `STEP_CTRL_TBL` | Step definitions (Manifest, Source-to-Raw, Raw-to-Replica, Replica-to-Refined). |
| 5 | `STEP_RUN_TBL` | One row per step execution. |
| 6 | `JOB_CTRL_TBL` | One row per table-level job definition (e.g. one row per replica table). |
| 7 | `CDA_FILE_LOADS` | One row per parquet file. `IS_LOADED` flag drives the Source-to-Raw copy. |
| 8 | `MANIFEST` | Parsed `manifest.json` entries for the current cycle, tagged by XCENTER. |
| 9 | `TABLE_LOAD_METADATA` | Latest and previous fingerprints and timestamps per table. Used for incremental detection. |
| 10 | `TABLES_TO_LOAD` | Staging table. ADF populates this before the Source-to-Raw ForEach. |
| 11 | `RAW_TABLE_LOAD` | One row per parquet file copied to raw. Written by notebook 06. |
| 12 | `JOB_RUN_TBL` | One row per job execution. Full audit log of every table-level run. |
| 13 | `TABLES_TO_LOAD_RPL` | Staging table. ADF populates this before the Raw-to-Replica ForEach. |
| 14 | `VALIDATION_CTRL_TBL` | Configurable validation rule definitions. |
| 15 | `BALANCE_METRICS` | Row count metrics per table per cycle. Written by balancing notebooks (08, 10, 13). |
| 16 | `VALIDATION_ERROR_LOG` | Failed validation rule details. Written by notebook 11. |
| 17 | `VALIDATION_RUN_TBL` | One row per validation rule execution. |
| 18 | `TABLES_TO_LOAD_RFN` | Staging table. ADF populates this before the Replica-to-Refined ForEach. |
| 19 | `JOB_PARM_TBL` | Job-level parameters: transformation SQL, SCD2 key columns, partition columns, driving table for soft-delete propagation. |

---

### `abc_stored_procedures_ddl.sql`

Stored procedure definitions for all operational actions. Notebooks never write directly to run-tracking tables; they call these procedures so that the write logic is centralised and consistent.

Key procedures:

| Procedure | Called by | Purpose |
|-----------|-----------|---------|
| `PROC_UPDATE_CYC_START` | ADF (before pipeline) | Opens a new cycle run, writes to `CYC_RUN_TBL`, sets `CYC_STS_CD='I'` |
| `PROC_UPDATE_CYC_END` | ADF (after pipeline) | Closes the cycle run, sets status to `C` or `F` |
| `PROC_UPDATE_STEP_START` | ADF (before each stage) | Opens a step run in `STEP_RUN_TBL` |
| `PROC_UPDATE_STEP_END` | ADF (after each stage) | Closes the step run |
| `PROC_UPDATE_JOB_START` | ADF (before each ForEach iteration) | Opens a job run in `JOB_RUN_TBL` |
| `PROC_UPDATE_JOB_END` | ADF (after each ForEach iteration) | Closes the job run with row counts and status |
| `DELETE_MANIFEST_DATA` | Notebook 02 | Clears MANIFEST rows for a given XCENTER before re-inserting |
| `GET_GW_FILE_METADATA_MASTER` | Notebook 02 | Updates `TABLE_LOAD_METADATA` with latest fingerprints and timestamps |
| `PROC_VALIDATION_START` | ADF (validation stage) | Resets validation rule statuses for the new cycle |

---

### `abc_tables_backup_ddl.sql`

DDL for the end-of-cycle backup tables (one `_BACKUP` shadow table per operational table that gets cleared and repopulated every cycle).

These backup tables are populated by an ADF `ForEach` activity at the end of each pipeline run:
- Pre-copy script: `TRUNCATE TABLE abc.<table>_BACKUP`
- Source: `SELECT * FROM abc.<table> WHERE XCENTER = 'CC'`
- Sink: bulk insert into the `_BACKUP` table

Purpose: if a pipeline run fails, the `_BACKUP` tables preserve the last known good state for debugging and recovery. Static config tables (`CYC_CTRL_TBL`, `JOB_CTRL_TBL`, etc.) and append-only audit logs (`CYC_RUN_TBL`, `JOB_RUN_TBL`, etc.) are not backed up as they are either never overwritten or always additive.

**Run after** `abc_tables_ddl.sql`.

---

### `refined_tables_ddl.sql`

DDL for the refined layer Delta tables registered in Unity Catalog as external tables.

Each refined table is defined as:

```sql
CREATE TABLE IF NOT EXISTS insurance_claims_domain.refined.<table_name>
USING DELTA LOCATION 'abfss://insurance-claims-domain@<storage-account>.dfs.core.windows.net/refined/claims/<table_name>/'
```

The Delta files live in ADLS; Unity Catalog holds only the metadata pointer. Dropping the table definition does not delete the underlying Delta files.

---

## Setup Order

```
1. abc_tables_ddl.sql          -- creates schema, tables, and seeds control rows
2. abc_stored_procedures_ddl.sql  -- creates all stored procedures
3. abc_tables_backup_ddl.sql   -- creates _BACKUP shadow tables
4. refined_tables_ddl.sql      -- registers refined Delta tables in Unity Catalog
```
