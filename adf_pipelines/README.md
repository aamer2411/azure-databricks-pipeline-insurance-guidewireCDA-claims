# ADF Pipelines

Azure Data Factory pipeline definitions exported as JSON. These files contain the full activity configuration for each pipeline stage including activity chains, expressions, parameters, and dependency conditions.

To use these in your own ADF instance: `Author > Pipelines > ... > Import from JSON`. After import, update all linked service references (`LS_ADB_*`, `LS_ASQL_*`) and dataset references (`ds_asql_abc`, `ds_adls_*`) to match your environment.

---

## Pipeline Inventory

### `pl_m_source_to_refined.json` - Master Pipeline

The top-level orchestrator. Runs the full end-to-end pipeline for a single cycle.

**Activity sequence:**

1. **Client Dummy Data Load** - Triggers notebook 00 to generate a new batch of simulated Guidewire CDA data in ADLS `client_data/`
2. **Wait until prev cycle is C or F** - `Until` loop polling `abc.CYC_CTRL_TBL`. Blocks the new cycle from starting if a previous cycle is still running (`CYC_STS_CD='I'`)
3. **Wait until prev Steps and Jobs are C or F** - `Until` loop polling `abc.STEP_RUN_TBL` and `abc.JOB_RUN_TBL`. Ensures no stale in-progress records remain before opening a new cycle
4. **PROC_UPDATE_CYC_START** - Stored procedure call that opens a new cycle run entry in `abc.CYC_RUN_TBL`
5. **ExecutePipeline: pl_c_source_to_raw** - Runs the manifest loading and Source-to-Raw stage
6. **ExecutePipeline: pl_c_raw_to_replica** - Runs the Raw-to-Replica stage (with balancing and validation)
7. **ExecutePipeline: pl_c_replica_to_refined** - Runs the Replica-to-Refined SCD Type 2 stage
8. **PROC_UPDATE_CYC_END** - Closes the cycle run with final status

---

### `pl_c_source_to_raw.json` - Source to Raw

Child pipeline covering Stage 1 (Manifest) and Stage 2 (Source to Raw).

**Activity sequence:**

1. **PROC_UPDATE_STEP_START** - Opens the manifest step in `abc.STEP_RUN_TBL`
2. **Notebook 01** - Copy manifest.json from source to raw zone
3. **Notebook 02** - Parse manifest, load `abc.MANIFEST`, update `abc.TABLE_LOAD_METADATA`
4. **Notebook 03** - Detect files to load, stage file list in global temp view
5. **Notebook 04** - Populate `abc.CDA_FILE_LOADS` with one row per new parquet file
6. **Notebook 05** - Check run status and set restart flags
7. **PROC_UPDATE_STEP_END** - Closes the manifest step
8. **Populate TABLES_TO_LOAD** - Stored procedure fills the staging table used by the ForEach
9. **PROC_UPDATE_STEP_START** - Opens the Source-to-Raw step
10. **ForEach: Source to Raw** - Runs notebook 06 in parallel for each table in `TABLES_TO_LOAD`. Each iteration calls `PROC_UPDATE_JOB_START`, runs the notebook, then calls `PROC_UPDATE_JOB_END` with the returned row counts
11. **PROC_UPDATE_STEP_END** - Closes the Source-to-Raw step

---

### `pl_c_raw_to_replica.json` - Raw to Replica

Child pipeline covering Stage 3 (Raw to Replica) including all balancing and validation notebooks.

**Activity sequence:**

1. **PROC_UPDATE_STEP_START** - Opens the Raw-to-Replica step
2. **Populate TABLES_TO_LOAD_RPL** - Fills the ForEach staging table for this stage
3. **ForEach: Raw to Replica** - Runs notebook 07 in parallel for each table. Each iteration calls `PROC_UPDATE_JOB_START/END` to track individual table runs
4. **Notebook 08** - Replica balancing pass 1
5. **Notebook 09** - Replica balancing pass 2
6. **Notebook 10** - Replica balancing final reconciliation (pass/fail verdict per table)
7. **PROC_UPDATE_STEP_END** - Closes the Raw-to-Replica step
8. **ExecutePipeline: pl_gc_replica_data_validation** - Triggers the validation sub-pipeline

---

### `pl_c_replica_to_refined.json` - Replica to Refined

Child pipeline covering Stage 4 (SCD Type 2 transformation from replica to refined layer).

**Activity sequence:**

1. **Lookup: prev_refined_cyc_run_sk** - Queries `abc.CYC_RUN_TBL` for the `CYC_RUN_SK` of the last successful Replica-to-Refined run. This is passed to notebook 12 to correctly scope soft-delete and retired-record propagation
2. **PROC_UPDATE_STEP_START** - Opens the Replica-to-Refined step
3. **Populate TABLES_TO_LOAD_RFN** - Fills the ForEach staging table for refined jobs
4. **ForEach: Replica to Refined** - Runs notebook 12 in parallel for each refined table, passing `JOB_SK` and `prev_refined_cyc_run_sk` per iteration
5. **Notebook 13** - Refined layer balancing and reconciliation
6. **PROC_UPDATE_STEP_END** - Closes the Replica-to-Refined step

---

### `pl_gc_relica_data_validation.json` - Replica Data Validation

Sub-pipeline for data quality checks on the replica layer.

**Activity sequence:**

1. **PROC_VALIDATION_START** - Resets validation rule statuses in `abc.VALIDATION_CTRL_TBL` for the new cycle
2. **Notebook 11** - Executes all validation rules from `abc.VALIDATION_CTRL_TBL` and logs failures to `abc.VALIDATION_ERROR_LOG`

---

## ADF Naming Convention

All pipeline names follow the pattern `pl_{scope}_{stage}`:

| Prefix | Meaning |
|--------|---------|
| `pl_m_` | Master pipeline |
| `pl_c_` | Child pipeline (called by master via ExecutePipeline) |
| `pl_gc_` | Grandchild pipeline (called by a child pipeline) |
