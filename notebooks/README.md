# Notebooks

Databricks notebooks exported in `.py` source format. Each file maps to one pipeline stage and can be imported directly into a Databricks workspace via the UI (`Import > File`) or the Databricks CLI.

All notebooks are parameterized with `dbutils.widgets` and receive their inputs from Azure Data Factory at runtime. They can also be run manually by setting the widget values in the notebook UI.

---

## Notebook Inventory

| File | Stage | Purpose |
|------|-------|---------|
| `00 Code to simulate s3 data loads (Here we use ADLS).py` | Pre-pipeline | Simulates a Guidewire CDA data delivery. Generates INSERT/UPDATE/DELETE records for 5 claims tables and writes them as snappy-compressed parquet to ADLS `client_data/`, along with a `manifest.json`. Maintains CDC state across runs in a Unity Catalog table. |
| `01 Copy Manifest File from Source to ADLS.py` | Stage 1a - Manifest | Copies `manifest.json` from the source zone (`client_data/`) to the raw zone (`raw/claims/`). Equivalent to the production step that copies the manifest from AWS S3. |
| `02 Load Manifest Data to Azure SQL.py` | Stage 1b - Manifest | Reads `manifest.json` from the raw zone, parses each table entry, and loads the data into `abc.MANIFEST`. Calls the stored procedure `GET_GW_FILE_METADATA_MASTER` to update `abc.TABLE_LOAD_METADATA` with the latest fingerprints and timestamps. |
| `03 Detect and List Files to Load.py` | Stage 1c - Manifest | Determines whether this is a full load or incremental load by querying `abc.CDA_FILE_LOADS`. Lists all new parquet file paths from ADLS and stages them in a global temp view (`TEMP_FILE_INFO_TABLE`) for notebook 04. |
| `04 Populate CDA_FILE_LOADS Table.py` | Stage 1d - Manifest | Reads the staged file list from notebook 03 and writes one row per parquet file into `abc.CDA_FILE_LOADS` with `IS_LOADED=0`. This is the control table that drives the Source-to-Raw copy. |
| `05 Check Run Status and Set Restart Flags.py` | Pre-stage guard | Checks the current run status for cycle, steps, and jobs in the ABC metadata tables. Sets restart flags so that a failed pipeline can resume from the correct point rather than reprocessing everything. |
| `06 Copy Parquet Files from Source to ADLS Raw.py` | Stage 2 - Source to Raw | Reads `abc.CDA_FILE_LOADS` to find all files with `IS_LOADED=0`. Copies each parquet file from `client_data/` to the corresponding path in `raw/`. Marks each file as `IS_LOADED=1` on completion. |
| `07 Copy ADLS Raw Parquet to Replica External Delta Tables.py` | Stage 3 - Raw to Replica | Reads raw parquet files for a single table (one notebook execution per table, driven by ADF ForEach). Deduplicates within the batch using `ROW_NUMBER OVER (PARTITION BY id ORDER BY gwcbi___payload_ts_ms DESC)`. MERGEs into an external Delta table in Unity Catalog (`insurance_claims_domain.replica.*`). Handles all CDC operation codes (0=INSERT, 1=soft-delete, 2=after-image UPDATE, 4=UPDATE). |
| `08 Replica Table Balancing Part 1.py` | Stage 3 - Validation | First pass of row count reconciliation. Captures source (raw parquet) and target (replica Delta table) counts per table for the current cycle. Writes metrics to `abc.BALANCE_METRICS`. |
| `09 Replica Table Balancing Part 2.py` | Stage 3 - Validation | Second pass. Compares metrics written by notebook 08 against thresholds defined in the control tables. Flags tables where the replica count deviates beyond the allowed tolerance. |
| `10 Replica Table Balancing - Final Reconciliation.py` | Stage 3 - Validation | Final reconciliation check across all replica tables for the cycle. Produces a pass/fail verdict per table and writes summary results to `abc.BALANCE_METRICS`. ADF reads this result to decide whether to proceed to the Replica-to-Refined stage. |
| `11 Replica Tables Data Validation.py` | Stage 3 - Validation | Runs configurable data quality rules defined in `abc.VALIDATION_CTRL_TBL` against the replica Delta tables. Logs any rule failures to `abc.VALIDATION_ERROR_LOG` with row-level detail. |
| `12 Load Replica to Refined - SCD Type 2.py` | Stage 4 - Replica to Refined | The core transformation notebook. Reads a configurable SQL transformation query from `abc.JOB_PARM_TBL`, executes it against the replica layer, computes MD5 hash keys (`ETL_Key_Hash` for entity identity, `ETL_SCD2_Hash` for change detection), and MERGEs the result into an external Delta table in Unity Catalog (`insurance_claims_domain.refined.*`) using SCD Type 2 logic. Assigns sequential surrogate keys. Propagates soft-deletes and retired records from replica to refined. |
| `13 Refined Table Balancing - Final Reconciliation.py` | Stage 4 - Validation | Row count reconciliation for the refined layer. Compares source (replica) and target (refined) counts for the cycle and writes a pass/fail result to `abc.BALANCE_METRICS`. |

---

## Credential Placeholders

Notebooks that connect to Azure SQL contain the following placeholder values that must be replaced before running:

```python
AsqldbServer   = "<your-sql-server>.database.windows.net"
AsqldbName     = "<your-database-name>"
AsqldbUserName = "<your-sql-username>"
AsqldbPassword = "<your-sql-password>"
```

Notebook 00 contains:

```python
STORAGE_KEY = "<your-adls-storage-key>"
```

In a production environment, all secrets are retrieved from Azure Key Vault:

```python
AsqldbPassword = dbutils.secrets.get("your-kv-scope", "AsqldbPassword")
STORAGE_KEY    = dbutils.secrets.get("your-kv-scope", "adls-storage-key")
```
