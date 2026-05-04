# Databricks notebook source
# MAGIC %md
# MAGIC ## 07 Copy ADLS Raw Parquet to Replica External Delta Tables
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Raw to Replica
# MAGIC
# MAGIC **Purpose:** For a single table, reads the pending file list from `RAW_TABLE_LOAD`,
# MAGIC loads parquet from ADLS `raw/`, and MERGEs it into external Delta tables registered
# MAGIC in Unity Catalog under `insurance_claims_domain.cda_replica.*`.
# MAGIC
# MAGIC The Delta files live in ADLS `replica/` — Unity Catalog holds only the metadata pointer
# MAGIC (`USING DELTA LOCATION`). Dropping the UC table does not delete the underlying files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How it works
# MAGIC
# MAGIC **Initial load** (first time this table runs — no Delta table yet):
# MAGIC 1. Read ALL parquet under `raw/{TYPE_DATA}/{table}/*/*/*.parquet`
# MAGIC 2. Add audit columns: `CYC_RUN_SK`, `GW_FINGERPRINT`, `GW_TIMESTAMP`, `ABC_AUDIT_DATE_TIME`, `Validation_Flag`, `Soft_Delete`
# MAGIC 3. Deduplicate: keep the latest event per `id` using `GW_TIMESTAMP → payload_ts_ms → seqval_hex`
# MAGIC 4. Drop `gwcbi___operation=1` (delete before-images — nothing to delete on first load)
# MAGIC 5. Write Delta files to `replica/` in ADLS and register as external table in Unity Catalog
# MAGIC
# MAGIC **Incremental load** (table already exists — merge new delivery):
# MAGIC 1. For each fingerprint in `RAW_TABLE_LOAD`, read all timestamp folders for that fingerprint
# MAGIC 2. Add audit columns (GW_FINGERPRINT / GW_TIMESTAMP set from the batch, not from filepath)
# MAGIC 3. Deduplicate within the batch: keep latest per `(id, GW_FINGERPRINT)`
# MAGIC 4. Handle schema drift: cast mismatched columns to match the existing replica schema
# MAGIC 5. Split by CDC operation:
# MAGIC    - `operation = 0, 2, 4` → MERGE into replica (update if id exists, insert if new)
# MAGIC    - `operation = 1` → soft delete (SET `Soft_Delete='Y'` — row stays, never physically removed)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Reconciliation formula
# MAGIC `ROWS_READ == ROWS_LOADED`
# MAGIC where `ROWS_LOADED = TARGET_ROWS_LOADED + ROWS_DUPLICATE + ROWS_DELETED`
# MAGIC
# MAGIC - `ROWS_READ` — raw parquet row count for this cycle's pending timestamps
# MAGIC - `TARGET_ROWS_LOADED` — rows in replica with `CYC_RUN_SK = current` AND `Soft_Delete='N'`
# MAGIC - `ROWS_DUPLICATE` — rows dropped by deduplication (`ROWS_RAW - rows_dedup`)
# MAGIC - `ROWS_DELETED` — rows soft-deleted (`operation=1`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | Source | ADLS raw/ (copied from S3 by Src→Raw) | Same — ADLS `raw/` |
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Schema param | `config_schema` widget | `ASQL_SCHEMA` widget — same purpose |
# MAGIC | ROWS_READ | MANIFEST − MANIFEST_ARCHIVE delta | Count of raw parquet rows for pending timestamps |

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters and Metric Variables
import numpy as np
import json
from pyspark.sql.functions import (
    col, lit, hex, input_file_name, reverse, split,
    current_timestamp, row_number
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ADF passes these parameters to the notebook at runtime.
# TABLE_NAME comes from each ForEach iteration (@item().TGT_FILE_TBL from TABLES_TO_LOAD_RPL).
# All other parameters are pipeline-level parameters.
#
# Parameters to create in ADF:
#   Name              Type     Source
#   -----------------------------------------------------------------------
#   TABLE_NAME        String   @item().TGT_FILE_TBL         (ForEach item — lowercase)
#   TYPE_DATA         String   pipeline parameter  e.g. claims
#   XCENTER           String   pipeline parameter  e.g. CC
#   ASQL_SCHEMA       String   pipeline parameter  e.g. abc
#   CYC_SK            String   pipeline parameter  e.g. 101
#   CYC_RUN_SK        String   output of PROC_UPDATE_CYC_START (from Src->Raw pipeline)
#   ADLS_RAW_PATH     String   pipeline parameter  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/raw
#   ADLS_REPLICA_PATH String   pipeline parameter  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/replica
#   Processed_DB      String   pipeline parameter  e.g. insurance_claims_domain
#   Processed_Schema  String   pipeline parameter  e.g. cda_replica
#   -----------------------------------------------------------------------

dbutils.widgets.text("TABLE_NAME", "")
TABLE_NAME       = str(dbutils.widgets.get("TABLE_NAME"))
TABLE_NAME_lower = TABLE_NAME.lower()
TABLE_NAME_upper = TABLE_NAME.upper()

dbutils.widgets.text("TYPE_DATA", "")
TYPE_DATA = dbutils.widgets.get("TYPE_DATA")

dbutils.widgets.text("XCENTER", "")
xcenter = dbutils.widgets.get("XCENTER")

dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

dbutils.widgets.text("CYC_RUN_SK", "")
cyc_run_sk = dbutils.widgets.get("CYC_RUN_SK")

dbutils.widgets.text("ADLS_RAW_PATH", "")
ADLS_RAW_PATH = dbutils.widgets.get("ADLS_RAW_PATH")

dbutils.widgets.text("ADLS_REPLICA_PATH", "")
ADLS_REPLICA_PATH = dbutils.widgets.get("ADLS_REPLICA_PATH")

dbutils.widgets.text("Processed_DB", "")
Processed_DB = dbutils.widgets.get("Processed_DB")

dbutils.widgets.text("Processed_Schema", "")
Processed_Schema = dbutils.widgets.get("Processed_Schema")

# Metric and status tracking variables — updated during load_data()
STATUS         = ''
error_msg      = ''
ROWS_READ      = 0
ROWS_RAW       = 0
rows_dedup     = 0
ROWS_DUPLICATE = 0
ROWS_DELETED   = 0
rows_deleted   = 0
ROWS_INSERTED  = 0
ROWS_UPDATED   = 0
TARGET_ROWS_LOADED = 0
ROWS_LOADED    = 0

print(f"TABLE_NAME        : {TABLE_NAME_lower}")
print(f"TYPE_DATA         : {TYPE_DATA}")
print(f"XCENTER           : {xcenter}")
print(f"ASQL_SCHEMA       : {asql_schema}")
print(f"CYC_SK            : {CYC_SK}")
print(f"CYC_RUN_SK        : {cyc_run_sk}")
print(f"ADLS_RAW_PATH     : {ADLS_RAW_PATH}")
print(f"ADLS_REPLICA_PATH : {ADLS_REPLICA_PATH}")
print(f"Processed_DB      : {Processed_DB}")
print(f"Processed_Schema  : {Processed_Schema}")

# COMMAND ----------

# DBTITLE 1,Configure ADLS Storage Key and Azure SQL Connection
# Production approach:
#   Server, database name, username, and password fetched from Azure Key Vault
#   via dbutils.secrets.get("testsecretkv2", <secret-name>).
#
# This project:
#   Values are hardcoded — no Key Vault configured for this practice environment.
#   Storage key set via spark.conf.set (consistent with other practice notebooks).

AsqldbServer   = "<your-sql-server>.database.windows.net"
AsqldbName     = "<your-database-name>"
AsqldbUserName = "<your-sql-username>"
AsqldbPassword = "<your-sql-password>"  # In production: dbutils.secrets.get("your-kv-scope", "AsqldbPassword")

url_Asql = (
    f"jdbc:sqlserver://{AsqldbServer};"
    f"databaseName={AsqldbName};"
    f"user={AsqldbUserName};password={AsqldbPassword};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;socketTimeout=60000"
)

print(f"Connecting to: {AsqldbServer} / {AsqldbName}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def run_asql_query(query: str):
    """
    Execute a DML statement or stored proc call against Azure SQL.
    Opens a JDBC connection, runs the statement, then always closes the connection.
    Used for INSERT / UPDATE / DELETE / EXECUTE — not for SELECT.
    """
    conn = None
    stmt = None
    try:
        conn = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(url_Asql)
        stmt = conn.createStatement()
        stmt.executeUpdate(query)
    finally:
        if stmt: stmt.close()
        if conn:  conn.close()


def get_metadata(table_or_query: str, is_query: bool):
    """
    Read data from Azure SQL via Spark JDBC and return a Spark DataFrame.
    is_query=False: table_or_query is a table name.
    is_query=True:  table_or_query is a SQL SELECT statement.
    """
    option_key = "query" if is_query else "dbtable"
    return (
        spark.read
        .format("jdbc")
        .option("url", url_Asql)
        .option(option_key, table_or_query)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("queryTimeout", "60")
        .load()
    )

# COMMAND ----------

# DBTITLE 1,Spark Configuration
# Adaptive query execution: Spark automatically optimises join strategies and
# partition sizes at runtime — important for large skewed parquet datasets.
spark.sql("SET spark.sql.adaptive.enabled = true")

# Auto-merge schema: allows Delta to accept new columns from source parquet
# without failing — handles schema evolution between GW CDA deliveries.
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# Disable ANSI mode: source columns (e.g. claimid, policyid) may contain
# string values like 'cc:128141' that cannot be cast to BIGINT. With ANSI
# off, invalid casts return NULL instead of raising CAST_INVALID_INPUT.
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Load Pending File List from RAW_TABLE_LOAD
# RAW_TABLE_LOAD was populated by notebook 06 (Src→Raw) for this cycle.
# Each row represents one parquet folder — identified by a (fingerprint, timestamp) pair:
#
#   GW_FINGERPRINT : 32-char MD5 of the table schema (e.g. f0a7604049cecc6facff72cad2e0d6cb)
#                    Changes only when GW CDA detects a schema change for that table.
#   GW_TIMESTAMP   : Unix epoch ms of the delivery folder
#                    (e.g. 1741478400000 → one folder per GW CDA run)
#
# Together they form the path: raw/{TYPE_DATA}/{table}/{fingerprint}/{timestamp}/
#
# READ UNCOMMITTED avoids lock contention — RAW_TABLE_LOAD is only written by nb06
# so dirty reads are safe here.

connection = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(url_Asql)
statement  = connection.createStatement()
statement.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

RAW_TABLE_LOAD_df = get_metadata(
    f"SELECT DISTINCT GW_FINGERPRINT, GW_TIMESTAMP "
    f"FROM {asql_schema}.RAW_TABLE_LOAD WITH (NOLOCK) "
    f"WHERE TABLE_NAME = '{TABLE_NAME_upper}' AND XCENTER = '{xcenter}'",
    is_query=True
)

# Build a 2D numpy array of (fingerprint, timestamp) pairs.
# This is the same pattern used in the production Raw→Replica notebook.
ts_2d_array = np.array(RAW_TABLE_LOAD_df.collect())
print(f"Pending fingerprint/timestamp pairs for {TABLE_NAME_lower}: {len(ts_2d_array)}")
print(ts_2d_array)

statement.close()
connection.close()

# COMMAND ----------

# DBTITLE 1,Calculate ROWS_READ (Source Row Count)
# ROWS_READ is the "source" side of the reconciliation check at the end.
# It must equal ROWS_LOADED (= TARGET_ROWS_LOADED + ROWS_DUPLICATE + ROWS_DELETED)
# for the job to be marked SUCCESS.
#
# Production derives ROWS_READ from the MANIFEST table by subtracting the
# previous run's totalProcessedRecordsCount (stored in MANIFEST_ARCHIVE).
#
# This project counts raw parquet rows directly for the pending timestamps —
# simpler and equally accurate since those are exactly the files being loaded.

ROWS_READ = 0
if len(ts_2d_array) > 0:
    blob_paths = [
        f"{ADLS_RAW_PATH}/{TYPE_DATA}/{TABLE_NAME_lower}/{fp}/{ts}/"
        for fp, ts in ts_2d_array
    ]
    df_raw_count = spark.read.parquet(*blob_paths, inferSchema=True)
    ROWS_READ = df_raw_count.count()

print(f"ROWS_READ : {ROWS_READ}")

# COMMAND ----------

# DBTITLE 1,Delta Table Helper — create_table() for Initial Load
def create_table(database, schema, table_name, df, path):
    """
    Write df as a Delta table to path and register it in Unity Catalog.
    Used only for the initial load — incremental loads use MERGE.
    """
    df.persist()
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .options(header=True, encoding="UTF-8") \
        .option("mergeSchema", "true") \
        .save(path)
    df.unpersist()

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} "
        f"USING DELTA LOCATION '{path}'"
    )
    print(f"{table_name} Delta table created at {path}")

# COMMAND ----------

# DBTITLE 1,CDC Load Logic — Initial Load or Incremental MERGE
# STATUS and row-count globals are set inside load_data() via the global keyword.
# The function returns (ROWS_RAW, rows_dedup, rows_deleted) as a tuple so the
# outer scope can unpack them into the named metric variables in Stage 9.
# Any unhandled exception sets STATUS='F' and is surfaced via error_msg.

global STATUS, error_msg
global ROWS_RAW, rows_dedup, rows_deleted

# Target Delta table path in ADLS — same path used to register the table in Unity Catalog.
tgt_path = f"{ADLS_REPLICA_PATH}/{TYPE_DATA}/{TABLE_NAME_lower}"

try:

    def load_data():
        global STATUS, error_msg
        global ROWS_RAW, rows_dedup, rows_deleted

        ROWS_RAW     = 0   # total raw parquet rows read (before dedup)
        rows_dedup   = 0   # rows after deduplication
        rows_deleted = 0   # rows soft-deleted (gwcbi___operation=1)

        # ================================================================== #
        # INITIAL LOAD                                                         #
        # Condition: the Delta table does not yet exist in Unity Catalog.      #
        # Action: read ALL raw parquet, dedup, create the Delta table.        #
        # ================================================================== #
        if not spark.catalog.tableExists(f"{Processed_DB}.{Processed_Schema}.{TABLE_NAME_lower}"):
            print("Initial load: table not found — creating from all raw parquet files.")

            # Read ALL parquet files for this table across every fingerprint and
            # every timestamp folder delivered so far.
            # Glob pattern: {table}/{fingerprint}/{timestamp}/*.parquet
            df = (
                spark.read
                .format("parquet")
                .option("inferSchema", "true")
                .load(f"{ADLS_RAW_PATH}/{TYPE_DATA}/{TABLE_NAME_lower}/*/*/*.parquet")
            )

            # GW CDA encodes spatial (geometry) columns as a struct {wkb: binary, srid: int}.
            # Convert the WKB binary to a hex string so Delta can store it as StringType.
            # Replica tables in production use the hex representation throughout.
            for spatial_col in ["spatialpoint", "losslocationspatialdenorm", "spatialpointdenorm"]:
                if spatial_col in df.columns:
                    df = df.withColumn(spatial_col, hex(col(f"{spatial_col}.wkb")))

            # Add ABC pipeline audit columns:
            #   CYC_RUN_SK         — ties this row to the current pipeline cycle run
            #   GW_FINGERPRINT     — extracted from the folder path (position [-3] from right)
            #                        tracks which schema version this row came from
            #   GW_TIMESTAMP       — extracted from the folder path (position [-2] from right)
            #                        identifies which GW CDA delivery this row belongs to
            #   ABC_AUDIT_DATE_TIME — when this row was written to the replica layer
            #   Validation_Flag    — populated downstream by validation rules; NULL at load time
            #   Soft_Delete        — 'N' for all rows on initial load (no deletes yet)
            df1 = (
                df
                .withColumn("CYC_RUN_SK",          lit(cyc_run_sk).cast(IntegerType()))
                .withColumn("filepath",             input_file_name())
                .withColumn("GW_FINGERPRINT",       reverse(split(col("filepath"), "/"))[2])
                .withColumn("GW_TIMESTAMP",         reverse(split(col("filepath"), "/"))[1])
                .withColumn("ABC_AUDIT_DATE_TIME",  current_timestamp())
                .withColumn("Validation_Flag",      lit(None).cast(StringType()))
                .withColumn("Soft_Delete",          lit("N").cast(StringType()))
                .drop("filepath")  # filepath was only needed to extract fingerprint/timestamp
            )

            # Count raw rows BEFORE dedup — this feeds into ROWS_DUPLICATE later.
            ROWS_RAW = df1.count()

            # Deduplicate: GW CDA can deliver multiple events for the same row id
            # (e.g. an INSERT followed by an UPDATE in the same delivery batch).
            # Keep only the most recent event per id using a three-level sort:
            #   1. GW_TIMESTAMP      — latest delivery folder wins
            #   2. gwcbi___payload_ts_ms — latest DB change timestamp wins
            #   3. gwcbi___seqval_hex   — latest sequence position wins (tie-breaker)
            # Partition key on initial load is just `id` — all fingerprints are read at once.
            w = Window.partitionBy("id").orderBy(
                col("GW_TIMESTAMP").desc(),
                col("gwcbi___payload_ts_ms").desc(),
                col("gwcbi___seqval_hex").desc()
            )
            df1 = df1.withColumn("rnk", row_number().over(w)).filter(col("rnk") == 1).drop("rnk")

            rows_dedup = df1.count()

            # operation=1 is a DELETE before-image — GW CDA sends the row state before
            # it was deleted. On the very first load there is nothing to delete in the
            # target, so these rows are simply counted and excluded from the write.
            df_deleted_initial = df1.filter(col("gwcbi___operation") == 1)
            rows_deleted       = df_deleted_initial.count()

            df_final = df1.filter(col("gwcbi___operation") != 1)

            # Write df_final as a Delta table and register it in Unity Catalog so
            # it is queryable as {Processed_DB}.{Processed_Schema}.{table}.
            create_table(Processed_DB, Processed_Schema, TABLE_NAME_lower, df_final, tgt_path)

        # ================================================================== #
        # INCREMENTAL LOAD                                                     #
        # Condition: the Delta table already exists — this is a subsequent run.#
        # Action: for each fingerprint batch, MERGE upserts and soft-delete    #
        #         operation=1 rows into the existing replica Delta table.      #
        # ================================================================== #
        else:
            print("Incremental load: table found — merging new data.")

            # Process one fingerprint at a time.
            # A new fingerprint folder appears when GW CDA detects a schema change
            # in that table — it is a separate schema version alongside the old one.
            fingerprints = list({fp for fp, _ in ts_2d_array})

            # Empty DataFrame to accumulate all distinct IDs touched this cycle.
            # Used at the end to compute rows_dedup = distinct IDs across all batches.
            schema_id   = StructType([StructField("id", IntegerType(), True)])
            df_dedup_id = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema_id)

            for fp in fingerprints:
                # Collect all timestamp folders for this fingerprint.
                # Multiple timestamps exist when GW CDA ran more than once since
                # the last pipeline cycle (each run adds a new timestamp folder).
                ts_paths = [
                    f"{ADLS_RAW_PATH}/{TYPE_DATA}/{TABLE_NAME_lower}/{fp}/{ts}/"
                    for i, ts in ts_2d_array
                    if i == fp
                ]

                # GW_TIMESTAMP is set to the LAST timestamp in the batch.
                # This matches production behaviour — the audit column records the
                # latest delivery time, not the individual file timestamp.
                last_ts = [ts for i, ts in ts_2d_array if i == fp][-1]

                # Read all timestamp folders for this fingerprint in one Spark read.
                new_data = spark.read.parquet(*ts_paths, inferSchema=True)

                # Spatial column handling — same as initial load.
                for spatial_col in ["spatialpoint", "losslocationspatialdenorm", "spatialpointdenorm"]:
                    if spatial_col in new_data.columns:
                        new_data = new_data.withColumn(spatial_col, hex(col(f"{spatial_col}.wkb")))

                # Add audit columns.
                # GW_FINGERPRINT and GW_TIMESTAMP are set from the batch variables (not
                # from filepath) because all files in ts_paths share the same fingerprint
                # and we record the last delivery timestamp as the representative value.
                new_data = (
                    new_data
                    .withColumn("GW_FINGERPRINT",      lit(fp))
                    .withColumn("GW_TIMESTAMP",        lit(last_ts))
                    .withColumn("CYC_RUN_SK",          lit(cyc_run_sk).cast(IntegerType()))
                    .withColumn("ABC_AUDIT_DATE_TIME", current_timestamp())
                    .withColumn("Validation_Flag",     lit(None).cast(StringType()))
                )

                # Accumulate raw row count before dedup.
                ROWS_RAW += new_data.count()

                # Deduplicate within this fingerprint batch.
                # Partition key includes GW_FINGERPRINT here (unlike initial load)
                # because the incremental window is scoped to one fingerprint at a time.
                w = Window.partitionBy("id", "GW_FINGERPRINT").orderBy(
                    col("GW_TIMESTAMP").desc(),
                    col("gwcbi___payload_ts_ms").desc(),
                    col("gwcbi___seqval_hex").desc()
                )
                new_data_1 = (
                    new_data
                    .withColumn("rnk", row_number().over(w))
                    .filter(col("rnk") == 1)
                    .drop("rnk")
                )

                # Union the touched IDs into df_dedup_id.
                # distinct().count() at the end gives the true unique ID count
                # across all fingerprint batches processed this cycle.
                df_dedup_id = df_dedup_id.union(new_data_1.select("id"))

                # Schema drift detection: compare source parquet dtypes against the
                # existing replica Delta table dtypes for columns they share.
                # If a column type changed (e.g. int → long), cast the source column
                # to the TARGET type so the MERGE does not fail on type mismatch.
                delta_table   = DeltaTable.forPath(spark, tgt_path)
                source_dtypes = dict(new_data_1.dtypes)
                target_dtypes = dict(delta_table.toDF().dtypes)
                common_cols   = set(source_dtypes.keys()).intersection(target_dtypes.keys())
                mismatches    = {
                    c: target_dtypes[c]
                    for c in common_cols
                    if source_dtypes[c] != target_dtypes[c]
                }
                if mismatches:
                    print(f"Datatype changes detected for {TABLE_NAME_lower}: {mismatches}")
                    new_data_1 = new_data_1.withColumns(
                        {c: col(c).cast(t) for c, t in mismatches.items()}
                    )

                # Split by GW CDA CDC operation code:
                #   0 = fresh INSERT (new row in source)
                #   2 = INSERT after-image (row was updated — this is the new state)
                #   4 = full UPDATE (row changed in place)
                #   1 = DELETE before-image (row was deleted — this is the old state)
                #
                # operation=1 rows go through soft delete (Soft_Delete='Y') rather than
                # physical removal so the row history is preserved for downstream analytics.
                df_deletes = new_data_1.filter(col("gwcbi___operation") == 1)
                df_upserts = new_data_1.filter(
                    col("gwcbi___operation").isin(0, 2, 4)
                ).withColumn("Soft_Delete", lit("N").cast(StringType()))

                # Count once so we don't trigger a second Spark job on df_deletes later.
                del_count     = df_deletes.count()
                rows_deleted += del_count

                # MERGE upserts into the existing replica Delta table.
                # Match condition: source id = target id (natural key).
                # whenMatchedUpdateAll  — row already exists → overwrite all columns
                # whenNotMatchedInsertAll — new id → insert as a new row
                print(f"Running MERGE for {TABLE_NAME_lower} (fingerprint: {fp})")
                delta_table.alias("t") \
                    .merge(df_upserts.alias("s"), "s.id = t.id") \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()

                # Soft delete: for operation=1 rows, flag the corresponding replica row
                # as deleted by setting Soft_Delete='Y'.  The row is kept in the table
                # so downstream refined / analytics layers can track the deletion event.
                # CYC_RUN_SK is also updated so the deletion is tied to this cycle run.
                if del_count > 0:
                    print(f"Applying soft deletes for {TABLE_NAME_lower} ({del_count} rows)")
                    df_deletes.createOrReplaceTempView("deleted_view")
                    spark.sql(
                        f"UPDATE {Processed_DB}.{Processed_Schema}.{TABLE_NAME_lower} "
                        f"SET Soft_Delete = 'Y', CYC_RUN_SK = {cyc_run_sk} "
                        f"WHERE id IN (SELECT id FROM deleted_view)"
                    )

            # Distinct count across all fingerprint batches = true deduplicated rows
            # processed this cycle (used to compute ROWS_DUPLICATE in Stage 9).
            rows_dedup = df_dedup_id.distinct().count()
            print(f"Incremental load complete for {TABLE_NAME_lower}")

        return ROWS_RAW, rows_dedup, rows_deleted

    metrics = load_data()

except Exception as e:
    STATUS    = 'F'
    error_msg = f"Error during Raw->Replica load for {TABLE_NAME_lower}: {str(e)}"
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Row Count Metrics
# Unpack the (ROWS_RAW, rows_dedup, rows_deleted) tuple returned by load_data().
ROWS_RAW       = metrics[0]
rows_dedup     = metrics[1]
rows_deleted   = metrics[2]

# ROWS_DUPLICATE: rows present in raw parquet that were removed by deduplication.
# These are legitimate duplicates delivered by GW CDA (e.g. the same id appeared
# in multiple timestamp folders). They are accounted for in ROWS_LOADED so the
# reconciliation ROWS_READ == ROWS_LOADED still holds.
ROWS_DUPLICATE = ROWS_RAW - rows_dedup
ROWS_DELETED   = rows_deleted

# TARGET_ROWS_LOADED: count of rows in the replica table that were written or
# updated in THIS cycle (CYC_RUN_SK = current) and are not soft-deleted.
# This includes both newly inserted rows and rows updated by a MERGE.
df_loaded = spark.sql(
    f"SELECT COUNT(*) AS load_count "
    f"FROM {Processed_DB}.{Processed_Schema}.{TABLE_NAME_lower} "
    f"WHERE CYC_RUN_SK = {int(cyc_run_sk)} AND Soft_Delete = 'N'"
)
TARGET_ROWS_LOADED = int(df_loaded.first().load_count)

# ROWS_INSERTED: rows with operation code 0 (new record) or 2 (after-image insert)
# written this cycle — i.e. the MERGE inserted or updated them as inserts.
df_inserted = spark.sql(
    f"SELECT COUNT(*) AS insert_count "
    f"FROM {Processed_DB}.{Processed_Schema}.{TABLE_NAME_lower} "
    f"WHERE CYC_RUN_SK = {int(cyc_run_sk)} AND gwcbi___operation IN (0, 2)"
)
ROWS_INSERTED = int(df_inserted.first().insert_count)

# ROWS_UPDATED: rows with operation code 4 (full update) written this cycle.
df_updated = spark.sql(
    f"SELECT COUNT(*) AS update_count "
    f"FROM {Processed_DB}.{Processed_Schema}.{TABLE_NAME_lower} "
    f"WHERE CYC_RUN_SK = {int(cyc_run_sk)} AND gwcbi___operation = 4"
)
ROWS_UPDATED = int(df_updated.first().update_count)

# ROWS_LOADED is the "target" side of the reconciliation check.
# Formula: TARGET_ROWS_LOADED + ROWS_DUPLICATE + ROWS_DELETED
#   — duplicates were in the source but dropped before writing → still "accounted for"
#   — soft-deleted rows were processed (and flagged) → still "accounted for"
# This ensures ROWS_READ == ROWS_LOADED when every source row is fully handled.
ROWS_LOADED = TARGET_ROWS_LOADED + ROWS_DUPLICATE + ROWS_DELETED

print(f"ROWS_READ          : {ROWS_READ}")
print(f"ROWS_RAW           : {ROWS_RAW}")
print(f"ROWS_DUPLICATE     : {ROWS_DUPLICATE}")
print(f"TARGET_ROWS_LOADED : {TARGET_ROWS_LOADED}")
print(f"ROWS_INSERTED      : {ROWS_INSERTED}")
print(f"ROWS_UPDATED       : {ROWS_UPDATED}")
print(f"ROWS_DELETED       : {ROWS_DELETED}")
print(f"ROWS_LOADED        : {ROWS_LOADED}")

# COMMAND ----------

# DBTITLE 1,Reconciliation Check, Status, and Exit
# Reconciliation check: every row that was read from source must be accounted for
# in the target — either written to Delta, deduplicated, or soft-deleted.
# If ROWS_READ != ROWS_LOADED it means rows were silently dropped → fail the job
# so ADF triggers the failure path and PROC_UPDATE_JOB_END records STATUS='F'.
#
# Note: if load_data() itself raised an exception, STATUS is already 'F' and
# we skip this check to preserve the original error message.

if STATUS != 'F':
    if ROWS_READ == ROWS_LOADED:
        STATUS    = 'S'
        error_msg = 'NULL'
    else:
        STATUS    = 'F'
        error_msg = (
            f"ROWS_READ ({ROWS_READ}) != ROWS_LOADED ({ROWS_LOADED}) "
            f"for table {TABLE_NAME_lower}"
        )

print(f"STATUS    : {STATUS}")
print(f"ERROR_MSG : {error_msg}")

# Return the job result to ADF as a JSON string via dbutils.notebook.exit().
# ADF reads this from the notebook activity output using:
#   @activity('Run Raw to Replica').output.runOutput
# The values are passed to PROC_UPDATE_JOB_END to close out the JOB_RUN_TBL row.
result = json.dumps({
    "STATUS":        STATUS,
    "ROWS_READ":     ROWS_READ,
    "ROWS_LOADED":   ROWS_LOADED,
    "ROWS_INSERTED": ROWS_INSERTED,
    "ROWS_UPDATED":  ROWS_UPDATED,
    "ROWS_DELETED":  ROWS_DELETED
})
print(result)
dbutils.notebook.exit(result)