# Databricks notebook source
# MAGIC %md
# MAGIC ## 09 Replica Table Balancing Part 2
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Raw to Replica (Balance Recon)
# MAGIC
# MAGIC **Purpose:** Prepares deduplicated source views for `cc_policy`, `cc_exposure`, and
# MAGIC `cc_contact` to be consumed by the balance reconciliation notebook (Part 2).
# MAGIC
# MAGIC For each table this notebook:
# MAGIC 1. Checks `RAW_TABLE_LOAD` to confirm the table was loaded in the current cycle.
# MAGIC 2. If loaded: reads raw parquet from ADLS `raw/`, extracts `GW_FINGERPRINT` and
# MAGIC    `GW_TIMESTAMP` from the file path, deduplicates by `id`, and stages
# MAGIC    `global_temp.{table}_source` for Part 2.
# MAGIC 3. If not loaded: uses the existing `balance_recon` Delta table as a fallback so
# MAGIC    Part 2 can still compute metrics from the prior cycle's state.
# MAGIC
# MAGIC Runs **in parallel** with `08 Replica Balance Recon - Source Prep Part 1` and with
# MAGIC the ForEach replica table loading. Part 2 waits for both source-prep notebooks AND
# MAGIC the ForEach to complete before running the final reconciliation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | Source | AWS S3 (`BUCKET` + `ext_s3_path`) | ADLS `raw/` (`ADLS_RAW_PATH` widget) |
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Schema param | `config_schema` widget | `ASQL_SCHEMA` widget - same purpose |
# MAGIC | Lookup tables (Pattern B) | `cctl_costtype`, `cctl_underwritingcompanytype` | None - all 5 tables are primary tables |
# MAGIC | Parallelism | 4 notebooks (Parts 11-14) | 2 notebooks (Part 1 + Part 2) |

# COMMAND ----------

# DBTITLE 1,Imports and Pipeline Parameters
import json
from pyspark.sql.functions import col, input_file_name, reverse, split, row_number
from pyspark.sql.window import Window

# ADF passes these parameters at runtime.
# Parameters to create in ADF:
#   Name               Type    Source
#   ------------------------------------------------------------------
#   CYC_SK             String  pipeline parameter  e.g. 101
#   ASQL_SCHEMA        String  pipeline parameter  e.g. abc
#   XCENTER            String  pipeline parameter  e.g. CC
#   TYPE_DATA          String  pipeline parameter  e.g. claims
#   ADLS_RAW_PATH      String  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/raw
#   ADLS_BALANCE_PATH  String  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/balance_recon/
#   ------------------------------------------------------------------

dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

dbutils.widgets.text("XCENTER", "")
xcenter = dbutils.widgets.get("XCENTER")

dbutils.widgets.text("TYPE_DATA", "")
TYPE_DATA = dbutils.widgets.get("TYPE_DATA")

dbutils.widgets.text("ADLS_RAW_PATH", "")
ADLS_RAW_PATH = dbutils.widgets.get("ADLS_RAW_PATH")

dbutils.widgets.text("ADLS_BALANCE_PATH", "")
ADLS_BALANCE_PATH = dbutils.widgets.get("ADLS_BALANCE_PATH")

ADLS_BALANCE_PATH = ADLS_BALANCE_PATH.rstrip("/") + "/"

print(f"CYC_SK            : {CYC_SK}")
print(f"ASQL_SCHEMA       : {asql_schema}")
print(f"XCENTER           : {xcenter}")
print(f"TYPE_DATA         : {TYPE_DATA}")
print(f"ADLS_RAW_PATH     : {ADLS_RAW_PATH}")
print(f"ADLS_BALANCE_PATH : {ADLS_BALANCE_PATH}")

# COMMAND ----------

# DBTITLE 1,Configure ADLS Storage Key and Azure SQL Connection
# Production approach:
#   Server, database name, username, and password fetched from Azure Key Vault
#   via dbutils.secrets.get("testsecretkv2", <secret-name>).
#
# This project:
#   Values are hardcoded - no Key Vault configured for this practice environment.
#   Storage key set via spark.conf.set (consistent with all practice notebooks).

spark.conf.set(
    "fs.azure.account.key.azurepractice68256.dfs.core.windows.net",
    "<your-adls-storage-key>"
)

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

# DBTITLE 1,Helper Functions - get_metadata and build_source_view
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


def build_source_view(table_name: str, df_cda_file_loads, df_raw_table_load):
    """
    Persist deduplicated source data to ADLS Delta at {ADLS_BALANCE_PATH}{table_name}/
    for consumption by notebook 10 (Final Reconciliation).

    global_temp views are not used because this workspace has Hive Metastore legacy
    access disabled (UC-only mode). Delta files on ADLS are the cross-notebook bridge.

    If the table was loaded this cycle (present in RAW_TABLE_LOAD):
      - Reads raw parquet from ADLS for the cycle's GW_TIMESTAMP range.
      - Extracts GW_FINGERPRINT and GW_TIMESTAMP from the file path.
      - Deduplicates by id: latest event per id wins.
      - Overwrites {ADLS_BALANCE_PATH}{table_name}/ with the deduped Delta.

    If not loaded this cycle:
      - The existing Delta at {ADLS_BALANCE_PATH}{table_name}/ persists from the
        prior cycle. Notebook 10 reads from that path directly — no action needed here.
      - If no Delta exists yet (first run), logs a warning. Notebook 10 will produce
        zero source metrics for this table.
    """
    df_table_check = df_raw_table_load.filter(col("TABLE_NAME") == table_name.upper())

    if df_table_check.count() > 0:
        # Table was loaded this cycle - derive GW_TIMESTAMP range from RAW_TABLE_LOAD
        min_TS = df_table_check.agg({"GW_TIMESTAMP": "min"}).collect()[0][0]
        max_TS = df_table_check.agg({"GW_TIMESTAMP": "max"}).collect()[0][0]

        # Filter CDA_FILE_LOADS to the pending timestamp range for this table
        path_df = (
            df_cda_file_loads
            .filter(
                (col("TABLE_NAME") == table_name) &
                (col("GW_TIMESTAMP") >= min_TS) &
                (col("GW_TIMESTAMP") <= max_TS)
            )
            .select("GW_FINGERPRINT", "GW_TIMESTAMP")
            .toPandas()
        )

        adls_paths = [
            f"{ADLS_RAW_PATH}/{TYPE_DATA}/{table_name}/{row['GW_FINGERPRINT']}/{row['GW_TIMESTAMP']}/"
            for _, row in path_df.iterrows()
        ]

        if adls_paths:
            df = (
                spark.read
                .option("inferSchema", True)
                .option("mergeSchema", True)
                .parquet(*adls_paths)
            )
            # Extract GW_FINGERPRINT and GW_TIMESTAMP from the file path.
            # Path: .../raw/{TYPE_DATA}/{table}/{fingerprint}/{timestamp}/file.parquet
            # reverse split on "/" gives: index 0=filename, 1=timestamp, 2=fingerprint
            df = (
                df
                .withColumn("filepath",       input_file_name())
                .withColumn("GW_FINGERPRINT", reverse(split(col("filepath"), "/"))[2])
                .withColumn("GW_TIMESTAMP",   reverse(split(col("filepath"), "/"))[1])
                .drop("filepath")
            )
            w = Window.partitionBy("id").orderBy(
                col("GW_TIMESTAMP").desc(),
                col("gwcbi___payload_ts_ms").desc(),
                col("gwcbi___seqval_hex").desc()
            )
            df_deduped = (
                df.withColumn("rnk", row_number().over(w))
                .filter(col("rnk") == 1)
                .drop("rnk")
            )
            df_deduped.write.format("delta").mode("overwrite").save(f"{ADLS_BALANCE_PATH}{table_name}/")
            print(f"{table_name}: {df_deduped.count()} rows written to {ADLS_BALANCE_PATH}{table_name}/")
        else:
            print(f"{table_name}: in RAW_TABLE_LOAD but no parquet paths found - balance_recon not updated")

    else:
        # Table not loaded this cycle - existing Delta at ADLS_BALANCE_PATH persists
        # from the prior cycle. Notebook 10 will read from that path directly.
        try:
            row_count = spark.read.format("delta").load(f"{ADLS_BALANCE_PATH}{table_name}/").count()
            print(f"{table_name} not loaded this cycle - fallback Delta has {row_count} rows at {ADLS_BALANCE_PATH}{table_name}/")
        except Exception:
            print(f"{table_name} not loaded this cycle - no balance_recon Delta exists yet (first run?); notebook 10 will produce zero source metrics")

# COMMAND ----------

# DBTITLE 1,Spark Configuration
# Adaptive query execution: Spark optimises join strategies at runtime -
# important for variable-size parquet reads across multiple timestamp folders.
spark.sql("SET spark.sql.adaptive.enabled = true")

# COMMAND ----------

# DBTITLE 1,Load SQL Metadata - RAW_TABLE_LOAD and CDA_FILE_LOADS
# RAW_TABLE_LOAD: one row per parquet folder successfully copied to ADLS raw/ this cycle.
# Used to determine which tables were loaded and their GW_TIMESTAMP range.
df_raw_table_load = get_metadata(
    f"SELECT TABLE_NAME, GW_FINGERPRINT, GW_TIMESTAMP "
    f"FROM {asql_schema}.RAW_TABLE_LOAD "
    f"WHERE XCENTER = '{xcenter}'",
    is_query=True
)
df_raw_table_load.cache()

# CDA_FILE_LOADS: one row per parquet file discovered in client_data/.
# Used to build ADLS raw/ paths for reading parquet in build_source_view().
df_cda_file_loads = get_metadata(
    f"SELECT TABLE_NAME, GW_FINGERPRINT, GW_TIMESTAMP "
    f"FROM {asql_schema}.CDA_FILE_LOADS "
    f"WHERE XCENTER = '{xcenter}'",
    is_query=True
)
df_cda_file_loads.cache()

print(f"RAW_TABLE_LOAD rows : {df_raw_table_load.count()}")
print(f"CDA_FILE_LOADS rows : {df_cda_file_loads.count()}")

# COMMAND ----------

# DBTITLE 1,cc_policy - Build Source View
build_source_view("cc_policy", df_cda_file_loads, df_raw_table_load)

# COMMAND ----------

# DBTITLE 1,cc_exposure - Build Source View
build_source_view("cc_exposure", df_cda_file_loads, df_raw_table_load)

# COMMAND ----------

# DBTITLE 1,cc_contact - Build Source View
build_source_view("cc_contact", df_cda_file_loads, df_raw_table_load)

# COMMAND ----------

# DBTITLE 1,Exit
# Signal success to ADF. These notebooks write source data to ADLS Delta for
# notebook 10 to consume. A successful exit (no exception + STATUS='S') is
# sufficient for ADF to proceed.
result = json.dumps({"STATUS": "S"})
print(result)
dbutils.notebook.exit(result)