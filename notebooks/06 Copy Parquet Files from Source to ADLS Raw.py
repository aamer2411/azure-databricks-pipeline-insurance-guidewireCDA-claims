# Databricks notebook source
# MAGIC %md
# MAGIC ## 06 Copy Parquet Files from Source to ADLS Raw
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Source to ADLS File Copy
# MAGIC
# MAGIC **Purpose:** For a single table, reads the pending file list from `CDA_FILE_LOADS`,
# MAGIC copies each parquet folder from the source (`client_data/`) to ADLS `raw/` in parallel,
# MAGIC runs a schema datatype check against the replica Delta table, then writes the processed
# MAGIC file records to `RAW_TABLE_LOAD` and exits with job status for ADF.
# MAGIC
# MAGIC This notebook runs once per table inside the `Process Each Table` ForEach activity.
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | Source | AWS S3 (`s3://bucket/path`) | ADLS `client_data/` (simulated S3) |
# MAGIC | Source auth | S3 access key + secret from Key Vault | ADLS storage key (hardcoded) |
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Schema param | `config_schema` widget | `ASQL_SCHEMA` widget — same purpose |
# MAGIC | Source path param | `BUCKET` + `ext_s3_path` | `ADLS_CLIENT_DATA_PATH` widget |

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters
import pandas as pd
import numpy as np
import json
import concurrent.futures
import multiprocessing
from pyspark.sql.functions import col, hex
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# ADF passes these parameters to the notebook at runtime.
# TABLE_NAME, START_TIMESTAMP, END_TIMESTAMP come from each ForEach iteration (TABLES_TO_LOAD row).
# All other parameters are pipeline-level parameters.
#
# Parameters to create in ADF:
#   Name                   Type     Source
#   -----------------------------------------------------------------------
#   TABLE_NAME             String   @item().TABLE_NAME           (ForEach item)
#   START_TIMESTAMP        String   @item().START_TIMESTAMP      (ForEach item)
#   END_TIMESTAMP          String   @item().END_TIMESTAMP        (ForEach item)
#   TYPE_DATA              String   pipeline parameter  e.g. claims
#   XCENTER                String   pipeline parameter  e.g. CC
#   ASQL_SCHEMA            String   pipeline parameter  e.g. abc
#   ADLS_CLIENT_DATA_PATH  String   pipeline parameter  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/client_data
#   ADLS_RAW_PATH          String   pipeline parameter  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/raw
#   CYC_RUN_SK             String   output of PROC_UPDATE_CYC_START Lookup activity
#   Truncate               String   output of notebook 05 (Y/N)
#   Force_Truncate         String   output of notebook 05 (Y/N)
#   Processed_DB           String   pipeline parameter  e.g. insurance_claims_domain
#   Processed_Schema       String   pipeline parameter  e.g. cda_replica
#   ADLS_REPLICA_PATH      String   pipeline parameter  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/replica
#   -----------------------------------------------------------------------

dbutils.widgets.text("TABLE_NAME", "")
TABLE_NAME       = str(dbutils.widgets.get("TABLE_NAME"))
TABLE_NAME_lower = TABLE_NAME.lower()
TABLE_NAME_upper = TABLE_NAME.upper()

dbutils.widgets.text("START_TIMESTAMP", "")
START_TIMESTAMP = dbutils.widgets.get("START_TIMESTAMP")

dbutils.widgets.text("END_TIMESTAMP", "")
END_TIMESTAMP = dbutils.widgets.get("END_TIMESTAMP")

dbutils.widgets.text("TYPE_DATA", "")
TYPE_DATA = dbutils.widgets.get("TYPE_DATA")

dbutils.widgets.text("XCENTER", "")
xcenter = str(dbutils.widgets.get("XCENTER"))

dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

# ADLS_CLIENT_DATA_PATH: root path of the simulated S3 source (client_data/ folder).
# Replaces BUCKET + ext_s3_path (S3 credentials) from production.
dbutils.widgets.text("ADLS_CLIENT_DATA_PATH", "")
ADLS_CLIENT_DATA_PATH = dbutils.widgets.get("ADLS_CLIENT_DATA_PATH")

dbutils.widgets.text("ADLS_RAW_PATH", "")
ADLS_RAW_PATH = dbutils.widgets.get("ADLS_RAW_PATH")

dbutils.widgets.text("CYC_RUN_SK", "")
cyc_run_sk = dbutils.widgets.get("CYC_RUN_SK")

dbutils.widgets.text("Truncate", "")
Truncate = dbutils.widgets.get("Truncate")

dbutils.widgets.text("Force_Truncate", "")
Force_Truncate = dbutils.widgets.get("Force_Truncate")

dbutils.widgets.text("Processed_DB", "")
Processed_DB = dbutils.widgets.get("Processed_DB")

dbutils.widgets.text("Processed_Schema", "")
Processed_Schema = dbutils.widgets.get("Processed_Schema")

dbutils.widgets.text("ADLS_REPLICA_PATH", "")
ADLS_REPLICA_PATH = dbutils.widgets.get("ADLS_REPLICA_PATH")

table = TABLE_NAME.lower()

# Initialise status tracking variables — updated during copy and used in exit JSON
status       = ''
error_msg    = ''
DATA_READ    = 'NULL'
DATA_WRITTEN = 'NULL'

print(f"TABLE_NAME      : {TABLE_NAME_lower}")
print(f"START_TIMESTAMP : {START_TIMESTAMP}")
print(f"END_TIMESTAMP   : {END_TIMESTAMP}")
print(f"TYPE_DATA       : {TYPE_DATA}")
print(f"XCENTER         : {xcenter}")
print(f"Truncate        : {Truncate}")
print(f"Force_Truncate  : {Force_Truncate}")

# COMMAND ----------

# DBTITLE 1,Configure ADLS Storage Key and Azure SQL Connection
# Production approach:
#   - S3 access key + secret fetched from Key Vault for source access
#   - Azure SQL credentials fetched from Key Vault
#
# This project:
#   - ADLS storage key hardcoded — used for both client_data/ (source) and raw/ (target)
#   - Azure SQL credentials hardcoded

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


def recursiveDirSize(path):
    """Return total size in bytes of all files directly under the given path."""
    total = 0
    for file in dbutils.fs.ls(path):
        total += file.size
    return total

# COMMAND ----------

# DBTITLE 1,Spark Configuration
# Enable adaptive query execution for better join and aggregation performance.
spark.sql("SET spark.sql.adaptive.enabled = true")

# Allow Delta to automatically merge schema changes from source into existing tables.
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# COMMAND ----------

# DBTITLE 1,Load Pending Files from CDA_FILE_LOADS
# Read pending files for this table from CDA_FILE_LOADS.
# Filters: TABLE_NAME match, IS_LOADED=0 (not yet copied), XCENTER match,
#          GW_TIMESTAMP within START_TIMESTAMP to END_TIMESTAMP range.
# Results are ordered by GW_TIMESTAMP so files are copied in chronological order.
# READ UNCOMMITTED isolation avoids blocking on the Azure SQL side.

connection = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(url_Asql)
statement  = connection.createStatement()
statement.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

pending_files_query = (
    f"(SELECT FILE_LOAD_ID, UPPER(TABLE_NAME) AS TABLE_NAME, GW_FINGERPRINT, "
    f"GW_TIMESTAMP, CLOUD_PATH, IS_LOADED, XCENTER "
    f"FROM {asql_schema}.CDA_FILE_LOADS "
    f"WHERE UPPER(TABLE_NAME) = '{TABLE_NAME_upper}' "
    f"AND IS_LOADED = 0 "
    f"AND XCENTER = '{xcenter}' "
    f"AND GW_TIMESTAMP >= {START_TIMESTAMP} "
    f"AND GW_TIMESTAMP <= {END_TIMESTAMP}) pending_files"
)

RAW_TABLE_LOAD_df = (
    spark.read.jdbc(url_Asql, table=pending_files_query)
    .orderBy("GW_TIMESTAMP")
)

statement.close()
connection.close()

# Build 2D array of (fingerprint, timestamp) pairs — used for path construction below
ts_2d_array = np.array(RAW_TABLE_LOAD_df.select("GW_FINGERPRINT", "GW_TIMESTAMP").collect())
print(f"Pending files for {TABLE_NAME_lower}: {len(ts_2d_array)} folders to process")
print(ts_2d_array)

# COMMAND ----------

# DBTITLE 1,Copy Files from Source to ADLS Raw
# Calculate DATA_READ: total bytes at source before copy.
data_read_byte_sum = 0
for fingerprint, timestamp in ts_2d_array:
    src_path = f"{ADLS_CLIENT_DATA_PATH}/{TABLE_NAME_lower}/{fingerprint}/{timestamp}"
    data_read_byte_sum += recursiveDirSize(src_path)
print(f"DATA_READ (bytes at source): {data_read_byte_sum}")



def copy_data(items, TABLE_NAME):
    """
    Copy a batch of (fingerprint, timestamp) folders from client_data/ to raw/.
    Returns total bytes written to ADLS raw/. Returns 0 on error.
    """
    try:
        byte_sum_batch = 0
        for fingerprint, timestamp in items:
            src_path = f"{ADLS_CLIENT_DATA_PATH}/{TABLE_NAME.lower()}/{fingerprint}/{timestamp}"
            tgt_path = f"{ADLS_RAW_PATH}/{TYPE_DATA}/{TABLE_NAME.lower()}/{fingerprint}/{timestamp}/"
            dbutils.fs.cp(src_path, tgt_path, True)
            byte_sum_batch += recursiveDirSize(tgt_path)
        return byte_sum_batch
    except Exception as e:
        global status, error_msg, DATA_READ, DATA_WRITTEN
        status       = 'F'
        DATA_READ    = 'NULL'
        DATA_WRITTEN = 'NULL'
        error_msg    = "Error occurred while copying data from source to ADLS raw"
        print(e)
        return 0



# Split into batches of 100 and copy in parallel using all available CPU cores
batch_size          = 100
ts_2d_array_batches = [ts_2d_array[i:i + batch_size] for i in range(0, len(ts_2d_array), batch_size)]
num_cores           = multiprocessing.cpu_count()


with concurrent.futures.ThreadPoolExecutor(max_workers=num_cores) as executor:
    results = [executor.submit(copy_data, batch, TABLE_NAME) for batch in ts_2d_array_batches]


byte_sum = sum(result.result() for result in concurrent.futures.as_completed(results))
print(f"DATA_WRITTEN (bytes at target): {byte_sum}")

# COMMAND ----------

# DBTITLE 1,Schema Datatype Check Against Replica
# If the replica Delta table already exists, compare source parquet dtypes against
# replica dtypes. Cast any mismatched replica columns to match the source schema.
# This prevents silent type drift between the Source to ADLS and Raw to Replica steps.

table_load_metadata_df = get_metadata(
    f"SELECT TABLE_NAME, LATEST_LASTSUCCESSFULWRITETIMESTAMP, LATEST_FINGERPRINT "
    f"FROM {asql_schema}.TABLE_LOAD_METADATA "
    f"WHERE XCENTER = '{xcenter}' AND TABLE_NAME = '{table}'",
    is_query=True
)

if spark.catalog.tableExists(f"{Processed_DB}.{Processed_Schema}.{table}"):
    print(f"Datatype check started for {table}")

    latest_timestamp   = table_load_metadata_df.collect()[0]['LATEST_LASTSUCCESSFULWRITETIMESTAMP']
    latest_fingerprint = table_load_metadata_df.collect()[0]['LATEST_FINGERPRINT']
    print(f"Latest fingerprint/timestamp: {latest_fingerprint}/{latest_timestamp}")

    # Read source parquet at the latest fingerprint/timestamp to get current schema
    src_parquet_path = f"{ADLS_CLIENT_DATA_PATH}/{table}/{latest_fingerprint}/{latest_timestamp}/*.parquet"
    df_raw = spark.read.format("parquet").option("inferSchema", "true").load(src_parquet_path)

    # Handle spatial columns — convert binary WKB struct to hex string
    for spatial_col in ['spatialpoint', 'losslocationspatialdenorm', 'spatialpointdenorm']:
        if spatial_col in df_raw.columns:
            df_raw = df_raw.withColumn(spatial_col, hex(col(f"{spatial_col}.wkb")))

    # Compare source and replica dtypes; collect mismatched columns
    delta_table    = DeltaTable.forPath(spark, f"{ADLS_REPLICA_PATH}/{TYPE_DATA}/{table}")
    source_dtypes  = dict(df_raw.dtypes)
    replica_dtypes = dict(delta_table.toDF().dtypes)
    common_columns = set(source_dtypes.keys()).intersection(replica_dtypes.keys())

    mismatches = {
        c: source_dtypes[c]
        for c in common_columns
        if source_dtypes[c] != replica_dtypes[c]
    }

    if mismatches:
        print(f"Datatype changes detected for {table}: {mismatches}")
        replica_df_fixed = delta_table.toDF().withColumns(
            {c: col(c).cast(t) for c, t in mismatches.items()}
        )
        replica_df_fixed.write.format("delta").mode("overwrite") \
            .options(header=True, encoding="UTF-8") \
            .option("overwriteSchema", True) \
            .saveAsTable(f"{Processed_DB}.{Processed_Schema}.{table}")
    else:
        print(f"No datatype changes found for {table}")
else:
    print(f"{table} not yet created in replica — skipping datatype check")

# COMMAND ----------

# DBTITLE 1,Set Status and Write to RAW_TABLE_LOAD
DATA_READ    = data_read_byte_sum
DATA_WRITTEN = byte_sum

# Determine job status based on bytes read vs written
if DATA_READ == DATA_WRITTEN and status != 'F':
    status    = 'S'
    error_msg = "null"
elif (DATA_READ == 'NULL' and DATA_WRITTEN == 'NULL') or (DATA_READ == 0 and DATA_WRITTEN == 0):
    status = 'F'
else:
    status = 'F'

print(f"DATA_READ    : {DATA_READ}")
print(f"DATA_WRITTEN : {DATA_WRITTEN}")
print(f"STATUS       : {status}")
print(f"ERROR_MSG    : {error_msg}")

# Append pending file rows to RAW_TABLE_LOAD — mirrors production pattern exactly.
# RAW_TABLE_LOAD_df is the CDA_FILE_LOADS slice read at the start of this notebook
# (same columns: TABLE_NAME, GW_FINGERPRINT, GW_TIMESTAMP, CLOUD_PATH, IS_LOADED, XCENTER).
# FILE_LOAD_ID is excluded because it is an IDENTITY column in our pilot DDL.
# Only written on fresh/forced runs (Truncate=Y or Force_Truncate=Y) — on restarts
# the rows already exist from the previous attempt so we skip to avoid duplicates.
if Truncate == 'Y' or Force_Truncate == 'Y':
    print('writing into RAW_TABLE_LOAD')
    (
        RAW_TABLE_LOAD_df
        .select("TABLE_NAME", "GW_FINGERPRINT", "GW_TIMESTAMP", "CLOUD_PATH", "IS_LOADED", "XCENTER")
        .write.jdbc(url_Asql, table=f"{asql_schema}.RAW_TABLE_LOAD", mode="append")
    )
    print(f"RAW_TABLE_LOAD: {len(ts_2d_array)} row(s) written for {TABLE_NAME_upper}")

# COMMAND ----------

# DBTITLE 1,Exit Notebook
# Return END_TIMESTAMP, STATUS, DATA_READ, DATA_WRITTEN to ADF.
# ADF uses STATUS to determine success or failure and passes DATA_READ / DATA_WRITTEN
# to PROC_UPDATE_JOB_END to record row counts for this table's job run.
result = json.dumps({
    "END_TIMESTAMP": END_TIMESTAMP,
    "STATUS":        status,
    "DATA_READ":     DATA_READ,
    "DATA_WRITTEN":  DATA_WRITTEN
})
print(result)
dbutils.notebook.exit(result)