# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 Detect and List Files to Load
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Manifest Step:** 3 of 4
# MAGIC
# MAGIC **Purpose:** Determine full or incremental load by comparing timestamps in `TABLE_LOAD_METADATA`
# MAGIC against `CDA_FILE_LOADS`. List parquet file paths from ADLS that have not yet been loaded
# MAGIC and stage them in a global temp view for notebook 04 to consume.
# MAGIC No actual data is read or loaded at this stage -- this notebook works purely at the
# MAGIC file path level; parquet contents are not touched until the Src->Raw notebook.
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | File listing | boto3 S3 paginator with `StartAfter` for incremental | `dbutils.fs.ls()` on ADLS `client_data/` |
# MAGIC | Parallel listing | `ThreadPoolExecutor` (10 workers) across tables | Sequential `dbutils.fs.ls()` -- 5 tables, no parallelism needed |
# MAGIC | MANIFEST path stripping | `REPLACE(DATAFILESPATH, CDA_STAGE_URL, '')` | `RIGHT(DATAFILESPATH, CHARINDEX('/', REVERSE(...)))` to extract table name |
# MAGIC | Output | Global temp view `TEMP_FILE_INFO_TABLE` | Same |

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters
# ADF passes these parameters to the notebook at runtime.
# Production approach: identical widget pattern, same parameter names.
#
# Parameters to create in the ADF pipeline:
#   Name                Type     Example value
#   -------------------------------------------------------------------------------------
#   CLIENT_DATA_PATH    String   abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/client_data
#   XCENTER             String   CC
#   CYC_SK              String   101
#   ASQL_SCHEMA         String   abc
#   -------------------------------------------------------------------------------------

# CLIENT_DATA_PATH is the ADLS source zone (our simulated S3).
# Production equivalent: BUCKET + ext_s3_path widgets pointing to the S3 bucket.
dbutils.widgets.text("CLIENT_DATA_PATH", "")
CLIENT_DATA_PATH = dbutils.widgets.get("CLIENT_DATA_PATH")

# XCENTER identifies the source system (e.g. CC = ClaimCenter, CM = ContactManager).
# Used to filter all Azure SQL queries to the correct source system.
# In production the same notebook runs for both CC and CM -- ADF passes the correct value each time.
# In this project we only use CC.
dbutils.widgets.text("XCENTER", "")
xcenter = dbutils.widgets.get("XCENTER")

# CYC_SK identifies the cycle. Used to look up CYC_CUR_RUN_SK from CYC_CTRL_TBL.
dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

# ASQL_SCHEMA is the Azure SQL schema name for all ABC metadata tables (e.g. abc).
dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

print(f"CLIENT_DATA_PATH : {CLIENT_DATA_PATH}")
print(f"XCENTER          : {xcenter}")
print(f"CYC_SK           : {CYC_SK}")
print(f"ASQL_SCHEMA      : {asql_schema}")

# COMMAND ----------

# DBTITLE 1,Configure ADLS Storage Key
# This project only: configure ADLS access so dbutils.fs and Spark can reach abfss:// paths.
# In production, credentials are set at cluster level via Spark config or Key Vault.
# Key retrieved from azurepractice68256 storage account (key1).
spark.conf.set(
    "fs.azure.account.key.azurepractice68256.dfs.core.windows.net",
    "<your-adls-storage-key>"
)
print("ADLS storage key configured.")

# COMMAND ----------

# DBTITLE 1,Azure SQL Connection
# Production approach:
#   Server, database name, username, and password fetched from Azure Key Vault
#   via dbutils.secrets.get("testsecretkv2", <secret-name>).
#
# This project:
#   Values are hardcoded -- no Key Vault configured for this practice environment.

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
from pyspark.sql import functions as F
import pandas as pd


def get_metadata(table_or_query: str, is_query: bool):
    """
    Read data from Azure SQL via Spark JDBC and return a Spark DataFrame.
    is_query=False: table_or_query is a table name -- Spark reads the entire table.
    is_query=True:  table_or_query is a SQL SELECT query -- returns only those rows/columns.
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

# DBTITLE 1,Get Current Cycle Run SK
# Fetch CYC_CUR_RUN_SK from CYC_CTRL_TBL for the current cycle.
# Present to match production -- cyc_run_sk is read here but not written to
# the output temp view (TEMP_FILE_INFO_TABLE). It is used in notebook 04
# when rows are inserted into CDA_FILE_LOADS.

result_df = get_metadata(
    f"SELECT CYC_CUR_RUN_SK FROM {asql_schema}.CYC_CTRL_TBL WHERE CYC_SK = {CYC_SK}",
    is_query=True
)

if result_df.count() > 0:
    cyc_run_sk = result_df.first()[0]
    print(f"cyc_run_sk: {cyc_run_sk}")

# COMMAND ----------

# DBTITLE 1,Load Reference Data from Azure SQL
# Read three tables from Azure SQL and register them as Spark temp views
# so the load-type detection query in the next cell can join across them.

# 1. TABLE_LOAD_METADATA -- latest and previous timestamps per table for this XCENTER
tlm_df = get_metadata(
    f"SELECT TABLE_NAME, LATEST_LASTSUCCESSFULWRITETIMESTAMP, "
    f"PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP, LOAD_STATUS "
    f"FROM {asql_schema}.TABLE_LOAD_METADATA WHERE XCENTER = '{xcenter}'",
    is_query=True
)
tlm_df.createOrReplaceTempView("TABLE_LOAD_METADATA")

# 2. CDA_FILE_LOADS -- all previously discovered files for this XCENTER
# Row count drives the full vs incremental decision:
#   count = 0  -->  Full load  (first ever run, no files tracked yet)
#   count > 0  -->  Incremental load (compare timestamps to find new folders only)
cfl_df = get_metadata(
    f"SELECT * FROM {asql_schema}.CDA_FILE_LOADS WHERE XCENTER = '{xcenter}'",
    is_query=True
)
cfl_count = cfl_df.count()
cfl_df.createOrReplaceTempView("CDA_FILE_LOADS")
print(f"CDA_FILE_LOADS rows for XCENTER={xcenter}: {cfl_count}")
print(f"Load type: {'FULL' if cfl_count == 0 else 'INCREMENTAL'}")

# 3. MANIFEST -- extract table name from DATAFILESPATH and expand schemaHistory
# DATAFILESPATH is stored as 'CC/CC_CLAIM' -- we extract just 'CC_CLAIM' (last segment)
# to match TABLE_LOAD_METADATA.TABLE_NAME.
# Production approach: REPLACE(DATAFILESPATH, CDA_STAGE_URL, '') strips the S3 base path.
# This project: RIGHT(..., CHARINDEX('/', REVERSE(...)) - 1) extracts the last segment.
manifest_df = get_metadata(
    f"SELECT UPPER(RIGHT(DATAFILESPATH, CHARINDEX('/', REVERSE(DATAFILESPATH)) - 1)) AS DATAFILESPATH, "
    f"SCHEMAHISTORY FROM {asql_schema}.MANIFEST WHERE XCENTER = '{xcenter}'",
    is_query=True
)
manifest_df.createOrReplaceTempView("MANIFEST")

# Expand schemaHistory: each entry is a Python dict repr {'fingerprint': first_seen_ts}
# One table can have multiple fingerprints if schema changed over time.
# Production expands this only for incremental load; here we expand always because
# dbutils.fs.ls() needs explicit table/fingerprint paths (unlike boto3 which lists all
# S3 objects under a prefix in one sweep).
manifest_pd = manifest_df.toPandas()
expanded_rows = []
for _, row in manifest_pd.iterrows():
    schema_dict = eval(row["SCHEMAHISTORY"])  # safe here -- value written by our own notebook 02
    for fingerprint, first_seen_ts in schema_dict.items():
        expanded_rows.append({
            "datafilespath": row["DATAFILESPATH"],  # e.g. CC_CLAIM
            "fingerprint":   fingerprint,
            "first_seen_ts": str(first_seen_ts)
        })

manifest_expanded_df = spark.createDataFrame(expanded_rows)
manifest_expanded_df.createOrReplaceTempView("MANIFEST_EXPANDED")
print(f"Manifest entries (table/fingerprint combinations): {manifest_expanded_df.count()}")

# COMMAND ----------

# DBTITLE 1,Detect Load Type and List Files
# Builds file_list: relative paths in format 'table_name/fingerprint/timestamp'
# e.g. 'cc_claim/f0a7604049cecc6facff72cad2e0d6cb/1772378931503'
#
# Full load  (cfl_count == 0):
#   Lists ALL timestamp folders for every table/fingerprint found in MANIFEST.
#   Production: boto3 lists all S3 objects under the bucket prefix.
#   This project: dbutils.fs.ls() on each client_data/{table}/{fingerprint}/ path.
#
# Incremental load (cfl_count > 0):
#   Finds tables where LATEST_LASTSUCCESSFULWRITETIMESTAMP > MAX(GW_TIMESTAMP in CDA_FILE_LOADS)
#   -- meaning new data has arrived since the last load.
#   Lists only timestamp folders newer than PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP.
#   Production: boto3 paginator with StartAfter=previous_timestamp/ skips already-seen folders.
#   This project: compares timestamp folder names (epoch ms strings, fixed 13 digits --
#   string comparison is equivalent to numeric comparison).

file_list = []

if cfl_count == 0:
    print("Full load -- listing all timestamp folders from ADLS")

    for row in manifest_expanded_df.collect():
        table_name  = row["datafilespath"].lower()  # CC_CLAIM -> cc_claim (ADLS folders are lowercase)
        fingerprint = row["fingerprint"]
        base_path   = f"{CLIENT_DATA_PATH}/{table_name}/{fingerprint}"

        try:
            for folder in dbutils.fs.ls(base_path):
                ts = folder.name.rstrip("/")
                file_list.append(f"{table_name}/{fingerprint}/{ts}")
        except Exception as e:
            print(f"  Warning: could not list {base_path} -- {e}")

    print(f"Total timestamp folders found: {len(file_list)}")

else:
    print("Incremental load -- finding tables with new data")

    # Tables with new data: LATEST_TS > MAX(GW_TIMESTAMP already in CDA_FILE_LOADS)
    table_load_metadata_df = spark.sql("""
        SELECT m.datafilespath AS TableName,
               m.fingerprint,
               t.PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP
        FROM   MANIFEST_EXPANDED m
        LEFT JOIN TABLE_LOAD_METADATA t ON m.datafilespath = t.TABLE_NAME
        WHERE  t.TABLE_NAME IN (
            SELECT tlm.TABLE_NAME
            FROM   TABLE_LOAD_METADATA tlm
            LEFT JOIN (
                SELECT UPPER(TABLE_NAME) AS TABLE_NAME,
                       MAX(GW_TIMESTAMP)  AS MAX_TIMESTAMP
                FROM   CDA_FILE_LOADS
                GROUP BY UPPER(TABLE_NAME)
            ) cfl ON UPPER(tlm.TABLE_NAME) = cfl.TABLE_NAME
            WHERE tlm.LATEST_LASTSUCCESSFULWRITETIMESTAMP > COALESCE(cfl.MAX_TIMESTAMP, '000')
        )
    """)
    table_load_metadata_df.createOrReplaceTempView("table_load_metadata_df")
    print(f"Tables with new data: {table_load_metadata_df.count()}")

    for row in table_load_metadata_df.collect():
        table_name   = row["TableName"].lower()
        fingerprint  = row["fingerprint"]
        previous_ts  = str(row["PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP"] or "0")
        base_path    = f"{CLIENT_DATA_PATH}/{table_name}/{fingerprint}"

        try:
            for folder in dbutils.fs.ls(base_path):
                ts = folder.name.rstrip("/")
                # Epoch ms strings are 13 digits -- string comparison equals numeric comparison
                if ts > previous_ts:
                    file_list.append(f"{table_name}/{fingerprint}/{ts}")
        except Exception as e:
            print(f"  Warning: could not list {base_path} -- {e}")

    print(f"New timestamp folders found: {len(file_list)}")

# COMMAND ----------

# DBTITLE 1,Parse File Paths and Stage Temp View
from pyspark.sql.types import StructType, StructField, StringType

# Parse each relative path into structured columns.
# Path format: table_name/fingerprint/timestamp
# e.g.  cc_claim/f0a7604049cecc6facff72cad2e0d6cb/1772378931503
#
# CLOUD_PATH = table_name/fingerprint/timestamp (same as the full path -- no filename).
# Notebook 04 uses CLOUD_PATH to locate the parquet files and insert rows into CDA_FILE_LOADS.

schema = StructType([
    StructField("TABLE_NAME",     StringType(), True),
    StructField("GW_FINGERPRINT", StringType(), True),
    StructField("GW_TIMESTAMP",   StringType(), True),
    StructField("CLOUD_PATH",     StringType(), True)
])

parsed = []
for element in file_list:
    ele_list = element.split("/")
    parsed.append({
        "TABLE_NAME":     ele_list[0],
        "GW_FINGERPRINT": ele_list[1],
        "GW_TIMESTAMP":   ele_list[2],
        "CLOUD_PATH":     ele_list[0] + "/" + ele_list[1] + "/" + ele_list[2]
    })

df1 = spark.createDataFrame(parsed, schema=schema)
df2 = df1.distinct()
df2 = df2.withColumn("XCENTER", F.lit(xcenter))

# Global temp view -- survives across notebook boundaries within the same Spark session.
# Notebook 04 reads this view to insert rows into abc.CDA_FILE_LOADS.
df2.createOrReplaceGlobalTempView("TEMP_FILE_INFO_TABLE")

print(f"TEMP_FILE_INFO_TABLE staged: {df2.count()} rows")
display(df2)