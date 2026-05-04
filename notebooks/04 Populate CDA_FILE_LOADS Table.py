# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 Populate CDA_FILE_LOADS
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Manifest Step:** 4 of 4
# MAGIC
# MAGIC **Purpose:** Read the file list staged in `global_temp.TEMP_FILE_INFO_TABLE` by notebook 03,
# MAGIC join it against `TABLE_LOAD_METADATA` and `CDA_FILE_LOADS`, and insert only new entries
# MAGIC into `abc.CDA_FILE_LOADS` with `IS_LOADED=0` (pending copy to raw).
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | Azure SQL Auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | LOAD_STATUS filter | `TABLE_LOAD_METADATA.LOAD_STATUS = 'I'` | Same -- set to `'I'` by `GET_GW_FILE_METADATA_MASTER` in notebook 02 |
# MAGIC | Dedup protection | LEFT JOIN CDA_FILE_LOADS + WHERE FILE_LOAD_ID IS NULL | Same -- safe to rerun |
# MAGIC | IS_LOADED default | 0 (JDBC append, default from DDL) | Same |
# MAGIC | IS_FINGERPRINT_UPDATED | Not set in this notebook -- defaults to 0 | Same |

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters
# ADF passes these parameters to the notebook at runtime.
# Production approach: identical widget pattern, same parameter names.
#
# Parameters to create in the ADF pipeline:
#   Name            Type     Example value
#   -------------------------------------------------------------------------------------
#   XCENTER         String   CC
#   CYC_SK          String   101
#   ASQL_SCHEMA     String   abc
#   -------------------------------------------------------------------------------------

# XCENTER identifies the source system (e.g. CC = ClaimCenter, CM = ContactManager).
# Used to scope the Azure SQL queries and tag CDA_FILE_LOADS rows.
# In production the same notebook runs for both CC and CM -- ADF passes the correct value each time.
# In this project we only use CC.
dbutils.widgets.text("XCENTER", "")
xcenter = dbutils.widgets.get("XCENTER")

# CYC_SK identifies the cycle. Used to look up CYC_CUR_RUN_SK which is written
# directly into every CDA_FILE_LOADS row as CYC_RUN_SK.
dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

# ASQL_SCHEMA is the Azure SQL schema name for all ABC metadata tables (e.g. abc).
dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

print(f"XCENTER     : {xcenter}")
print(f"CYC_SK      : {CYC_SK}")
print(f"ASQL_SCHEMA : {asql_schema}")

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
# CYC_CUR_RUN_SK is used here -- it is written directly into every CDA_FILE_LOADS row
# as CYC_RUN_SK so each file can be traced back to the pipeline run that discovered it.

result_df = get_metadata(
    f"SELECT CYC_CUR_RUN_SK FROM {asql_schema}.CYC_CTRL_TBL WHERE CYC_SK = {CYC_SK}",
    is_query=True
)

if result_df.count() > 0:
    cyc_run_sk = result_df.first()[0]
    print(f"cyc_run_sk: {cyc_run_sk}")

# COMMAND ----------

# DBTITLE 1,Load Reference Data from Azure SQL
# Read TABLE_LOAD_METADATA and CDA_FILE_LOADS into Spark temp views.
# These are joined against TEMP_FILE_INFO_TABLE in the next cell to produce
# only the rows that need to be inserted into CDA_FILE_LOADS.

df_1 = get_metadata(f"{asql_schema}.TABLE_LOAD_METADATA", is_query=False)
df_2 = get_metadata(f"{asql_schema}.CDA_FILE_LOADS", is_query=False)

df_1.createOrReplaceTempView("TABLE_LOAD_METADATA")
df_2.createOrReplaceTempView("CDA_FILE_LOADS")

print(f"TABLE_LOAD_METADATA rows : {df_1.count()}")
print(f"CDA_FILE_LOADS rows      : {df_2.count()}")

# COMMAND ----------

# DBTITLE 1,Populate CDA_FILE_LOADS
# Join TEMP_FILE_INFO_TABLE (staged by notebook 03) against TABLE_LOAD_METADATA and CDA_FILE_LOADS
# to produce only the rows that should be inserted.
#
# INNER JOIN TABLE_LOAD_METADATA WHERE LOAD_STATUS = 'I':
#   Only inserts rows for tables that have new data detected this run.
#   LOAD_STATUS is set to 'I' by GET_GW_FILE_METADATA_MASTER (notebook 02).
#   It is reset to 'C' by the Src->Raw notebook after the files are copied.
#
# LEFT JOIN CDA_FILE_LOADS + WHERE FILE_LOAD_ID IS NULL (anti-join):
#   Skips any file that already exists in CDA_FILE_LOADS (same TABLE_NAME + GW_TIMESTAMP + XCENTER).
#   This makes the notebook safe to rerun -- no duplicate rows are inserted.
#
# CYC_RUN_SK is embedded directly in the SELECT to tag each row with the current run.

query = f"""
    SELECT Src.TABLE_NAME,
           Src.GW_FINGERPRINT,
           Src.GW_TIMESTAMP,
           Src.CLOUD_PATH,
           Src.XCENTER,
           {cyc_run_sk} AS CYC_RUN_SK
    FROM   global_temp.TEMP_FILE_INFO_TABLE Src
    INNER JOIN TABLE_LOAD_METADATA Tbls
        ON  UPPER(Src.TABLE_NAME) = UPPER(Tbls.TABLE_NAME)
        AND Src.XCENTER = Tbls.XCENTER
        AND Tbls.LOAD_STATUS = 'I'
    LEFT JOIN CDA_FILE_LOADS Tgt
        ON  Src.TABLE_NAME  = Tgt.TABLE_NAME
        AND Src.GW_TIMESTAMP = Tgt.GW_TIMESTAMP
        AND Src.XCENTER     = Tgt.XCENTER
    WHERE Tgt.FILE_LOAD_ID IS NULL
    ORDER BY Src.TABLE_NAME, Src.GW_TIMESTAMP
"""

df3 = spark.sql(query)
print(f"Rows to insert into CDA_FILE_LOADS: {df3.count()}")

df3.write.jdbc(url_Asql, table=f"{asql_schema}.CDA_FILE_LOADS", mode="append")
print(f"CDA_FILE_LOADS populated for XCENTER={xcenter}, CYC_RUN_SK={cyc_run_sk}")