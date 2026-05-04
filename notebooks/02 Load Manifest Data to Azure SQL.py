# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 Load Manifest Data to Azure SQL
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Manifest Step:** 2 of 4
# MAGIC
# MAGIC **Purpose:** Read manifest.json from ADLS raw path (copied by notebook 01), parse each table entry,
# MAGIC and load into `abc.MANIFEST` in Azure SQL. Then call `GET_GW_FILE_METADATA_MASTER` to update
# MAGIC `abc.TABLE_LOAD_METADATA` with the latest and previous timestamps and fingerprints.
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | Azure SQL Auth | Key Vault secrets (server, user, password) | Hardcoded connection values |
# MAGIC | Schema | Parameterised via `ASQL_SCHEMA` widget | Same -- `abc` passed via widget |
# MAGIC | CDA Stage URL | S3 bucket path (BUCKET + ext_s3_path widgets) | ADLS raw path -- same purpose, different source |
# MAGIC | MANIFEST rows | One row per table per run (813 tables in prod) | Same pattern -- 5 tables |
# MAGIC | TABLE_LOAD_METADATA | Updated by stored proc after MANIFEST load | Same |

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters
# ADF passes these parameters to the notebook at runtime.
# Production approach: identical widget pattern, same parameter names.
#
# Parameters to create in the ADF pipeline:
#   Name            Type     Example value
#   -------------------------------------------------------------------------------------
#   ADLS_RAW_PATH   String   abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/raw
#   TYPE_DATA       String   claims
#   XCENTER         String   CC
#   CYC_SK          String   101
#   ASQL_SCHEMA     String   abc
#   -------------------------------------------------------------------------------------

dbutils.widgets.text("ADLS_RAW_PATH", "")
ADLS_RAW_PATH = dbutils.widgets.get("ADLS_RAW_PATH")

# TYPE_DATA identifies the subfolder within raw path (e.g. claims, contact_manager).
# Production uses the same notebook for multiple data types; we use "claims" only.
dbutils.widgets.text("TYPE_DATA", "")
TYPE_DATA = dbutils.widgets.get("TYPE_DATA")

# XCENTER identifies the source system (e.g. CC = ClaimCenter, CM = ContactManager).
# Used to tag MANIFEST rows and scope the DELETE before re-insert.
# In production the same notebook runs for both CC and CM -- ADF passes the correct value each time.
# In this project we only use CC.
dbutils.widgets.text("XCENTER", "")
xcenter = dbutils.widgets.get("XCENTER")

# CYC_SK identifies the cycle. Used to look up CYC_CUR_RUN_SK from CYC_CTRL_TBL.
dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

# ASQL_SCHEMA is the Azure SQL schema name for all ABC metadata tables.
# Production uses config_schema; this project uses ASQL_SCHEMA -- same purpose.
dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

print(f"ADLS_RAW_PATH : {ADLS_RAW_PATH}")
print(f"TYPE_DATA     : {TYPE_DATA}")
print(f"XCENTER       : {xcenter}")
print(f"CYC_SK        : {CYC_SK}")
print(f"ASQL_SCHEMA   : {asql_schema}")

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
#   Server and database match the practice Azure SQL instance.

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


def run_asql_query(query: str):
    """
    Execute a DML statement or stored proc call (EXECUTE ...) against Azure SQL.
    Opens a JDBC connection, runs the statement, then always closes the connection.
    Used for INSERT / UPDATE / DELETE / EXECUTE -- not for SELECT.
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
    Used for SELECT queries -- not for DML.

    is_query=False: table_or_query is a table name  e.g. "abc.CYC_CTRL_TBL"
                    Spark reads the entire table.
    is_query=True:  table_or_query is a SQL SELECT   e.g. "SELECT CYC_CUR_RUN_SK FROM ..."
                    Spark runs the query and returns only those rows/columns.
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
# CYC_CUR_RUN_SK is the run identifier written by PROC_UPDATE_CYC_START at pipeline start.
# It is NOT a foreign key enforced at the database level -- it is a logical reference
# used to tag MANIFEST rows and TABLE_LOAD_METADATA so records can be traced back
# to the exact pipeline run that produced them.

result_df = get_metadata(
    f"SELECT CYC_CUR_RUN_SK FROM {asql_schema}.CYC_CTRL_TBL WHERE CYC_SK = {CYC_SK}",
    is_query=True
)

if result_df.count() == 0:
    raise ValueError(f"No active cycle found in CYC_CTRL_TBL for CYC_SK={CYC_SK}")

cyc_run_sk = result_df.first()[0]
print(f"CYC_RUN_SK: {cyc_run_sk}")

# COMMAND ----------

# DBTITLE 1,Populate MANIFEST Table
# load_manifest() does the following:
#   1. Deletes existing MANIFEST rows for this XCENTER (so re-runs are safe).
#   2. Reads manifest.json from ADLS -- a nested JSON where each top-level key
#      is a table name and its value is the metadata struct.
#   3. Unpacks each table struct into a flat row, stringifies schemaHistory
#      (stored as a Python dict repr: {'fingerprint': timestamp}).
#   4. Adds XCENTER, AUDIT_DT_TM, CYC_RUN_SK audit columns.
#   5. Writes all rows to abc.MANIFEST via JDBC append.
#
# Production approach: identical logic. No differences in this cell.

def load_manifest():
    # Clear previous rows for this XCENTER before re-inserting
    run_asql_query(f"EXECUTE {asql_schema}.DELETE_MANIFEST_DATA '{xcenter}'")

    # Read manifest.json -- multiline because the JSON spans multiple lines
    manifest_path = f"{ADLS_RAW_PATH}/{TYPE_DATA}/manifest.json"
    df = spark.read.option("multiline", True).format("json").load(manifest_path)

    # Each top-level schema field maps to one table entry in the JSON.
    # Select the nested struct fields, convert to Pandas, stringify schemaHistory.
    rows = []
    for field in df.schema.fields:
        table_df = df.select(f"{field.name}.*").toPandas()
        table_df["schemaHistory"] = table_df["schemaHistory"].astype(str)
        rows.append(table_df)

    combined_pd = pd.concat(rows, axis=0, ignore_index=True)

    # Convert back to Spark and add audit columns
    final_df = (
        spark.createDataFrame(combined_pd)
        .withColumn("XCENTER",     F.lit(xcenter))
        .withColumn("AUDIT_DT_TM", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("CYC_RUN_SK",  F.lit(cyc_run_sk))
    )

    final_df.write.jdbc(url_Asql, table=f"{asql_schema}.MANIFEST", mode="append")
    print(f"MANIFEST loaded: {final_df.count()} rows written for XCENTER={xcenter}")


load_manifest()

# COMMAND ----------

# DBTITLE 1,Update TABLE_LOAD_METADATA
# GET_GW_FILE_METADATA_MASTER reads the freshly loaded abc.MANIFEST rows
# and updates abc.TABLE_LOAD_METADATA for each table:
#   - Shifts LATEST timestamps and fingerprints to PREVIOUS
#   - Writes new LATEST values from MANIFEST
#   - Sets LOAD_STATUS = 'C', records CYC_RUN_SK
#
# CDA_STAGE_URL: in production this is the S3 bucket path (s3://<bucket>/<ext_s3_path>).
# Here we pass the equivalent ADLS raw path. The stored proc accepts it to match
# the production signature but does not use it in its query logic.

CDA_STAGE_URL = f"{ADLS_RAW_PATH}/{TYPE_DATA}"

run_asql_query(
    f"EXECUTE {asql_schema}.GET_GW_FILE_METADATA_MASTER '{CDA_STAGE_URL}','{xcenter}',{cyc_run_sk}"
)

print(f"TABLE_LOAD_METADATA updated for XCENTER={xcenter}, CYC_RUN_SK={cyc_run_sk}")