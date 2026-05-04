# Databricks notebook source
# MAGIC %md
# MAGIC ## 11 Replica Tables Data Validation
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Raw to Replica (Data Validation)
# MAGIC
# MAGIC **Purpose:** Executes a configurable SQL validation rule (stored in `abc.VALIDATION_CTRL_TBL`)
# MAGIC against a replica table. Rows that fail the rule are flagged (`Validation_Flag='F'`) in the
# MAGIC replica table. Failed record PKs are written to `abc.VALIDATION_ERROR_LOG`. Exits with
# MAGIC `STATUS=S` (no failures) or `STATUS=F` (any failures) for ADF to evaluate.
# MAGIC
# MAGIC Called once per validation rule by an ADF ForEach activity. ADF passes the `val_sk` for the
# MAGIC rule to execute.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Schema param | `config_schema` widget | `config_schema` widget — same name kept |
# MAGIC | PROC_VALIDATION_START/END | Commented out in source — not active | Not implemented — omitted |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports and Pipeline Parameters
import json
from pyspark.sql.functions import concat_ws, collect_list, lit, current_timestamp

# ADF passes these parameters at runtime via a ForEach activity (one per validation rule).
# Parameters to create in ADF:
#   Name           Type    Source
#   ---------------------------------------------------------------
#   val_sk         String  @item().VAL_SK from VALIDATION_CTRL_TBL
#   xcenter        String  pipeline parameter  e.g. CC
#   config_schema  String  pipeline parameter  e.g. abc
#   val_rule       String  @item().VAL_RULE     (rule description)
#   table_name     String  @item().TABLE_NAME   (replica table to validate)
#   primary_key    String  @item().PRIMARY_KEY  (PK column for error logging)
#   catalog_name   String  pipeline parameter  e.g. insurance_claims_domain
#   schema_name    String  pipeline parameter  e.g. replica
#   cyc_run_sk     String  output of PROC_UPDATE_CYC_START
#   val_run_sk     String  @item().VAL_RUN_SK
#   ---------------------------------------------------------------

dbutils.widgets.text("val_sk", "")
val_sk = dbutils.widgets.get("val_sk")

dbutils.widgets.text("xcenter", "")
xcenter = dbutils.widgets.get("xcenter")

dbutils.widgets.text("config_schema", "")
config_schema = dbutils.widgets.get("config_schema")

dbutils.widgets.text("val_rule", "")
val_rule = dbutils.widgets.get("val_rule")

dbutils.widgets.text("table_name", "")
table_name = dbutils.widgets.get("table_name")

dbutils.widgets.text("primary_key", "")
primary_key = dbutils.widgets.get("primary_key")

dbutils.widgets.text("catalog_name", "")
catalog_name = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")

dbutils.widgets.text("cyc_run_sk", "")
cyc_run_sk = dbutils.widgets.get("cyc_run_sk")

dbutils.widgets.text("val_run_sk", "")
val_run_sk = dbutils.widgets.get("val_run_sk")

print(f"val_sk        : {val_sk}")
print(f"xcenter       : {xcenter}")
print(f"config_schema : {config_schema}")
print(f"val_rule      : {val_rule}")
print(f"table_name    : {table_name}")
print(f"primary_key   : {primary_key}")
print(f"catalog_name  : {catalog_name}")
print(f"schema_name   : {schema_name}")
print(f"cyc_run_sk    : {cyc_run_sk}")
print(f"val_run_sk    : {val_run_sk}")


# COMMAND ----------

# DBTITLE 1,Set Default Catalog and Schema
# Set default catalog and schema so validation queries in VALIDATION_CTRL_TBL
# can reference replica tables without fully qualifying the catalog prefix.
spark.sql(f"USE {catalog_name}.{schema_name}")


# COMMAND ----------

# DBTITLE 1,Configure Azure SQL Connection
# Production approach:
#   Server, database name, username, and password fetched from Azure Key Vault
#   via dbutils.secrets.get("testsecretkv2", <secret-name>).
#
# This project:
#   Values are hardcoded - no Key Vault configured for this practice environment.

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

# DBTITLE 1,Helper Function - get_metadata
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

# DBTITLE 1,Fetch Validation Rule from VALIDATION_CTRL_TBL
# Fetch the validation rule row for this val_sk + xcenter.
# VAL_QUERY contains the SQL to execute against the replica table —
# it should return PKs of rows that fail the validation.
df_val_rule = get_metadata(
    f"SELECT * FROM {config_schema}.VALIDATION_CTRL_TBL "
    f"WHERE VAL_SK = {val_sk} AND XCENTER = '{xcenter}'",
    is_query=True
)
df_val_rule.createOrReplaceTempView("val_rule")

val_query = spark.sql("SELECT VAL_QUERY FROM val_rule").collect()[0][0]
print(f"Validation query: {val_query}")


# COMMAND ----------

# DBTITLE 1,Run Validation Query
# Run the validation SQL from VALIDATION_CTRL_TBL.
# The query returns rows (identified by primary key) that fail the rule.
# An empty result means all rows passed.
df_failed = spark.sql(val_query)
df_failed.createOrReplaceTempView("df_failed")

print(f"Failed rows returned: {df_failed.count()}")


# COMMAND ----------

# DBTITLE 1,Flag Failed Rows in Replica Table
# Set Validation_Flag='F' on replica rows whose PKs appear in the validation result.
# Only rows written in the current cycle (cyc_run_sk) are updated.
spark.sql(f"""
    UPDATE {catalog_name}.{schema_name}.{table_name}
    SET Validation_Flag = 'F'
    WHERE {primary_key} IN (SELECT {primary_key} FROM df_failed)
      AND cyc_run_sk = {cyc_run_sk}
""")

print(f"Validation_Flag set to F for failed rows in {catalog_name}.{schema_name}.{table_name}")


# COMMAND ----------

# DBTITLE 1,Compute Validation Counts
total_count  = spark.sql(f"SELECT count(*) FROM {catalog_name}.{schema_name}.{table_name} WHERE cyc_run_sk = {cyc_run_sk}").collect()[0][0]
failed_count = spark.sql(f"SELECT count(*) FROM {catalog_name}.{schema_name}.{table_name} WHERE cyc_run_sk = {cyc_run_sk} AND Validation_Flag = 'F'").collect()[0][0]
passed_count = total_count - failed_count

print(f"total_count  : {total_count}")
print(f"failed_count : {failed_count}")
print(f"passed_count : {passed_count}")


# COMMAND ----------

# DBTITLE 1,Write Failed PKs to VALIDATION_ERROR_LOG
# If any rows failed, write a single summary row to abc.VALIDATION_ERROR_LOG.
# GW_ERROR_VALUES holds the comma-separated PKs of all failed rows.
# Only runs when there are failures — skipped entirely on a clean validation pass.
if failed_count > 0:
    error_df = (
        df_failed
        .agg(concat_ws(',', collect_list('publicid')).alias('GW_ERROR_VALUES'))
        .withColumn("GW_TABLE_NAME",  lit(table_name))
        .withColumn("GW_PRIMARY_KEY", lit(primary_key))
        .withColumn("INSERTED_DATE",  lit(current_timestamp()))
        .withColumn("VAL_SK",         lit(val_sk))
        .withColumn("VAL_RUN_SK",     lit(val_run_sk))
    )
    error_df.write.jdbc(url=url_Asql, table=f"{config_schema}.VALIDATION_ERROR_LOG", mode="append")
    print(f"Error log written: {failed_count} failed PKs logged to {config_schema}.VALIDATION_ERROR_LOG")
else:
    print("No failures — VALIDATION_ERROR_LOG not written")


# COMMAND ----------

# DBTITLE 1,Exit
# STATUS=S if no rows failed; STATUS=F if any row failed.
# ADF reads this exit value to determine whether to flag the pipeline as failed.
status = 'S' if failed_count == 0 else 'F'

print(f"STATUS: {status}")
dbutils.notebook.exit(json.dumps({
    "STATUS":               status,
    "TOTAL_RECORD_COUNT":   total_count,
    "FAILED_RECORD_COUNT":  failed_count,
    "PASSED_RECORD_COUNT":  passed_count
}))
