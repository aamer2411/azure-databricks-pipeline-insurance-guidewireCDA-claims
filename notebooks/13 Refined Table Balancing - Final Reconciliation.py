# Databricks notebook source
# MAGIC %md
# MAGIC ## 13 Refined Table Balancing - Final Reconciliation
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Replica to Refined (Balance Recon)
# MAGIC
# MAGIC **Purpose:** Compares key financial metrics between the replica Delta tables and the
# MAGIC refined Delta tables (loaded by the ForEach in the Replica→Refined step). Writes
# MAGIC one row per metric to `abc.BALANCE_METRICS` in Azure SQL and exits with
# MAGIC `Balance_Recon_Status = S` (all passed) or `F` (any failed) for ADF to evaluate.
# MAGIC
# MAGIC Runs after the ForEach refined load (notebook 12) completes for all tables.
# MAGIC
# MAGIC **Metrics computed:**
# MAGIC
# MAGIC | Metric | Replica query | Refined query |
# MAGIC |--------|--------------|---------------|
# MAGIC | ClaimCount | `count(distinct claimnumber)` from `cc_claim` | Same from `claim_detail` where `ETL_ActiveRow_Flag='Y'` |
# MAGIC | LossPayment | `sum(amount)` where `costtype='claimcost'`, `costcategory='LossPayment'` | Same from `claim_financial` |
# MAGIC | ExpensePayment | `sum(amount)` where `costtype IN ('aoexpense','dccexpense')`, `costcategory='ExpensePayment'` | Same |
# MAGIC | LossReserve | `sum(amount)` where `costtype='claimcost'`, `costcategory='LossReserve'` | Same |
# MAGIC | ExpenseReserve | `sum(amount)` where `costtype IN ('aoexpense','dccexpense')`, `costcategory='ExpenseReserve'` | Same |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Financial metrics (replica) | Via `cc_transactionlineitem` + `cctl_*` joins | Directly from `cc_transaction.costtype` / `costcategory` |
# MAGIC | Financial metrics (refined) | `Transaction_Amount` / `ReserveChange_Amount` columns | `amount` column (same source field, practice schema) |
# MAGIC | ClaimCount (refined) | `ClaimDetails_ID` (production alias) | `claimnumber` (our schema column) |
# MAGIC | Process label | `Replica To Refined` | `Replica To Refined` |
# MAGIC | S3 widgets | Present (`BUCKET`, `ext_s3_path`) | Removed — practice uses ADLS only |

# COMMAND ----------

# DBTITLE 1,Imports and Pipeline Parameters
import json
import datetime
import pytz
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import col
from decimal import Decimal

# ADF passes these parameters at runtime.
# Parameters to create in ADF:
#   Name            Type    Source
#   ------------------------------------------------------------------
#   CYC_SK          String  pipeline parameter  e.g. 101
#   ASQL_SCHEMA     String  pipeline parameter  e.g. abc
#   CATALOG         String  pipeline parameter  e.g. insurance_claims_domain
#   REPLICA_SCHEMA  String  pipeline parameter  e.g. replica
#   REFINED_SCHEMA  String  pipeline parameter  e.g. refined
#   ------------------------------------------------------------------

dbutils.widgets.text("CYC_SK", "")
CYC_SK = int(dbutils.widgets.get("CYC_SK"))

dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

dbutils.widgets.text("CATALOG", "")
catalog = dbutils.widgets.get("CATALOG")

dbutils.widgets.text("REPLICA_SCHEMA", "")
replica_schema = dbutils.widgets.get("REPLICA_SCHEMA")

dbutils.widgets.text("REFINED_SCHEMA", "")
refined_schema = dbutils.widgets.get("REFINED_SCHEMA")

print(f"CYC_SK         : {CYC_SK}")
print(f"ASQL_SCHEMA    : {asql_schema}")
print(f"CATALOG        : {catalog}")
print(f"REPLICA_SCHEMA : {replica_schema}")
print(f"REFINED_SCHEMA : {refined_schema}")

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

# DBTITLE 1,Spark Configuration
# Adaptive query execution: Spark optimises join strategies at runtime.
spark.sql("SET spark.sql.adaptive.enabled = true")

# COMMAND ----------

# DBTITLE 1,Fetch Current Cycle Run SK
# Retrieve the current cycle run SK from Azure SQL.
# Used to label BALANCE_METRICS rows with the run that produced them.
df_cyc = get_metadata(
    f"SELECT CYC_CUR_RUN_SK FROM {asql_schema}.CYC_CTRL_TBL WHERE CYC_SK = {CYC_SK}",
    is_query=True
)
cyc_run_sk = int(df_cyc.first()[0])
print(f"cyc_run_sk: {cyc_run_sk}")

# COMMAND ----------

# DBTITLE 1,Metric - ClaimCount
# Replica: count of distinct active claims (retired=0, Soft_Delete='N').
# Refined: count of distinct claims in the current active SCD2 version (ETL_ActiveRow_Flag='Y').
#
# Both counts should match — every non-deleted claim in replica has exactly one
# active row in the claim_detail refined table.

replica_claimcount = spark.sql(f"""
    SELECT count(distinct claimnumber) AS replica_claimcount
    FROM {catalog}.{replica_schema}.cc_claim
    WHERE retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_claimcount']

refined_claimcount = spark.sql(f"""
    SELECT count(distinct claimnumber) AS refined_claimcount
    FROM {catalog}.{refined_schema}.claim_detail
    WHERE ETL_ActiveRow_Flag = 'Y'
""").first()['refined_claimcount']

status_claim_count = 'PASSED' if replica_claimcount == refined_claimcount else 'FAILED'

print(f"replica_claimcount : {replica_claimcount}")
print(f"refined_claimcount : {refined_claimcount}")
print(f"status             : {status_claim_count}")

# COMMAND ----------

# DBTITLE 1,Metric - LossPayment
# Replica: sum of loss payment amounts from cc_transaction.
# Refined: sum of the same amounts from claim_financial (active rows only).
#
# Production joins cc_transactionlineitem + cctl_costtype + cctl_transaction.
# This project: costtype and costcategory are stored directly on cc_transaction
# (and carried through into claim_financial), so no lookup joins are needed.

replica_losspayment = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_losspayment
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossPayment'
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_losspayment']

refined_losspayment = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS refined_losspayment
    FROM {catalog}.{refined_schema}.claim_financial
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossPayment'
      AND ETL_ActiveRow_Flag = 'Y'
""").first()['refined_losspayment']

status_loss_payment = 'PASSED' if round(float(replica_losspayment), 2) == round(float(refined_losspayment), 2) else 'FAILED'

print(f"replica_losspayment : {replica_losspayment}")
print(f"refined_losspayment : {refined_losspayment}")
print(f"status              : {status_loss_payment}")

# COMMAND ----------

# DBTITLE 1,Metric - ExpensePayment
# Replica: sum of expense payment amounts from cc_transaction.
# Refined: sum of the same amounts from claim_financial (active rows only).

replica_expensepayment = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_expensepayment
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpensePayment'
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_expensepayment']

refined_expensepayment = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS refined_expensepayment
    FROM {catalog}.{refined_schema}.claim_financial
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpensePayment'
      AND ETL_ActiveRow_Flag = 'Y'
""").first()['refined_expensepayment']

status_expense_payment = 'PASSED' if round(float(replica_expensepayment), 2) == round(float(refined_expensepayment), 2) else 'FAILED'

print(f"replica_expensepayment : {replica_expensepayment}")
print(f"refined_expensepayment : {refined_expensepayment}")
print(f"status                 : {status_expense_payment}")

# COMMAND ----------

# DBTITLE 1,Metric - LossReserve
# Replica: sum of loss reserve amounts from cc_transaction.
# Refined: sum of the same amounts from claim_financial (active rows only).
#
# Production uses ReserveChange_Amount on the refined side (production column name).
# This project: claim_financial carries the source amount column directly.

replica_lossreserve = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_lossreserve
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossReserve'
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_lossreserve']

refined_lossreserve = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS refined_lossreserve
    FROM {catalog}.{refined_schema}.claim_financial
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossReserve'
      AND ETL_ActiveRow_Flag = 'Y'
""").first()['refined_lossreserve']

status_loss_reserve = 'PASSED' if round(float(replica_lossreserve), 2) == round(float(refined_lossreserve), 2) else 'FAILED'

print(f"replica_lossreserve : {replica_lossreserve}")
print(f"refined_lossreserve : {refined_lossreserve}")
print(f"status              : {status_loss_reserve}")

# COMMAND ----------

# DBTITLE 1,Metric - ExpenseReserve
# Replica: sum of expense reserve amounts from cc_transaction.
# Refined: sum of the same amounts from claim_financial (active rows only).

replica_expensereserve = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_expensereserve
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpenseReserve'
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_expensereserve']

refined_expensereserve = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS refined_expensereserve
    FROM {catalog}.{refined_schema}.claim_financial
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpenseReserve'
      AND ETL_ActiveRow_Flag = 'Y'
""").first()['refined_expensereserve']

status_expense_reserve = 'PASSED' if round(float(replica_expensereserve), 2) == round(float(refined_expensereserve), 2) else 'FAILED'

print(f"replica_expensereserve : {replica_expensereserve}")
print(f"refined_expensereserve : {refined_expensereserve}")
print(f"status                 : {status_expense_reserve}")

# COMMAND ----------

# DBTITLE 1,Write BALANCE_METRICS to Azure SQL
# Build the BALANCE_METRICS DataFrame — one row per metric — and append to
# abc.BALANCE_METRICS in Azure SQL. ADF reads the exit STATUS from this notebook
# via an IfCondition activity to decide whether to continue or fail the pipeline.

current_time = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%dT%H:%M:%S')

schema = StructType([
    StructField('Cyc_SK',          IntegerType(),      True),
    StructField('Curr_Cyc_Run_SK', IntegerType(),      True),
    StructField('Metric_Type',     StringType(),       True),
    StructField('Process',         StringType(),       True),
    StructField('Source_Value',    DecimalType(38, 2), True),
    StructField('Target_Value',    DecimalType(38, 2), True),
    StructField('Status',          StringType(),       True),
    StructField('Aud_Date_Time',   StringType(),       True),
])

rows = [
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "ClaimCount",     "Process": "Replica To Refined", "Source_Value": Decimal(str(replica_claimcount)),     "Target_Value": Decimal(str(refined_claimcount)),     "Status": status_claim_count,     "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "LossPayment",    "Process": "Replica To Refined", "Source_Value": Decimal(str(replica_losspayment)),    "Target_Value": Decimal(str(refined_losspayment)),    "Status": status_loss_payment,    "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "ExpensePayment", "Process": "Replica To Refined", "Source_Value": Decimal(str(replica_expensepayment)), "Target_Value": Decimal(str(refined_expensepayment)), "Status": status_expense_payment, "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "LossReserve",    "Process": "Replica To Refined", "Source_Value": Decimal(str(replica_lossreserve)),    "Target_Value": Decimal(str(refined_lossreserve)),    "Status": status_loss_reserve,    "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "ExpenseReserve", "Process": "Replica To Refined", "Source_Value": Decimal(str(replica_expensereserve)), "Target_Value": Decimal(str(refined_expensereserve)), "Status": status_expense_reserve, "Aud_Date_Time": current_time},
]

df_metrics = spark.createDataFrame(rows, schema=schema)

df_metrics.write.jdbc(
    url=url_Asql,
    table=f"{asql_schema}.BALANCE_METRICS",
    mode="append"
)

print(f"Balance_Metrics written: {df_metrics.count()} rows")
df_metrics.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Exit
# All 5 metrics must pass for STATUS='S'. Any single failure -> STATUS='F'.
# ADF reads Balance_Recon_Status via an IfCondition activity after this notebook.

all_passed = all(s == 'PASSED' for s in [
    status_claim_count,
    status_loss_payment,
    status_expense_payment,
    status_loss_reserve,
    status_expense_reserve,
])
STATUS = 'S' if all_passed else 'F'

print(f"Balance_Recon_Status: {STATUS}")
result = json.dumps({"Balance_Recon_Status": STATUS})
dbutils.notebook.exit(result)