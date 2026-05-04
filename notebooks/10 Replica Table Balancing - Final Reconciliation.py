# Databricks notebook source
# MAGIC %md
# MAGIC ## 10 Replica Table Balancing - Final Reconciliation
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Raw to Replica (Balance Recon)
# MAGIC
# MAGIC **Purpose:** Compares key financial metrics between the raw source (ADLS Delta tables
# MAGIC written by notebooks 08 and 09) and the replica Delta tables (loaded by the ForEach). Writes
# MAGIC one row per metric to `abc.BALANCE_METRICS` in Azure SQL and exits with
# MAGIC `Balance_Recon_Status = S` (all passed) or `F` (any failed) for ADF to evaluate.
# MAGIC
# MAGIC Runs after **both** source-prep notebooks (08, 09) and the ForEach replica load complete.
# MAGIC
# MAGIC **Metrics computed:**
# MAGIC
# MAGIC | Metric | Source query | Replica query |
# MAGIC |--------|-------------|---------------|
# MAGIC | ClaimCount | `count(distinct claimnumber)` from `cc_claim_source` | Same from `cc_claim` |
# MAGIC | LossPayment | `sum(amount)` where `costtype='claimcost'`, `costcategory='LossPayment'` | Same from `cc_transaction` |
# MAGIC | ExpensePayment | `sum(amount)` where `costtype IN ('aoexpense','dccexpense')`, `costcategory='ExpensePayment'` | Same |
# MAGIC | LossReserve | `sum(amount)` where `costtype='claimcost'`, `costcategory='LossReserve'` | Same |
# MAGIC | ExpenseReserve | `sum(amount)` where `costtype IN ('aoexpense','dccexpense')`, `costcategory='ExpenseReserve'` | Same |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Schema param | `config_schema` widget | `ASQL_SCHEMA` widget - same purpose |
# MAGIC | Financial metrics | Via `cc_transactionlineitem` + `cctl_*` joins | Directly from `cc_transaction.costtype` and `cc_transaction.costcategory` |
# MAGIC | ClaimCount filter | `state <> 1` (integer FK to cctl_claimstate = Draft) | `state IN ('Open', 'Closed', 'Reopened')` — actual values from s3_data_generator; no Draft state exists in pilot data |
# MAGIC | Previous metrics lookup | Present (but all `+ prev_*` were commented out) | Removed - dead code |

# COMMAND ----------

# DBTITLE 1,Imports and Pipeline Parameters
import json
import datetime
import pytz
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import col

# ADF passes these parameters at runtime.
# Parameters to create in ADF:
#   Name               Type    Source
#   ------------------------------------------------------------------
#   CYC_SK             String  pipeline parameter  e.g. 101
#   ASQL_SCHEMA        String  pipeline parameter  e.g. abc
#   XCENTER            String  pipeline parameter  e.g. CC
#   CATALOG            String  pipeline parameter  e.g. insurance_claims_domain
#   REPLICA_SCHEMA     String  pipeline parameter  e.g. replica
#   ADLS_BALANCE_PATH  String  abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/balance_recon/
#   ------------------------------------------------------------------

dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

dbutils.widgets.text("ASQL_SCHEMA", "")
asql_schema = dbutils.widgets.get("ASQL_SCHEMA")

dbutils.widgets.text("XCENTER", "")
xcenter = dbutils.widgets.get("XCENTER")

dbutils.widgets.text("CATALOG", "")
catalog = dbutils.widgets.get("CATALOG")

dbutils.widgets.text("REPLICA_SCHEMA", "")
replica_schema = dbutils.widgets.get("REPLICA_SCHEMA")

dbutils.widgets.text("ADLS_BALANCE_PATH", "")
adls_balance_path = dbutils.widgets.get("ADLS_BALANCE_PATH")

print(f"CYC_SK             : {CYC_SK}")
print(f"ASQL_SCHEMA        : {asql_schema}")
print(f"XCENTER            : {xcenter}")
print(f"CATALOG            : {catalog}")
print(f"REPLICA_SCHEMA     : {replica_schema}")
print(f"ADLS_BALANCE_PATH  : {adls_balance_path}")

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

# DBTITLE 1,Get Current Cycle Run SK
# Retrieve the current cycle run SK — needed to scope replica queries to
# rows written in this cycle only (CYC_RUN_SK = cyc_run_sk).
df_cyc = get_metadata(
    f"SELECT CYC_CUR_RUN_SK FROM {asql_schema}.CYC_CTRL_TBL WHERE CYC_SK = {CYC_SK}",
    is_query=True
)
cyc_run_sk = df_cyc.first()[0]
print(f"cyc_run_sk: {cyc_run_sk}")

# COMMAND ----------

# DBTITLE 1,Load Source Delta Tables as Temp Views
# global_temp views are not used because this workspace has Hive Metastore legacy
# access disabled (UC-only mode). Notebooks 08 and 09 wrote the deduped source data
# to ADLS Delta at {ADLS_BALANCE_PATH}{table_name}/. Read them here and register as
# session-local temp views so the metric SQL cells below can query them by name.

adls_balance_path = adls_balance_path.rstrip("/") + "/"

spark.read.format("delta").load(f"{adls_balance_path}cc_claim/").createOrReplaceTempView("cc_claim_source")
spark.read.format("delta").load(f"{adls_balance_path}cc_transaction/").createOrReplaceTempView("cc_transaction_source")

print(f"cc_claim_source      : {spark.table('cc_claim_source').count()} rows")
print(f"cc_transaction_source: {spark.table('cc_transaction_source').count()} rows")

# COMMAND ----------

# DBTITLE 1,Claim Count - Source vs Replica
# Replica: count of distinct active claims written in this cycle.
# Source:  count of distinct active claims in the raw source view.
#
# Production filters state <> 1 (integer FK to cctl_claimstate = Draft).
# This project: state is a string. s3_data_generator produces three values:
#   Open, Closed, Reopened — no Draft state exists in pilot data.
# Filter explicitly lists all valid states to mirror production intent
# (exclude Draft) while matching actual pilot data values.

replica_claimcount = spark.sql(f"""
    SELECT count(distinct claimnumber) AS replica_claimcount
    FROM {catalog}.{replica_schema}.cc_claim
    WHERE CYC_RUN_SK = {cyc_run_sk}
      AND state IN ('Open', 'Closed', 'Reopened')
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_claimcount']

source_claimcount = spark.sql("""
    SELECT count(distinct claimnumber) AS source_claimcount
    FROM cc_claim_source
    WHERE state IN ('Open', 'Closed', 'Reopened')
      AND retired = 0
      AND gwcbi___operation != 1
""").first()['source_claimcount']

status_claim_count = 'PASSED' if round(float(replica_claimcount), 2) == round(float(source_claimcount), 2) else 'FAILED'

print(f"replica_claimcount : {replica_claimcount}")
print(f"source_claimcount  : {source_claimcount}")
print(f"status             : {status_claim_count}")

# COMMAND ----------

# DBTITLE 1,Loss Payment - Source vs Replica
# Replica: sum of transaction amounts for loss cost payments in this cycle.
# Source:  same from the raw source view.
#
# Production: joins cc_transactionlineitem + cctl_costtype + cctl_transaction.
# This project: cc_transaction has costtype and costcategory columns directly
#   so no cctl_* or cc_transactionlineitem joins are needed.

replica_losspayment = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_losspayment
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossPayment'
      AND CYC_RUN_SK = {cyc_run_sk}
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_losspayment']

source_losspayment = spark.sql("""
    SELECT coalesce(sum(amount), 0) AS source_losspayment
    FROM cc_transaction_source
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossPayment'
      AND retired = 0
      AND gwcbi___operation != 1
""").first()['source_losspayment']

status_loss_payment = 'PASSED' if round(float(replica_losspayment), 2) == round(float(source_losspayment), 2) else 'FAILED'

print(f"replica_losspayment : {replica_losspayment}")
print(f"source_losspayment  : {source_losspayment}")
print(f"status              : {status_loss_payment}")

# COMMAND ----------

# DBTITLE 1,Expense Payment - Source vs Replica
# Replica: sum of transaction amounts for expense payments in this cycle.
# Source:  same from the raw source view.

replica_expensepayment = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_expensepayment
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpensePayment'
      AND CYC_RUN_SK = {cyc_run_sk}
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_expensepayment']

source_expensepayment = spark.sql("""
    SELECT coalesce(sum(amount), 0) AS source_expensepayment
    FROM cc_transaction_source
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpensePayment'
      AND retired = 0
      AND gwcbi___operation != 1
""").first()['source_expensepayment']

status_expense_payment = 'PASSED' if round(float(replica_expensepayment), 2) == round(float(source_expensepayment), 2) else 'FAILED'

print(f"replica_expensepayment : {replica_expensepayment}")
print(f"source_expensepayment  : {source_expensepayment}")
print(f"status                 : {status_expense_payment}")

# COMMAND ----------

# DBTITLE 1,Loss Reserve - Source vs Replica
# Replica: sum of transaction amounts for loss reserves in this cycle.
# Source:  same from the raw source view.

replica_lossreserve = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_lossreserve
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossReserve'
      AND CYC_RUN_SK = {cyc_run_sk}
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_lossreserve']

source_lossreserve = spark.sql("""
    SELECT coalesce(sum(amount), 0) AS source_lossreserve
    FROM cc_transaction_source
    WHERE costtype = 'claimcost'
      AND costcategory = 'LossReserve'
      AND retired = 0
      AND gwcbi___operation != 1
""").first()['source_lossreserve']

status_loss_reserve = 'PASSED' if round(float(replica_lossreserve), 2) == round(float(source_lossreserve), 2) else 'FAILED'

print(f"replica_lossreserve : {replica_lossreserve}")
print(f"source_lossreserve  : {source_lossreserve}")
print(f"status              : {status_loss_reserve}")

# COMMAND ----------

# DBTITLE 1,Expense Reserve - Source vs Replica
# Replica: sum of transaction amounts for expense reserves in this cycle.
# Source:  same from the raw source view.

replica_expensereserve = spark.sql(f"""
    SELECT coalesce(sum(amount), 0) AS replica_expensereserve
    FROM {catalog}.{replica_schema}.cc_transaction
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpenseReserve'
      AND CYC_RUN_SK = {cyc_run_sk}
      AND retired = 0
      AND Soft_Delete = 'N'
""").first()['replica_expensereserve']

source_expensereserve = spark.sql("""
    SELECT coalesce(sum(amount), 0) AS source_expensereserve
    FROM cc_transaction_source
    WHERE costtype IN ('aoexpense', 'dccexpense')
      AND costcategory = 'ExpenseReserve'
      AND retired = 0
      AND gwcbi___operation != 1
""").first()['source_expensereserve']

status_expense_reserve = 'PASSED' if round(float(replica_expensereserve), 2) == round(float(source_expensereserve), 2) else 'FAILED'

print(f"replica_expensereserve : {replica_expensereserve}")
print(f"source_expensereserve  : {source_expensereserve}")
print(f"status                 : {status_expense_reserve}")

# COMMAND ----------

# DBTITLE 1,Build Balance_Metrics DataFrame and Write to Azure SQL
# Build the Balance_Metrics DataFrame — one row per metric — and append to
# abc.BALANCE_METRICS in Azure SQL. ADF reads the exit STATUS from this notebook
# via an IfCondition activity to decide whether to continue or fail the pipeline.

from decimal import Decimal

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
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "ClaimCount",     "Process": "Raw To Replica", "Source_Value": Decimal(str(source_claimcount)),     "Target_Value": Decimal(str(replica_claimcount)),     "Status": status_claim_count,     "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "LossPayment",    "Process": "Raw To Replica", "Source_Value": Decimal(str(source_losspayment)),    "Target_Value": Decimal(str(replica_losspayment)),    "Status": status_loss_payment,    "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "ExpensePayment", "Process": "Raw To Replica", "Source_Value": Decimal(str(source_expensepayment)), "Target_Value": Decimal(str(replica_expensepayment)), "Status": status_expense_payment, "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "LossReserve",    "Process": "Raw To Replica", "Source_Value": Decimal(str(source_lossreserve)),    "Target_Value": Decimal(str(replica_lossreserve)),    "Status": status_loss_reserve,    "Aud_Date_Time": current_time},
    {"Cyc_SK": int(CYC_SK), "Curr_Cyc_Run_SK": int(cyc_run_sk), "Metric_Type": "ExpenseReserve", "Process": "Raw To Replica", "Source_Value": Decimal(str(source_expensereserve)), "Target_Value": Decimal(str(replica_expensereserve)), "Status": status_expense_reserve, "Aud_Date_Time": current_time},
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
# All 5 metrics must pass for STATUS='S'. Any single failure → STATUS='F'.
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