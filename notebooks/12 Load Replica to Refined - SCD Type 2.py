# Databricks notebook source
# MAGIC %md
# MAGIC ## 12 Load Replica to Refined - SCD Type 2
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Step:** Replica to Refined
# MAGIC
# MAGIC **Purpose:** For a single refined table, reads from replica Delta tables in Unity Catalog,
# MAGIC applies a configurable transformation SQL (stored in `abc.JOB_PARM_TBL`), computes SCD Type 2
# MAGIC hash keys, and MERGEs the result into an external Delta table registered in Unity Catalog
# MAGIC under `insurance_claims_domain.refined.*`.
# MAGIC
# MAGIC The refined Delta files live in ADLS `refined/` â€” Unity Catalog holds only the metadata pointer
# MAGIC (`USING DELTA LOCATION`). Dropping the UC table does not delete the underlying files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How it works
# MAGIC
# MAGIC **Initial load** (table does not yet exist in Unity Catalog):
# MAGIC 1. Execute the transformation SQL (joins/filters replica tables via `JOB_PARM_TBL.tfm_query_1`)
# MAGIC 2. Compute `ETL_Key_Hash` (MD5 over primary key) and `ETL_SCD2_Hash` (MD5 over business columns, excluding delivery metadata)
# MAGIC 3. Add SCD2 ETL metadata columns (`ETL_ActiveRow_Flag='Y'`, effective/expiry dates, cycle tracking)
# MAGIC 4. Deduplicate on `src_keys` if the transformation query produces duplicates
# MAGIC 5. Assign sequential `SurrogateKey` via `row_number()`
# MAGIC 6. Write Delta files to ADLS `refined/` and register as external table in Unity Catalog
# MAGIC
# MAGIC **Incremental load** (table already exists):
# MAGIC 1. Same transformation, hash, dedup steps
# MAGIC 2. Run SCD Type 2 MERGE:
# MAGIC    - `WHEN MATCHED AND active AND ETL_SCD2_Hash changed` â†’ expire old row (`ETL_ActiveRow_Flag='N'`)
# MAGIC    - `WHEN NOT MATCHED` â†’ insert new active version
# MAGIC 3. Assign `SurrogateKey` to newly inserted rows (those with SK=0 after MERGE)
# MAGIC
# MAGIC **Post-merge cleanup:**
# MAGIC - Mark refined rows inactive where driving replica table has `Soft_Delete='Y'` since last refined run
# MAGIC - Mark refined rows inactive where driving replica table has `retired != 0` since last refined run
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key SCD2 columns
# MAGIC | Column | Purpose |
# MAGIC |--------|----------|
# MAGIC | `ETL_Key_Hash` | MD5 of primary key â€” identifies the entity across versions |
# MAGIC | `ETL_SCD2_Hash` | MD5 of business columns (excl. key + scdexclusions) â€” detects data changes |
# MAGIC | `ETL_ActiveRow_Flag` | `Y`=current version, `N`=expired version |
# MAGIC | `ETL_RecordEffective_Date` | When this version became active |
# MAGIC | `ETL_RecordExpiry_Date` | When this version was superseded (`9999-12-31` if still active) |
# MAGIC | `ETL_Ins_Cyc_SK` | Cycle run when this version was first created |
# MAGIC | `ETL_Lst_Updt_Cyc_Sk` | Cycle run when this version was last expired or updated |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | SQL auth | Key Vault secrets | Hardcoded connection values |
# MAGIC | Transformation query | Stored in `abc.JOB_PARM_TBL` per JOB_SK | Same â€” `abc.JOB_PARM_TBL` |
# MAGIC | Refined tables | 35 tables across all steps | 2 tables: `claim_detail`, `claim_financial` |
# MAGIC | SurrogateKey | Assigned via `row_number()` | Same |
# MAGIC | PROC_UPDATE_JOB_START/END | Called as ADF activities (not in notebook) | Same â€” omitted from notebook |

# COMMAND ----------

# DBTITLE 1,Imports and Pipeline Parameters
import json
import datetime
import pytz
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ADF passes these parameters to the notebook at runtime.
# JOB_SK comes from each ForEach iteration (@item().JOB_SK from TABLES_TO_LOAD_RFN).
# prev_refined_cyc_run_sk: the CYC_RUN_SK of the last successful Replica->Refined run.
# Used to identify which soft-deleted / retired replica rows need to be expired in refined.
#
# Parameters to create in ADF:
#   Name                      Type     Source
#   -----------------------------------------------------------------------
#   JOB_SK                    String   @item().JOB_SK                 (ForEach item)
#   ADLS_REFINED_PATH         String   pipeline parameter  e.g. abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/refined
#   CYC_SK                    String   pipeline parameter  e.g. 101
#   CYC_RUN_SK                String   output of PROC_UPDATE_CYC_START
#   SRC_SCHEMA                String   pipeline parameter  e.g. replica
#   config_schema             String   pipeline parameter  e.g. abc
#   type_data                 String   pipeline parameter  e.g. claims
#   Catalog                   String   pipeline parameter  e.g. insurance_claims_domain
#   DB                        String   pipeline parameter  e.g. refined
#   prev_refined_cyc_run_sk   String   Lookup on CYC_RUN_TBL â€” CYC_RUN_SK of last successful refined run
#   -----------------------------------------------------------------------

dbutils.widgets.text("JOB_SK", "")
JOB_SK = dbutils.widgets.get("JOB_SK")

dbutils.widgets.text("ADLS_REFINED_PATH", "")
ADLS_REFINED_PATH = dbutils.widgets.get("ADLS_REFINED_PATH")

dbutils.widgets.text("CYC_SK", "")
CYC_SK = dbutils.widgets.get("CYC_SK")

dbutils.widgets.text("CYC_RUN_SK", "")
CYC_RUN_SK = dbutils.widgets.get("CYC_RUN_SK")

dbutils.widgets.text("SRC_SCHEMA", "")
SRC_SCHEMA = dbutils.widgets.get("SRC_SCHEMA")

dbutils.widgets.text("config_schema", "")
config_schema = dbutils.widgets.get("config_schema")

dbutils.widgets.text("type_data", "")
type_data = dbutils.widgets.get("type_data")

dbutils.widgets.text("Catalog", "")
Catalog = dbutils.widgets.get("Catalog")

dbutils.widgets.text("DB", "")
DB = dbutils.widgets.get("DB")

dbutils.widgets.text("prev_refined_cyc_run_sk", "0")
prev_refined_cyc_run_sk = dbutils.widgets.get("prev_refined_cyc_run_sk")

# Metric tracking â€” updated in load and metrics cells.
STATUS        = ''
ROWS_READ     = 0
ROWS_LOADED   = 0
ROWS_INSERTED = 0
ROWS_UPDATED  = 0
source_count  = 0
dup_count     = 0

# Timestamps used for SCD2 expiry dates and ETL column values.
# current_time is a formatted string for direct use in SQL literals.
# current_time_UTC is a datetime object for strftime() calls.
current_time     = datetime.datetime.now(pytz.timezone('UTC')).strftime("%Y-%m-%dT%H:%M:%S")
current_time_UTC = datetime.datetime.now(pytz.timezone('UTC'))

print(f"JOB_SK                  : {JOB_SK}")
print(f"ADLS_REFINED_PATH       : {ADLS_REFINED_PATH}")
print(f"CYC_SK                  : {CYC_SK}")
print(f"CYC_RUN_SK              : {CYC_RUN_SK}")
print(f"SRC_SCHEMA              : {SRC_SCHEMA}")
print(f"config_schema           : {config_schema}")
print(f"type_data               : {type_data}")
print(f"Catalog                 : {Catalog}")
print(f"DB                      : {DB}")
print(f"prev_refined_cyc_run_sk : {prev_refined_cyc_run_sk}")

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

# DBTITLE 1,Spark Configuration
# Auto-merge schema: allows Delta to accept new columns from source data
# without failing - handles schema evolution between GW CDA deliveries.
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

# Adaptive query execution: Spark automatically optimises join strategies and
# partition sizes at runtime - important for JOIN-heavy transformation queries.
spark.sql("SET spark.sql.adaptive.enabled = true")

# Disable automatic broadcast joins - replica tables can be large; forcing
# broadcast for large DataFrames causes OOM on the driver.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Disable ANSI mode: replica source columns (e.g. transactiondate, createtime)
# may contain empty strings '' instead of NULL for TIMESTAMP fields. With ANSI
# off, invalid casts return NULL instead of raising CAST_INVALID_INPUT.
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Helper Functions - get_metadata, create_table, deltaScd2Query
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


def create_table(catalog, database, table_name, df, path):
    """
    Write df as a Delta table to path and register it in Unity Catalog.
    Used only for the initial load - incremental loads use SCD2 MERGE.
    persist()/unpersist() avoids recomputing df twice (once for write, once for UC register).
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
        f"CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table_name} "
        f"USING DELTA LOCATION '{path}'"
    )
    print(f"{table_name} Delta table created at {path}")
    return True


def deltaScd2Query(delta_table_name, update_table_name, composite_key, comparison_columns, update_list):
    """
    Execute a SCD Type 2 MERGE into a Delta table.

    The MERGE does two things in a single statement:
      - WHEN MATCHED AND active AND hash changed  -> expire old row (ActiveFlag='N')
      - WHEN NOT MATCHED                          -> insert new active version

    The USING clause is a UNION ALL of:
      (a) Source rows mapped to target keys - feeds the MATCHED expire path
      (b) Source rows that are new OR changed (null merge keys) - feeds the NOT MATCHED insert path

    Uses current_time and CYC_RUN_SK from outer scope for timestamp and cycle tracking.

    Args:
        delta_table_name:   Fully qualified target Delta table  e.g. catalog.schema.table
        update_table_name:  Source temp view name (must be registered before calling)
        composite_key:      List of key column(s) for entity matching  e.g. ['ETL_Key_Hash']
        comparison_columns: List of change-detection column(s)         e.g. ['ETL_SCD2_Hash']
        update_list:        Backtick-wrapped column list ending with ')'.
                            Build with: '`' + '`,`'.join(df.columns) + '`)' then split(',')
                            The trailing ')' in the last element closes both INSERT and VALUES clauses.
    """
    merge_into = "MERGE INTO " + delta_table_name + "\nUSING (\n\tSELECT "

    keys = ",".join([
        update_table_name + "." + composite_key[i] + " as mergeKey" + str(i)
        for i in range(len(composite_key))
    ])

    # First half of UNION ALL: source rows with their key columns projected as mergeKey aliases.
    # These feed the WHEN MATCHED path (expire old active rows where hash changed).
    # Second half: same source rows but with NULL merge keys.
    # These feed the WHEN NOT MATCHED path (insert new versions of changed rows, plus brand-new rows).
    part2 = (
        ", " + update_table_name + ".*\n\tFROM " + update_table_name
        + "\n\n\tUNION ALL\n\n\tSELECT NULL as "
        + ", NULL as ".join(["mergeKey" + str(i) for i in range(len(composite_key))])
        + ", " + update_table_name + ".*"
    )

    # JOIN condition for the NULL-key rows: only include source rows that exist in the target
    # AND whose hash differs from the active target row (i.e., only changed rows get a new version).
    part3 = (
        "\n\tFROM " + update_table_name + " JOIN " + delta_table_name
        + "\n\tON " + " AND ".join([
            f"\n\t\t{update_table_name}.{key} = {delta_table_name}.{key}"
            for key in list(dict.fromkeys(composite_key))
        ])
    )

    part4 = (
        "\n\t WHERE\n\t\t" + delta_table_name + ".ETL_ActiveRow_Flag = 'Y' AND NOT ("
        + " AND ".join([
            f"\n\t\t{delta_table_name}.{c} <=> {update_table_name}.{c}"
            for c in [s for s in comparison_columns if s not in composite_key]
        ]) + ")"
    )

    # ON condition: match target row to the staged update by key hash.
    part5 = (
        "\n\n) staged_updates\nON "
        + " AND ".join([
            delta_table_name + "." + composite_key[i] + " = mergeKey" + str(i)
            for i in range(len(composite_key))
        ])
    )

    # WHEN MATCHED: expire the old active row (set flag='N', record expiry date and cycle).
    part6 = (
        "\nWHEN MATCHED AND " + delta_table_name + ".ETL_ActiveRow_Flag = 'Y' AND NOT ("
        + " AND ".join([
            f"\n\t\t{delta_table_name}.{c} <=> staged_updates.{c}"
            for c in [s for s in comparison_columns if s not in composite_key]
        ]) + ")"
    )

    # WHEN NOT MATCHED: insert new row (new entity or new version of a changed entity).
    # ETL_Ins_Cyc_SK is prepended separately so it is not included in the update_list hash.
    part7 = (
        " THEN \n\t UPDATE SET ETL_ActiveRow_Flag = 'N'"
        + f", ETL_RecordExpiry_Date = '{current_time}'"
        + f", ETL_Lst_Updt_Cyc_Sk = '{CYC_RUN_SK}'"
        + f", ETL_Updated_Date = '{current_time}'"
        + "\nWHEN NOT MATCHED THEN\n\tINSERT(ETL_Ins_Cyc_SK,"
        + ",".join(update_list)
        + f"\n\tVALUES ('{CYC_RUN_SK}',"
        + ",".join(["staged_updates." + s for s in update_list])
    )

    query = merge_into + keys + part2 + part3 + part4 + part5 + part6 + part7
    print(query)
    spark.sql(query)
    print("SCD Type 2 MERGE completed")

# COMMAND ----------

# DBTITLE 1,Load Job Control Parameters from Azure SQL
# Read JOB_CTRL_TBL for this JOB_SK - provides tgt_file_tbl (target table name)
# and tgt_path_schem (target schema name).
abc_control_table = get_metadata(
    f"SELECT * FROM {config_schema}.JOB_CTRL_TBL WHERE JOB_SK = {JOB_SK}",
    is_query=True
)
abc_control_table.createOrReplaceTempView("job_ctltbl")

# Read JOB_PARM_TBL for this JOB_SK - provides all transformation and SCD2 parameters.
abc_parm_control_table = get_metadata(
    f"SELECT * FROM {config_schema}.JOB_PARM_TBL WHERE JOB_SK = {JOB_SK}",
    is_query=True
)
abc_parm_control_table.createOrReplaceTempView("job_param_ctltbl")

# Join job and param tables so all parameters can be queried from a single view.
process = spark.sql(
    f"SELECT job.*, parm_sk, parm_nm, parm_val, parm_zone "
    f"FROM job_ctltbl job "
    f"INNER JOIN job_param_ctltbl parm ON job.job_sk = parm.job_sk AND job.job_sk = {JOB_SK}"
)
process.createOrReplaceTempView("rfn")

# Extract job-level fields from JOB_CTRL_TBL.
tgt_file_tbl   = spark.sql(f"SELECT DISTINCT tgt_file_tbl   FROM rfn WHERE job_sk = {JOB_SK}").collect()[0][0]
tgt_path_schem = spark.sql(f"SELECT DISTINCT tgt_path_schem FROM rfn WHERE job_sk = {JOB_SK}").collect()[0][0]

# Extract SCD2 and load parameters from JOB_PARM_TBL.
# src_keys:        comma-separated primary key column(s) - used to compute ETL_Key_Hash
# SurrogateKey:    name of the auto-increment SK column in the refined table
# partition:       partition column(s) - empty string means no partitioning
# scdexclusions:   columns excluded from ETL_SCD2_Hash (delivery metadata that should
#                  not trigger new SCD2 versions, e.g. cyc_run_sk, gw_timestamp)
# driving_table:   primary replica table used to propagate soft-deletes and retires
# driving_table_PK: PK column of driving_table
src_keys         = spark.sql(f"SELECT DISTINCT parm_val FROM rfn WHERE parm_nm = 'src_keys'         AND job_sk = {JOB_SK}").collect()[0][0]
SurrogateKey     = spark.sql(f"SELECT DISTINCT parm_val FROM rfn WHERE parm_nm = 'SurrogateKey'     AND job_sk = {JOB_SK}").collect()[0][0]
partition        = spark.sql(f"SELECT DISTINCT parm_val FROM rfn WHERE parm_nm = 'partition'        AND job_sk = {JOB_SK}").collect()[0][0]
scdexclusions    = spark.sql(f"SELECT DISTINCT parm_val FROM rfn WHERE parm_nm = 'scdexclusions'    AND job_sk = {JOB_SK}").collect()[0][0]
driving_table    = spark.sql(f"SELECT DISTINCT parm_val FROM rfn WHERE parm_nm = 'Driving_Table'    AND job_sk = {JOB_SK}").collect()[0][0]
driving_table_PK = spark.sql(f"SELECT DISTINCT parm_val FROM rfn WHERE parm_nm = 'Driving_Table_PK' AND job_sk = {JOB_SK}").collect()[0][0]

# Read transformation query from JOB_PARM_TBL (stored as single tfm_query param).
# All parts with parm_nm LIKE '%tfm_query%' are transposed and joined with a space.
df_query = spark.sql(
    f"SELECT parm_val FROM rfn WHERE parm_nm LIKE '%tfm_query%' AND job_sk = {JOB_SK}"
)
df_query_pd = df_query.toPandas().transpose()
df_query_pd["query"] = df_query_pd.apply(lambda x: " ".join(x.astype(str)), axis=1)
spark.createDataFrame(df_query_pd).createOrReplaceTempView("querytable")
query = spark.sql("SELECT query FROM querytable").collect()[0][0]

# Target Delta table path in ADLS and Unity Catalog fully-qualified table name.
schema_name   = DB
tgt_tbl_name  = f"{Catalog}.{schema_name}.{tgt_file_tbl}"
tgt_deltapath = f"{ADLS_REFINED_PATH}/{type_data}/{tgt_file_tbl}/"

print(f"tgt_file_tbl     : {tgt_file_tbl}")
print(f"tgt_tbl_name     : {tgt_tbl_name}")
print(f"tgt_deltapath    : {tgt_deltapath}")
print(f"src_keys         : {src_keys}")
print(f"SurrogateKey     : {SurrogateKey}")
print(f"partition        : {partition}")
print(f"scdexclusions    : {scdexclusions}")
print(f"driving_table    : {driving_table}")
print(f"driving_table_PK : {driving_table_PK}")
print(f"query            : {query}")

# COMMAND ----------

# DBTITLE 1,Execute Transformation Query and Compute SCD2 Hashes
try:
    # Replace @catalog and @src_schema placeholders in the transformation query.
    # These are stored as literals in JOB_PARM_TBL so the query works in any
    # environment by substituting the correct catalog and schema at runtime.
    query = query.replace("@catalog", Catalog)
    query = query.replace("@src_schema", SRC_SCHEMA)

    outDF = spark.sql(query)
    outDF.createOrReplaceTempView("vw_outdf")
    out_columns = [column.lower() for column in outDF.columns]

    # Compute ETL_Key_Hash: MD5 over the primary key column(s).
    # This uniquely identifies each entity across all SCD2 versions.
    #
    # Compute ETL_SCD2_Hash: MD5 over all business columns, excluding:
    #   - src_keys (the primary key - identity signal, not a change signal)
    #   - scdexclusions (delivery metadata like cyc_run_sk, gw_timestamp that change
    #     every cycle but do not represent a business data change)
    # A change in ETL_SCD2_Hash triggers a new SCD2 version for that entity.
    if len(set(out_columns) - set(src_keys.lower().split(","))) == 0:
        # Edge case: all columns are part of the key - ETL_Key_Hash == ETL_SCD2_Hash
        prepsql = (
            "select md5(concat("
            + "COALESCE(`" + "`,\"\"), COALESCE(`".join(src_keys.split(",")) + "`,\"\"))"
            + ") as ETL_Key_Hash, md5(concat("
            + "COALESCE(`" + "`,\"\"), COALESCE(`".join(list(set(out_columns))) + "`,\"\"))"
            + ") as ETL_SCD2_Hash, `" + "`,`".join(out_columns) + "` from vw_outDF"
        )
    else:
        # Standard case: ETL_SCD2_Hash excludes key columns and scdexclusions
        scd2_cols = list(
            set(out_columns)
            - set(src_keys.lower().split(","))
            - set(scdexclusions.lower().split(","))
        )
        prepsql = (
            "select md5(concat("
            + "COALESCE(`" + "`,\"\"), COALESCE(`".join(src_keys.split(",")) + "`,\"\"))"
            + ") as ETL_Key_Hash, md5(concat("
            + "COALESCE(`" + "`,\"\"), COALESCE(`".join(scd2_cols) + "`,\"\"))"
            + ") as ETL_SCD2_Hash, `" + "`,`".join(out_columns) + "` from vw_outDF"
        )

    hashOutDF = spark.sql(prepsql)
    # Initialise SurrogateKey to 0 - assigned real values in the load cells.
    hashOutDF = hashOutDF.withColumn(SurrogateKey, F.lit(0))

    hashOutDF.createOrReplaceTempView("vw_update_table1")

    # Add SCD2 ETL metadata columns to every row.
    # ETL_ActiveRow_Flag='Y': all incoming rows are the current (active) version.
    # ETL_RecordExpiry_Date='9999-12-31': sentinel value meaning "still active".
    # Created/Updated dates: current UTC timestamp.
    hashOutDF = spark.sql(
        "SELECT a.*, "
        + "'Y' AS ETL_ActiveRow_Flag, "
        + f"CAST('{current_time_UTC.strftime('%Y-%m-%dT%H:%M:%S')}' AS TIMESTAMP) AS ETL_RecordEffective_Date, "
        + "CAST('9999-12-31 00:00:00.000000+00:00' AS TIMESTAMP) AS ETL_RecordExpiry_Date, "
        + f"CAST('{current_time_UTC.strftime('%Y-%m-%dT%H:%M:%S')}' AS TIMESTAMP) AS ETL_Created_Date, "
        + f"CAST('{current_time_UTC.strftime('%Y-%m-%dT%H:%M:%S')}' AS TIMESTAMP) AS ETL_Updated_Date "
        + "FROM vw_update_table1 a"
    )
    hashOutDF = hashOutDF.withColumn("ETL_Lst_Updt_Cyc_Sk", F.lit(str(CYC_RUN_SK)))

    hashOutDF.createOrReplaceTempView("nw_update_table")

    # Deduplicate on src_keys: if the transformation query produces multiple rows
    # for the same primary key (possible when replica tables yield duplicates due to
    # multiple deliveries not yet fully deduplicated), keep only the most recent row.
    ddf = spark.sql(
        f"SELECT * FROM ("
        f"  SELECT ROW_NUMBER() OVER (PARTITION BY {src_keys} ORDER BY ETL_Updated_Date DESC) AS ind, *"
        f"  FROM ("
        f"    SELECT * FROM nw_update_table"
        f"    WHERE {src_keys} IN ("
        f"      SELECT {src_keys} FROM ("
        f"        SELECT {src_keys}, COUNT(*) FROM nw_update_table GROUP BY {src_keys} HAVING COUNT(*) > 1"
        f"      )"
        f"    )"
        f"  )"
        f") WHERE ind = 1"
    )
    ddf = ddf.drop("ind")

    ndf = spark.sql(
        f"SELECT * FROM nw_update_table"
        f" WHERE {src_keys} IN ("
        f"   SELECT {src_keys} FROM ("
        f"     SELECT {src_keys}, COUNT(*) FROM nw_update_table GROUP BY {src_keys} HAVING COUNT(*) = 1"
        f"   )"
        f" )"
    )

    hashOutDF = ddf.union(ndf)
    hashOutDF.createOrReplaceTempView("vw_update_table")

except Exception as e:
    print(e)
    raise Exception(e)

# COMMAND ----------

# DBTITLE 1,Compute ROWS_READ
# ROWS_READ is the source-side count for the reconciliation check.
#
# Initial load: count all rows in vw_update_table (the full dataset from transformation query).
#
# Incremental: count rows that represent actual new work for this cycle.
# Rows that already exist in the target with the same hash (both active and inactive versions)
# are considered duplicates and excluded from ROWS_READ.

if not spark.catalog.tableExists(f"{Catalog}.{schema_name}.{tgt_file_tbl}"):
    df_read   = spark.sql("SELECT COUNT(*) AS read_count FROM vw_update_table")
    ROWS_READ = df_read.collect()[0]["read_count"]
else:
    source_df_count = spark.sql("SELECT COUNT(*) AS src_count FROM vw_update_table")
    source_count    = source_df_count.collect()[0]["src_count"]

    # Rows already in the target with the same ETL_SCD2_Hash (both active and inactive)
    # are unchanged rows - they do not represent new MERGE work this cycle.
    df_dup = spark.sql(f"""
        SELECT DISTINCT src.ETL_Key_Hash
        FROM vw_update_table src
        JOIN {Catalog}.{DB}.{tgt_file_tbl} tar
            ON src.ETL_Key_Hash = tar.ETL_Key_Hash
           AND src.ETL_SCD2_Hash = tar.ETL_SCD2_Hash
           AND tar.ETL_ActiveRow_Flag = 'Y'

        UNION

        SELECT DISTINCT src.ETL_Key_Hash
        FROM vw_update_table src
        JOIN {Catalog}.{DB}.{tgt_file_tbl} tar
            ON src.ETL_Key_Hash = tar.ETL_Key_Hash
           AND src.ETL_SCD2_Hash = tar.ETL_SCD2_Hash
           AND tar.ETL_ActiveRow_Flag = 'N'
           AND src.ETL_Key_Hash IN (
               SELECT ETL_Key_Hash FROM {Catalog}.{DB}.{tgt_file_tbl}
               GROUP BY ETL_Key_Hash HAVING COUNT(ETL_Key_Hash) = 1
           )
    """)
    dup_count = df_dup.distinct().count()
    ROWS_READ = source_count - dup_count

print(f"source_count : {source_count}")
print(f"dup_count    : {dup_count}")
print(f"ROWS_READ    : {ROWS_READ}")

# COMMAND ----------

# DBTITLE 1,Initial Load
initialrun = False

try:
    if not spark.catalog.tableExists(f"{Catalog}.{schema_name}.{tgt_file_tbl}"):
        initialrun = True
        print(f"Initial load: {tgt_file_tbl} not found - creating from transformation query result.")

        hashOutDF = hashOutDF.withColumn("ETL_Ins_Cyc_SK", F.lit(str(CYC_RUN_SK)))

        # Assign sequential SurrogateKey starting at 1.
        # monotonically_increasing_id() gives a unique but non-sequential id per row;
        # row_number() over that ordering gives a clean 1-N sequence.
        hashOutDF = hashOutDF.withColumn(SurrogateKey, F.lit(0))
        hashOutDF = hashOutDF.withColumn(
            SurrogateKey,
            F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
        )

        create_table(Catalog, DB, tgt_file_tbl, hashOutDF, tgt_deltapath)

except Exception as e:
    print(e)
    raise Exception(e)

# COMMAND ----------

# DBTITLE 1,Incremental Load - SCD2 MERGE
try:
    if not initialrun:
        print(f"Incremental load: {tgt_file_tbl} found - running SCD2 MERGE.")

        # Get the current max SurrogateKey so newly inserted rows receive the next available SK.
        fload  = spark.read.format("delta").option("mergeSchema", "true").load(tgt_deltapath)
        max_id = fload.agg({SurrogateKey: "max"}).collect()[0][f"max({SurrogateKey})"]

        # Window for SK assignment after MERGE - used to assign sequential IDs
        # to rows where SurrogateKey=0 (newly inserted by the MERGE).
        sk_window = Window.orderBy(SurrogateKey)

        # Initialise SurrogateKey to 0 for all source rows.
        # The MERGE inserts new rows with SK=0; the post-MERGE step replaces 0 with max_id+N.
        hashOutDF = hashOutDF.withColumn(SurrogateKey, F.lit(0))

        # Build the update_list used by deltaScd2Query for the INSERT clause.
        # Format: ['`col1`', '`col2`', ..., '`colN`)'] - the ')' in the last element
        # closes both the INSERT(col_list) and VALUES(val_list) clauses in the generated SQL.
        hashoutdfstr  = "`" + "`,`".join(hashOutDF.columns) + "`)"
        hashoutdflist = hashoutdfstr.split(",")

        # Run the SCD2 MERGE:
        #   - Rows with a changed ETL_SCD2_Hash: old active row is expired, new row is inserted
        #   - Brand-new rows (no match on ETL_Key_Hash): inserted directly as active rows
        deltaScd2Query(
            tgt_tbl_name, "vw_update_table",
            ["ETL_Key_Hash"], ["ETL_SCD2_Hash"],
            hashoutdflist
        )

        # After the MERGE, newly inserted rows have SurrogateKey=0.
        # Read the full table back, assign real SK values to SK=0 rows (new_rows),
        # then overwrite the table to persist the new SK assignments.
        outdf = spark.read.format("delta").option("mergeSchema", "true").load(tgt_deltapath)
        outdf.createOrReplaceTempView("final")

        old_rows = spark.sql(f"SELECT * FROM final WHERE {SurrogateKey} != 0")  # existing rows - keep SK
        new_rows = spark.sql(f"SELECT * FROM final WHERE {SurrogateKey} = 0")   # new rows - assign SK

        new_rows = new_rows.withColumn(SurrogateKey, F.row_number().over(sk_window) + max_id)
        outdf    = old_rows.unionAll(new_rows)

        outdf.write \
            .mode("overwrite") \
            .format("delta") \
            .options(header=True, encoding="UTF-8") \
            .option("mergeSchema", "true") \
            .save(tgt_deltapath)

        print(f"Incremental SCD2 MERGE and SK assignment completed for {tgt_file_tbl}")

except Exception as e:
    print(e)
    raise Exception(e)

# COMMAND ----------

# DBTITLE 1,Row Count Metrics and Reconciliation
# Count rows inserted this cycle: ETL_Ins_Cyc_SK = current CYC_RUN_SK.
tgt_row_cnt_ins = spark.sql(
    f"SELECT COUNT(*) AS tgt_row_cnt_ins "
    f"FROM {Catalog}.{schema_name}.`{tgt_file_tbl}` "
    f"WHERE ETL_Ins_Cyc_SK = '{CYC_RUN_SK}'"
)

# Count rows expired (updated) this cycle:
#   Initial load: no updates - no prior versions exist to expire.
#   Incremental:  rows where ETL_Lst_Updt_Cyc_Sk = current AND not newly inserted this cycle.
if initialrun:
    tgt_row_cnt_updt = spark.sql(
        f"SELECT COUNT(*) AS tgt_row_cnt_updt "
        f"FROM {Catalog}.{schema_name}.`{tgt_file_tbl}` "
        f"WHERE ETL_Lst_Updt_Cyc_Sk = '-1'"
    )
else:
    tgt_row_cnt_updt = spark.sql(
        f"SELECT COUNT(*) AS tgt_row_cnt_updt "
        f"FROM {Catalog}.{schema_name}.`{tgt_file_tbl}` "
        f"WHERE ETL_Lst_Updt_Cyc_Sk = '{CYC_RUN_SK}' AND ETL_Ins_Cyc_SK != '{CYC_RUN_SK}'"
    )

ROWS_INSERTED = int(tgt_row_cnt_ins.toPandas()["tgt_row_cnt_ins"][0])
ROWS_UPDATED  = int(tgt_row_cnt_updt.toPandas()["tgt_row_cnt_updt"][0])
ROWS_LOADED   = ROWS_INSERTED + ROWS_UPDATED

print(f"ROWS_READ     : {ROWS_READ}")
print(f"ROWS_LOADED   : {ROWS_LOADED}")
print(f"ROWS_INSERTED : {ROWS_INSERTED}")
print(f"ROWS_UPDATED  : {ROWS_UPDATED}")

# Reconciliation: every source row should result in an insert (new entity or new version).
if ROWS_READ == ROWS_INSERTED:
    STATUS = 'S'
else:
    STATUS = 'F'

print(f"STATUS : {STATUS}")

# COMMAND ----------

# DBTITLE 1,Propagate Soft Deletes and Retired Records
# Propagate soft-deletes from replica to refined.
# Any replica row with Soft_Delete='Y' that was written AFTER the last successful
# refined run should cause the corresponding refined row to be marked inactive.
# This keeps the refined table in sync with source system deletions.
df = spark.sql(f"""
    UPDATE {Catalog}.{DB}.{tgt_file_tbl}
    SET ETL_ActiveRow_Flag = 'N',
        ETL_RecordExpiry_Date = '{current_time}',
        ETL_Updated_Date = '{current_time}',
        ETL_Lst_Updt_Cyc_Sk = '{CYC_RUN_SK}'
    WHERE TRIM({src_keys}) IN (
        SELECT TRIM({driving_table_PK})
        FROM {Catalog}.{SRC_SCHEMA}.{driving_table}
        WHERE cyc_run_sk > {prev_refined_cyc_run_sk}
          AND Soft_Delete = 'Y'
    )
""")
cnt_soft_delete = df.collect()[0]["num_affected_rows"]
print(f"{cnt_soft_delete} rows marked inactive in {tgt_file_tbl} (Soft_Delete='Y' in replica)")

# Propagate retired records from replica to refined.
# GW ClaimCenter sets retired != 0 when a record is logically retired (not hard-deleted).
# All 5 of our replica tables have a 'retired' column so this block will always execute.
# The check is included for correctness - consistent with the production pattern.
df_driving = spark.sql(f"SELECT * FROM {Catalog}.{SRC_SCHEMA}.{driving_table}")

if "retired" in df_driving.columns:
    df = spark.sql(f"""
        UPDATE {Catalog}.{DB}.{tgt_file_tbl}
        SET ETL_ActiveRow_Flag = 'N',
            ETL_RecordExpiry_Date = '{current_time}',
            ETL_Updated_Date = '{current_time}',
            ETL_Lst_Updt_Cyc_Sk = '{CYC_RUN_SK}'
        WHERE TRIM({src_keys}) IN (
            SELECT TRIM({driving_table_PK})
            FROM {Catalog}.{SRC_SCHEMA}.{driving_table}
            WHERE cyc_run_sk > {prev_refined_cyc_run_sk}
              AND retired != 0
        )
    """)
    cnt_retired = df.collect()[0]["num_affected_rows"]
    print(f"{cnt_retired} rows marked inactive in {tgt_file_tbl} (retired != 0 in replica)")
else:
    print(f"No 'retired' column in {driving_table} - skipping retire propagation")

# COMMAND ----------

# DBTITLE 1,Exit
# Return the job result to ADF as a JSON string via dbutils.notebook.exit().
# ADF reads this from the notebook activity output using:
#   @activity('Load Replica to Refined').output.runOutput
# The values are passed to PROC_UPDATE_JOB_END to close out the JOB_RUN_TBL row.
result = json.dumps({
    "ROWS_READ":     ROWS_READ,
    "ROWS_LOADED":   ROWS_LOADED,
    "ROWS_INSERTED": ROWS_INSERTED,
    "ROWS_UPDATED":  ROWS_UPDATED,
    "STATUS":        STATUS
})
print(result)
dbutils.notebook.exit(result)