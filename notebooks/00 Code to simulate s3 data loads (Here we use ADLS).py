# Databricks notebook source
# MAGIC %md
# MAGIC # GW CDA S3 Data Generator — Practice Project
# MAGIC
# MAGIC Simulates Guidewire CDA data delivery into ADLS `client_data/` folder inside the
# MAGIC `azurepractice68256` storage account (`insurance-claims-domain` container).
# MAGIC
# MAGIC **ADLS folder structure written by this notebook:**
# MAGIC ```
# MAGIC insurance-claims-domain/
# MAGIC   client_data/
# MAGIC     manifest.json
# MAGIC     cc_policy/{fingerprint}/{timestamp}/part-00000.snappy.parquet
# MAGIC     cc_claim/{fingerprint}/{timestamp}/part-00000.snappy.parquet
# MAGIC     cc_exposure/{fingerprint}/{timestamp}/part-00000.snappy.parquet
# MAGIC     cc_contact/{fingerprint}/{timestamp}/part-00000.snappy.parquet
# MAGIC     cc_transaction/{fingerprint}/{timestamp}/part-00000.snappy.parquet
# MAGIC ```
# MAGIC
# MAGIC **State persistence:** Generated record IDs are saved to Unity Catalog table
# MAGIC `insurance_claims_domain.s3_data.generator_state` so subsequent runs produce realistic CDC operations.
# MAGIC
# MAGIC **CDC behaviour per run:**
# MAGIC
# MAGIC | Run | INSERTs | UPDATEs | DELETEs | Notes |
# MAGIC |-----|---------|---------|---------|-------|
# MAGIC | 1st | 100% | 0% | 0% | No prior IDs exist |
# MAGIC | 2nd+ | ~60% | ~30% | ~10% | Mixed CDC on existing + new records |
# MAGIC
# MAGIC **Also included (to exercise Raw→Replica dedup logic):**
# MAGIC - Every UPDATE produces 2 rows for the same `id` in the same file — one stale (60s older `gwcbi___payload_ts_ms`) and one current. The Raw→Replica notebook must discard the stale one via `ROW_NUMBER OVER (PARTITION BY id ORDER BY gwcbi___payload_ts_ms DESC)`.
# MAGIC
# MAGIC **Multi-run parquet / manifest behaviour (matches real GW CDA):**
# MAGIC
# MAGIC | Aspect | Behaviour |
# MAGIC |---|---|
# MAGIC | Parquet folders | New `{timestamp}` folder each run — old folders stay (append-only) |
# MAGIC | `manifest.json` | Overwritten each run |
# MAGIC | `schemaHistory` | Accumulated — fingerprint → first-seen timestamp, never removed |
# MAGIC | `lastSuccessfulWriteTimestamp` | Replaced with current run timestamp |
# MAGIC | `totalProcessedRecordsCount` | Cumulative — adds this run's rows to running total |
# MAGIC
# MAGIC | Table | Rows per run |
# MAGIC |---|---|
# MAGIC | cc_policy | 50 |
# MAGIC | cc_claim | 100 |
# MAGIC | cc_exposure | 150 |
# MAGIC | cc_contact | 80 |
# MAGIC | cc_transaction | 200 |

# COMMAND ----------

# DBTITLE 1,Imports
import hashlib, json, random, time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, TimestampType
)
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configuration

# ── Environment ───────────────────────────────────────────────────────────────
STORAGE_ACCOUNT  = "azurepractice68256"
CONTAINER        = "insurance-claims-domain"   # hyphen, as created in portal
CATALOG          = "insurance_claims_domain"   # Unity Catalog catalog name (underscore)
SIM_SCHEMA       = "s3_data"
STATE_TABLE      = f"{CATALOG}.{SIM_SCHEMA}.generator_state"

EXT_PATH      = "client_data"                  # landing zone inside the container
BASE_PATH     = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{EXT_PATH}"
MANIFEST_PATH = f"{BASE_PATH}/manifest.json"

# ── Storage Key ───────────────────────────────────────────────────────────────
# Dev/learning only — do NOT hardcode keys in production (use cluster config or Key Vault)
# Retrieve your key:
#   az storage account keys list --account-name azurepractice68256 \
#       --resource-group azurepractice68256 --query "[0].value" -o tsv
STORAGE_KEY = "<your-adls-storage-key>"  # In production: use Key Vault or cluster-level Spark config
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY
)

print(f"Storage    : {STORAGE_ACCOUNT} / {CONTAINER}")
print(f"Base path  : {BASE_PATH}")
print(f"Manifest   : {MANIFEST_PATH}")
print(f"State table: {STATE_TABLE}")


# COMMAND ----------

# DBTITLE 1,Load Existing IDs from State Table

# Schema and table already created via one-time DDL script.
# Just load existing IDs here.

existing = (
    spark.table(STATE_TABLE)
         .filter("is_deleted = false")
         .groupBy("table_name")
         .agg(F.collect_list("record_id").alias("ids"))
         .collect()
)
existing_ids = {row["table_name"]: row["ids"] for row in existing}

IS_FIRST_RUN = not bool(existing_ids)
print(f"State table : {STATE_TABLE}")
print(f"First run   : {IS_FIRST_RUN}")
for tbl, ids in existing_ids.items():
    print(f"  {tbl:<18}  {len(ids):>5} active IDs available for CDC")


# COMMAND ----------

# DBTITLE 1,Fingerprints & Run Timestamp
# ── Fingerprints & Run Timestamp ──────────────────────────────────────────────
TABLES = ["cc_policy", "cc_claim", "cc_exposure", "cc_contact", "cc_transaction"]

fingerprints = {t: hashlib.md5(f"{t}_schema_v1".encode()).hexdigest() for t in TABLES}
RUN_TS       = str(int(time.time() * 1000))

print(f"Run timestamp : {RUN_TS}")
print("Fingerprints:")
for t in TABLES:
    print(f"  {t:<18}  {fingerprints[t]}")

# COMMAND ----------

# DBTITLE 1,CDC Helper Functions
# ── CDC Helpers ───────────────────────────────────────────────────────────────
def rand_id(prefix="cc"):
    return f"{prefix}:{random.randint(100000, 999999)}"

def rand_date(start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    delta = datetime(end_year, 12, 31) - start
    return start + timedelta(days=random.randint(0, delta.days))

def make_cdc_cols(operation=0, payload_ts_ms=None):
    """
    operation codes (real GW CDA):
      0 = INSERT
      1 = DELETE  (before-image — soft delete)
      2 = INSERT  (after-image of UPDATE)
      4 = UPDATE  (full row)
    payload_ts_ms: pass an explicit value when generating a stale duplicate
                   (lower ts = older = should lose in ROW_NUMBER dedup)
    """
    now_ms = payload_ts_ms if payload_ts_ms else int(time.time() * 1000)
    lsn    = random.randint(1_000_000, 9_999_999)
    return {
        "gwcbi___operation"      : operation,
        "gwcbi___connector_ts_ms": now_ms,
        "gwcbi___lsn"            : lsn,
        "gwcbi___seqval"         : float(lsn),
        "gwcbi___payload_ts_ms"  : now_ms,
        "gwcbi___tx_id"          : random.randint(10_000, 99_999),
        "gwcbi___seqval_hex"     : format(lsn, "x").zfill(16),
    }

CDC_FIELDS = [
    StructField("gwcbi___operation",       IntegerType(), True),
    StructField("gwcbi___connector_ts_ms", LongType(),    True),
    StructField("gwcbi___lsn",             LongType(),    True),
    StructField("gwcbi___seqval",          DoubleType(),  True),
    StructField("gwcbi___payload_ts_ms",   LongType(),    True),
    StructField("gwcbi___tx_id",           LongType(),    True),
    StructField("gwcbi___seqval_hex",      StringType(),  True),
]

def split_ids(existing, n_new, n_update, n_delete):
    """
    Returns (new_ids, update_ids, delete_ids).
    If not enough existing IDs, falls back to all INSERTs.
    """
    pool = list(existing)
    random.shuffle(pool)
    del_ids    = pool[:n_delete]
    upd_ids    = pool[n_delete: n_delete + n_update]
    new_ids    = [rand_id() for _ in range(n_new)]
    return new_ids, upd_ids, del_ids

# COMMAND ----------

# DBTITLE 1,cc_policy Schema & Generator
POLICY_TYPES  = ["Homeowners", "Condominium", "Dwelling Fire", "Renters"]
POLICY_STATUS = ["In Force", "Cancelled", "Expired"]

policy_schema = StructType([
    StructField("id",             StringType(),    False),
    StructField("publicid",       StringType(),    True),
    StructField("policynumber",   StringType(),    True),
    StructField("policytype",     StringType(),    True),
    StructField("effectivedate",  TimestampType(), True),
    StructField("expirationdate", TimestampType(), True),
    StructField("status",         StringType(),    True),
    StructField("premium",        DoubleType(),    True),
    StructField("insuranceline",  StringType(),    True),
    StructField("createtime",     TimestampType(), True),
    StructField("updatetime",     TimestampType(), True),
    StructField("retired",        IntegerType(),   True),
] + CDC_FIELDS)

def gen_policy_rows(existing_pool, n=50):
    n_new = int(n * 0.60); n_upd = int(n * 0.30); n_del = n - n_new - n_upd
    if IS_FIRST_RUN: n_new, n_upd, n_del = n, 0, 0
    new_ids, upd_ids, del_ids = split_ids(existing_pool, n_new, n_upd, n_del)

    rows = []
    now_ms = int(time.time() * 1000)

    # INSERTs — brand new records
    for rid in new_ids:
        eff = rand_date(2020, 2024)
        rows.append({"id": rid, "publicid": rid,
            "policynumber": f"HO-{random.randint(2020,2025)}-{random.randint(100000,999999)}",
            "policytype": random.choice(POLICY_TYPES),
            "effectivedate": eff, "expirationdate": eff + timedelta(days=365),
            "status": random.choice(POLICY_STATUS),
            "premium": round(random.uniform(800, 5000), 2),
            "insuranceline": "Homeowners",
            "createtime": eff, "updatetime": eff, "retired": 0,
            **make_cdc_cols(0)})

    # UPDATEs — existing IDs, newer payload_ts_ms (operation=4)
    # Also add a stale duplicate (older ts) to exercise ROW_NUMBER dedup
    for rid in upd_ids:
        eff = rand_date(2020, 2024)
        # Stale duplicate — same id, older timestamp — dedup must discard this
        rows.append({"id": rid, "publicid": rid,
            "policynumber": f"HO-STALE-{random.randint(100000,999999)}",
            "policytype": random.choice(POLICY_TYPES),
            "effectivedate": eff, "expirationdate": eff + timedelta(days=365),
            "status": "Expired",
            "premium": round(random.uniform(800, 5000), 2),
            "insuranceline": "Homeowners",
            "createtime": eff, "updatetime": eff, "retired": 0,
            **make_cdc_cols(4, payload_ts_ms=now_ms - 60_000)})  # 60s older
        # Latest update — this one wins
        rows.append({"id": rid, "publicid": rid,
            "policynumber": f"HO-{random.randint(2020,2025)}-{random.randint(100000,999999)}",
            "policytype": random.choice(POLICY_TYPES),
            "effectivedate": eff, "expirationdate": eff + timedelta(days=365),
            "status": random.choice(POLICY_STATUS),
            "premium": round(random.uniform(800, 5000), 2),
            "insuranceline": "Homeowners",
            "createtime": eff, "updatetime": datetime.utcnow(), "retired": 0,
            **make_cdc_cols(4)})

    # DELETEs — operation=1 (before-image), replica sets Soft_Delete='Y'
    for rid in del_ids:
        eff = rand_date(2020, 2024)
        rows.append({"id": rid, "publicid": rid,
            "policynumber": f"HO-DEL-{random.randint(100000,999999)}",
            "policytype": random.choice(POLICY_TYPES),
            "effectivedate": eff, "expirationdate": eff + timedelta(days=365),
            "status": "Cancelled", "premium": 0.0, "insuranceline": "Homeowners",
            "createtime": eff, "updatetime": datetime.utcnow(), "retired": 1,
            **make_cdc_cols(1)})

    return rows, new_ids, del_ids

# COMMAND ----------

# DBTITLE 1,cc_claim Schema & Generator
LOSS_TYPES   = ["Property", "Liability", "Medical", "Vehicle"]
CLAIM_STATES = ["Open", "Closed", "Reopened"]
LOSS_CAUSES  = list(range(1, 9))

claim_schema = StructType([
    StructField("id",              StringType(),    False),
    StructField("publicid",        StringType(),    True),
    StructField("claimnumber",     StringType(),    True),
    StructField("policyid",        StringType(),    True),
    StructField("lossdate",        TimestampType(), True),
    StructField("reporteddate",    TimestampType(), True),
    StructField("closeddate",      TimestampType(), True),
    StructField("losscause",       IntegerType(),   True),
    StructField("losstype",        StringType(),    True),
    StructField("state",           StringType(),    True),
    StructField("catastropheid",   StringType(),    True),
    StructField("assignedgroupid", StringType(),    True),
    StructField("assigneduserid",  StringType(),    True),
    StructField("createtime",      TimestampType(), True),
    StructField("updatetime",      TimestampType(), True),
    StructField("retired",         IntegerType(),   True),
] + CDC_FIELDS)

def gen_claim_rows(policy_ids, existing_pool, n=100):
    n_new = int(n * 0.60); n_upd = int(n * 0.30); n_del = n - n_new - n_upd
    if IS_FIRST_RUN: n_new, n_upd, n_del = n, 0, 0
    new_ids, upd_ids, del_ids = split_ids(existing_pool, n_new, n_upd, n_del)

    rows = []
    now_ms = int(time.time() * 1000)

    for rid in new_ids:
        loss_dt = rand_date(2022, 2025)
        rpt_dt  = loss_dt + timedelta(days=random.randint(0, 30))
        state   = random.choice(CLAIM_STATES)
        rows.append({"id": rid, "publicid": rid,
            "claimnumber": f"{random.randint(2022,2025)}-CC-{random.randint(100000,999999)}",
            "policyid": random.choice(policy_ids),
            "lossdate": loss_dt, "reporteddate": rpt_dt,
            "closeddate": loss_dt + timedelta(days=random.randint(30,365)) if state=="Closed" else None,
            "losscause": random.choice(LOSS_CAUSES), "losstype": random.choice(LOSS_TYPES),
            "state": state,
            "catastropheid": rand_id("cat") if random.random() > 0.85 else None,
            "assignedgroupid": rand_id("grp"), "assigneduserid": rand_id("usr"),
            "createtime": rpt_dt, "updatetime": rpt_dt, "retired": 0,
            **make_cdc_cols(0)})

    for rid in upd_ids:
        loss_dt = rand_date(2022, 2025)
        rpt_dt  = loss_dt + timedelta(days=random.randint(0, 30))
        # Stale duplicate
        rows.append({"id": rid, "publicid": rid,
            "claimnumber": f"STALE-CC-{random.randint(100000,999999)}",
            "policyid": random.choice(policy_ids),
            "lossdate": loss_dt, "reporteddate": rpt_dt, "closeddate": None,
            "losscause": random.choice(LOSS_CAUSES), "losstype": random.choice(LOSS_TYPES),
            "state": "Open", "catastropheid": None,
            "assignedgroupid": rand_id("grp"), "assigneduserid": rand_id("usr"),
            "createtime": loss_dt, "updatetime": loss_dt, "retired": 0,
            **make_cdc_cols(2, payload_ts_ms=now_ms - 60_000)})
        # Latest
        rows.append({"id": rid, "publicid": rid,
            "claimnumber": f"{random.randint(2022,2025)}-CC-{random.randint(100000,999999)}",
            "policyid": random.choice(policy_ids),
            "lossdate": loss_dt, "reporteddate": rpt_dt,
            "closeddate": loss_dt + timedelta(days=random.randint(30,365)),
            "losscause": random.choice(LOSS_CAUSES), "losstype": random.choice(LOSS_TYPES),
            "state": "Closed",
            "catastropheid": rand_id("cat") if random.random() > 0.85 else None,
            "assignedgroupid": rand_id("grp"), "assigneduserid": rand_id("usr"),
            "createtime": loss_dt, "updatetime": datetime.utcnow(), "retired": 0,
            **make_cdc_cols(2)})

    for rid in del_ids:
        loss_dt = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid,
            "claimnumber": f"DEL-CC-{random.randint(100000,999999)}",
            "policyid": random.choice(policy_ids),
            "lossdate": loss_dt, "reporteddate": loss_dt, "closeddate": None,
            "losscause": 1, "losstype": "Property", "state": "Closed",
            "catastropheid": None, "assignedgroupid": rand_id("grp"),
            "assigneduserid": rand_id("usr"),
            "createtime": loss_dt, "updatetime": datetime.utcnow(), "retired": 1,
            **make_cdc_cols(1)})

    return rows, new_ids, del_ids

# COMMAND ----------

# DBTITLE 1,cc_exposure Schema & Generator
COVERAGE_TYPES = ["Dwelling", "Personal Property", "Liability", "Medical Payments", "Loss of Use"]
EXP_STATES     = ["Open", "Closed"]

exposure_schema = StructType([
    StructField("id",            StringType(),    False),
    StructField("publicid",      StringType(),    True),
    StructField("claimid",       StringType(),    True),
    StructField("coverageid",    StringType(),    True),
    StructField("coveragetype",  StringType(),    True),
    StructField("exposurestate", StringType(),    True),
    StructField("losscause",     IntegerType(),   True),
    StructField("createtime",    TimestampType(), True),
    StructField("updatetime",    TimestampType(), True),
    StructField("retired",       IntegerType(),   True),
] + CDC_FIELDS)

def gen_exposure_rows(claim_ids, existing_pool, n=150):
    n_new = int(n * 0.60); n_upd = int(n * 0.30); n_del = n - n_new - n_upd
    if IS_FIRST_RUN: n_new, n_upd, n_del = n, 0, 0
    new_ids, upd_ids, del_ids = split_ids(existing_pool, n_new, n_upd, n_del)

    rows = []; now_ms = int(time.time() * 1000)
    for rid in new_ids:
        created = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid,
            "claimid": random.choice(claim_ids), "coverageid": rand_id("cov"),
            "coveragetype": random.choice(COVERAGE_TYPES),
            "exposurestate": random.choice(EXP_STATES),
            "losscause": random.choice(LOSS_CAUSES),
            "createtime": created, "updatetime": created, "retired": 0,
            **make_cdc_cols(0)})
    for rid in upd_ids:
        created = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid,
            "claimid": random.choice(claim_ids), "coverageid": rand_id("cov"),
            "coveragetype": random.choice(COVERAGE_TYPES), "exposurestate": "Open",
            "losscause": random.choice(LOSS_CAUSES),
            "createtime": created, "updatetime": created, "retired": 0,
            **make_cdc_cols(4, payload_ts_ms=now_ms - 60_000)})  # stale
        rows.append({"id": rid, "publicid": rid,
            "claimid": random.choice(claim_ids), "coverageid": rand_id("cov"),
            "coveragetype": random.choice(COVERAGE_TYPES), "exposurestate": "Closed",
            "losscause": random.choice(LOSS_CAUSES),
            "createtime": created, "updatetime": datetime.utcnow(), "retired": 0,
            **make_cdc_cols(4)})
    for rid in del_ids:
        created = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid,
            "claimid": random.choice(claim_ids), "coverageid": rand_id("cov"),
            "coveragetype": "Dwelling", "exposurestate": "Closed",
            "losscause": 1, "createtime": created, "updatetime": datetime.utcnow(), "retired": 1,
            **make_cdc_cols(1)})
    return rows, new_ids, del_ids

# COMMAND ----------

# DBTITLE 1,cc_contact Schema & Generator
FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer",
               "Michael","Linda","William","Barbara","David","Susan"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia",
               "Miller","Davis","Wilson","Taylor","Anderson","Thomas"]
SUBTYPES    = ["Person", "Company"]

contact_schema = StructType([
    StructField("id",            StringType(),    False),
    StructField("publicid",      StringType(),    True),
    StructField("firstname",     StringType(),    True),
    StructField("lastname",      StringType(),    True),
    StructField("emailaddress1", StringType(),    True),
    StructField("primaryphone",  StringType(),    True),
    StructField("dateofbirth",   TimestampType(), True),
    StructField("taxid",         StringType(),    True),
    StructField("subtype",       StringType(),    True),
    StructField("createtime",    TimestampType(), True),
    StructField("updatetime",    TimestampType(), True),
    StructField("retired",       IntegerType(),   True),
] + CDC_FIELDS)

def gen_contact_rows(existing_pool, n=80):
    n_new = int(n * 0.60); n_upd = int(n * 0.30); n_del = n - n_new - n_upd
    if IS_FIRST_RUN: n_new, n_upd, n_del = n, 0, 0
    new_ids, upd_ids, del_ids = split_ids(existing_pool, n_new, n_upd, n_del)

    rows = []; now_ms = int(time.time() * 1000)
    for rid in new_ids:
        fn = random.choice(FIRST_NAMES); ln = random.choice(LAST_NAMES)
        created = rand_date(2020, 2025)
        rows.append({"id": rid, "publicid": rid, "firstname": fn, "lastname": ln,
            "emailaddress1": f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com",
            "primaryphone": f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}",
            "dateofbirth": rand_date(1950, 1995) if random.random() > 0.3 else None,
            "taxid": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}" if random.random() > 0.5 else None,
            "subtype": random.choice(SUBTYPES), "createtime": created, "updatetime": created, "retired": 0,
            **make_cdc_cols(0)})
    for rid in upd_ids:
        fn = random.choice(FIRST_NAMES); ln = random.choice(LAST_NAMES)
        created = rand_date(2020, 2025)
        rows.append({"id": rid, "publicid": rid, "firstname": fn, "lastname": ln,
            "emailaddress1": f"old.{fn.lower()}@example.com", "primaryphone": "(000) 000-0000",
            "dateofbirth": None, "taxid": None, "subtype": "Person",
            "createtime": created, "updatetime": created, "retired": 0,
            **make_cdc_cols(4, payload_ts_ms=now_ms - 60_000)})  # stale
        rows.append({"id": rid, "publicid": rid, "firstname": fn, "lastname": ln,
            "emailaddress1": f"{fn.lower()}.{ln.lower()}{random.randint(1,99)}@example.com",
            "primaryphone": f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}",
            "dateofbirth": rand_date(1950, 1995), "taxid": None,
            "subtype": random.choice(SUBTYPES), "createtime": created, "updatetime": datetime.utcnow(), "retired": 0,
            **make_cdc_cols(4)})
    for rid in del_ids:
        created = rand_date(2020, 2025)
        rows.append({"id": rid, "publicid": rid, "firstname": "DELETED", "lastname": "DELETED",
            "emailaddress1": None, "primaryphone": None, "dateofbirth": None, "taxid": None,
            "subtype": "Person", "createtime": created, "updatetime": datetime.utcnow(), "retired": 1,
            **make_cdc_cols(1)})
    return rows, new_ids, del_ids

# COMMAND ----------

# DBTITLE 1,cc_transaction Schema & Generator
COST_TYPES      = ["claimcost", "aoexpense", "dccexpense"]
COST_CATEGORIES = ["LossPayment", "ExpensePayment", "LossReserve", "ExpenseReserve"]
TX_STATUSES     = ["Approved", "Pending", "Rejected"]

transaction_schema = StructType([
    StructField("id",              StringType(),    False),
    StructField("publicid",        StringType(),    True),
    StructField("claimid",         StringType(),    True),
    StructField("amount",          DoubleType(),    True),
    StructField("currency",        StringType(),    True),
    StructField("transactiondate", TimestampType(), True),
    StructField("status",          StringType(),    True),
    StructField("costtype",        StringType(),    True),
    StructField("costcategory",    StringType(),    True),
    StructField("reserveline",     StringType(),    True),
    StructField("createtime",      TimestampType(), True),
    StructField("updatetime",      TimestampType(), True),
    StructField("retired",         IntegerType(),   True),
] + CDC_FIELDS)

def gen_transaction_rows(claim_ids, existing_pool, n=200):
    n_new = int(n * 0.60); n_upd = int(n * 0.30); n_del = n - n_new - n_upd
    if IS_FIRST_RUN: n_new, n_upd, n_del = n, 0, 0
    new_ids, upd_ids, del_ids = split_ids(existing_pool, n_new, n_upd, n_del)

    rows = []; now_ms = int(time.time() * 1000)
    for rid in new_ids:
        tx_date = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid, "claimid": random.choice(claim_ids),
            "amount": round(random.uniform(100, 75000), 2), "currency": "USD",
            "transactiondate": tx_date, "status": random.choice(TX_STATUSES),
            "costtype": random.choice(COST_TYPES), "costcategory": random.choice(COST_CATEGORIES),
            "reserveline": rand_id("rl"), "createtime": tx_date, "updatetime": tx_date, "retired": 0,
            **make_cdc_cols(0)})
    for rid in upd_ids:
        tx_date = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid, "claimid": random.choice(claim_ids),
            "amount": round(random.uniform(100, 75000), 2), "currency": "USD",
            "transactiondate": tx_date, "status": "Pending",
            "costtype": random.choice(COST_TYPES), "costcategory": random.choice(COST_CATEGORIES),
            "reserveline": rand_id("rl"), "createtime": tx_date, "updatetime": tx_date, "retired": 0,
            **make_cdc_cols(4, payload_ts_ms=now_ms - 60_000)})  # stale
        rows.append({"id": rid, "publicid": rid, "claimid": random.choice(claim_ids),
            "amount": round(random.uniform(100, 75000), 2), "currency": "USD",
            "transactiondate": tx_date, "status": "Approved",
            "costtype": random.choice(COST_TYPES), "costcategory": random.choice(COST_CATEGORIES),
            "reserveline": rand_id("rl"), "createtime": tx_date, "updatetime": datetime.utcnow(), "retired": 0,
            **make_cdc_cols(4)})
    for rid in del_ids:
        tx_date = rand_date(2022, 2025)
        rows.append({"id": rid, "publicid": rid, "claimid": random.choice(claim_ids),
            "amount": 0.0, "currency": "USD", "transactiondate": tx_date, "status": "Rejected",
            "costtype": "claimcost", "costcategory": "LossPayment",
            "reserveline": rand_id("rl"), "createtime": tx_date, "updatetime": datetime.utcnow(), "retired": 1,
            **make_cdc_cols(1)})
    return rows, new_ids, del_ids

# COMMAND ----------

# DBTITLE 1,Generate All Rows
policy_rows, policy_new_ids, policy_del_ids       = gen_policy_rows(existing_ids.get("cc_policy", []))
policy_ids = [r["id"] for r in policy_rows if r["gwcbi___operation"] != 1]  # non-deleted for FK use

claim_rows, claim_new_ids, claim_del_ids          = gen_claim_rows(policy_ids, existing_ids.get("cc_claim", []))
claim_ids = [r["id"] for r in claim_rows if r["gwcbi___operation"] != 1]

exposure_rows, exposure_new_ids, exposure_del_ids = gen_exposure_rows(claim_ids, existing_ids.get("cc_exposure", []))
contact_rows, contact_new_ids, contact_del_ids    = gen_contact_rows(existing_ids.get("cc_contact", []))
tx_rows, tx_new_ids, tx_del_ids                   = gen_transaction_rows(claim_ids, existing_ids.get("cc_transaction", []))

all_rows    = {"cc_policy": policy_rows, "cc_claim": claim_rows, "cc_exposure": exposure_rows,
               "cc_contact": contact_rows, "cc_transaction": tx_rows}
new_ids_map = {"cc_policy": policy_new_ids, "cc_claim": claim_new_ids, "cc_exposure": exposure_new_ids,
               "cc_contact": contact_new_ids, "cc_transaction": tx_new_ids}
del_ids_map = {"cc_policy": policy_del_ids, "cc_claim": claim_del_ids, "cc_exposure": exposure_del_ids,
               "cc_contact": contact_del_ids, "cc_transaction": tx_del_ids}

print(f"{'Table':<20} {'Total rows':>10} {'INSERTs':>9} {'UPDATEs (×2)':>13} {'DELETEs':>9}")
print("-" * 65)
for t in TABLES:
    rows = all_rows[t]
    ops  = [r["gwcbi___operation"] for r in rows]
    ins  = ops.count(0)
    upd  = ops.count(2) + ops.count(4)   # includes stale duplicates
    dlt  = ops.count(1)
    print(f"  {t:<18} {len(rows):>10} {ins:>9} {upd:>13} {dlt:>9}")

# COMMAND ----------

# DBTITLE 1,Write Parquet Files to ADLS
schemas = {
    "cc_policy": policy_schema, "cc_claim": claim_schema, "cc_exposure": exposure_schema,
    "cc_contact": contact_schema, "cc_transaction": transaction_schema
}
row_counts = {}

def write_table(table_name):
    rows   = all_rows[table_name]
    schema = schemas[table_name]
    fp     = fingerprints[table_name]
    path   = f"{BASE_PATH}/{table_name}/{fp}/{RUN_TS}"

    pdf = pd.DataFrame(rows)
    df  = spark.createDataFrame(pdf, schema=schema)
    (df.coalesce(1)
       .write
       .format("parquet")
       .option("compression", "snappy")
       .mode("overwrite")
       .save(path))

    ops = [r["gwcbi___operation"] for r in rows]
    print(f"  {table_name:<18}  {len(rows):>4} rows  "
          f"(I={ops.count(0)} U={ops.count(2)+ops.count(4)} D={ops.count(1)})  "
          f"→ .../{table_name}/{fp[:8]}.../{RUN_TS}")
    return len([r for r in rows if r["gwcbi___operation"] != 1])  # count non-deletes for manifest

print(f"Writing parquet files (RUN_TS={RUN_TS})...\n")
for t in TABLES:
    row_counts[t] = write_table(t)
print("\nAll parquet files written.")

# COMMAND ----------

# DBTITLE 1,Generate & Write manifest.json
existing_manifest = {}
try:
    raw = dbutils.fs.head(MANIFEST_PATH, 200_000)
    existing_manifest = json.loads(raw)
    print(f"Existing manifest found — accumulating state.")
except Exception:
    print("No existing manifest — creating fresh.")

manifest = {}
for table in TABLES:
    fp       = fingerprints[table]
    tbl_key  = table.upper()
    existing = existing_manifest.get(tbl_key, {})

    schema_hist = dict(existing.get("schemaHistory", {}))
    if fp not in schema_hist:
        schema_hist[fp] = RUN_TS

    prev_count  = int(existing.get("totalProcessedRecordsCount", "0"))
    total_count = prev_count + row_counts[table]

    manifest[tbl_key] = {
        "dataFilespath"               : f"CC/{tbl_key}",
        "lastSuccessfulWriteTimestamp" : RUN_TS,
        "schemaHistory"               : schema_hist,
        "totalProcessedRecordsCount"  : str(total_count),
    }

dbutils.fs.put(MANIFEST_PATH, json.dumps(manifest, indent=2), overwrite=True)
print(f"\nmanifest.json written to: {MANIFEST_PATH}")
print(json.dumps(manifest, indent=2))

# COMMAND ----------

# DBTITLE 1,Persist State to Unity Catalog
from pyspark.sql.types import BooleanType

state_schema = StructType([
    StructField("table_name",   StringType(), False),
    StructField("record_id",    StringType(), False),
    StructField("fingerprint",  StringType(), False),
    StructField("first_run_ts", StringType(), False),
    StructField("is_deleted",   BooleanType(), False),
])

# Build rows for newly inserted IDs
new_state_rows = []
for tbl in TABLES:
    fp = fingerprints[tbl]
    for rid in new_ids_map[tbl]:
        new_state_rows.append((tbl, rid, fp, RUN_TS, False))

if new_state_rows:
    new_df = spark.createDataFrame(new_state_rows, schema=state_schema)
    new_df.write.format("delta").mode("append").saveAsTable(STATE_TABLE)
    print(f"Inserted {len(new_state_rows)} new IDs into {STATE_TABLE}")

# Mark deleted IDs
del_count = 0
for tbl in TABLES:
    ids = del_ids_map[tbl]
    if ids:
        id_list = ", ".join(f"'{i}'" for i in ids)
        spark.sql(f"""
            UPDATE {STATE_TABLE}
            SET is_deleted = true
            WHERE table_name = '{tbl}' AND record_id IN ({id_list})
        """)
        del_count += len(ids)

if del_count:
    print(f"Marked {del_count} IDs as deleted in {STATE_TABLE}")

# Summary
summary = spark.sql(f"""
    SELECT table_name,
           COUNT(*) FILTER (WHERE is_deleted = false) AS active_ids,
           COUNT(*) FILTER (WHERE is_deleted = true)  AS deleted_ids
    FROM {STATE_TABLE}
    GROUP BY table_name ORDER BY table_name
""")
print(f"\nState table summary ({STATE_TABLE}):")
summary.show()

# COMMAND ----------

# DBTITLE 1,Verify ADLS Contents
sep = "=" * 65
print(sep)
print(f"ADLS contents under: {BASE_PATH}")
print(sep)

def list_recursive(path, indent=0):
    try:
        for item in dbutils.fs.ls(path):
            size_str = f"  ({item.size:,} bytes)" if item.size > 0 else ""
            print(f"{'  ' * indent}{item.name}{size_str}")
            if item.isDir():
                list_recursive(item.path, indent + 1)
    except Exception as e:
        print(f"{'  ' * indent}[error: {e}]")

list_recursive(BASE_PATH)
print(sep)
print("Done. Data ready for pipeline ingestion.")
print("Run this notebook again to generate a new CDC cycle (UPDATEs + DELETEs on existing IDs).")
print(sep)