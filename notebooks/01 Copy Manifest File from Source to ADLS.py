# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 Copy Manifest File from Source to ADLS
# MAGIC
# MAGIC **Pipeline:** GW CDA Claims (Practice) | **Manifest Step:** 1 of 4
# MAGIC
# MAGIC **Purpose:** Receive ADF pipeline parameters and copy manifest.json from the source landing zone into the ADLS raw path so downstream manifest notebooks read it from a consistent location.
# MAGIC
# MAGIC | | Production | This Project |
# MAGIC |--|-----------|-------------|
# MAGIC | Source | AWS S3 bucket | ADLS client_data/ (written by s3_data_generator) |
# MAGIC | Copy | s3://... -> abfss://...raw/claims/ | abfss://...client_data/ -> abfss://...raw/claims/ |
# MAGIC | Credentials | S3 keys from Key Vault | Not needed -- ADLS to ADLS copy |
# MAGIC | Azure SQL | Not used in this notebook | Same -- Not used in this notebook |

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters
# ADF passes these parameters to the notebook at runtime.
# Production approach: identical widget pattern, same parameter names.
#
# Parameters to create in the ADF pipeline:
#   Name               Type     Example value
#   ---------------------------------------------------------------------
#   ADLS_RAW_PATH      String   abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/raw
#   TYPE_DATA          String   claims
#   CLIENT_DATA_PATH   String   abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/client_data
#   ---------------------------------------------------------------------

dbutils.widgets.text("ADLS_RAW_PATH", "")
ADLS_RAW_PATH = dbutils.widgets.get("ADLS_RAW_PATH")

dbutils.widgets.text("TYPE_DATA", "")
TYPE_DATA = dbutils.widgets.get("TYPE_DATA")

# Production: ext_s3_path -- prefix path inside the S3 bucket
# This project: CLIENT_DATA_PATH -- ADLS client_data/ (our simulated S3)
dbutils.widgets.text("CLIENT_DATA_PATH", "")
CLIENT_DATA_PATH = dbutils.widgets.get("CLIENT_DATA_PATH")

print(f"ADLS_RAW_PATH    : {ADLS_RAW_PATH}")
print(f"TYPE_DATA        : {TYPE_DATA}")
print(f"CLIENT_DATA_PATH : {CLIENT_DATA_PATH}")

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

# DBTITLE 1,Copy manifest.json from Source to ADLS Raw
# Production approach:
#   S3 access + secret keys fetched from Key Vault.
#   Secret key is URL-encoded (/ -> %2F) to handle special chars in the URL.
#   dbutils.fs.cp(f"s3://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{BUCKET}/{ext_s3_path}/manifest.json",
#                 f"{ADLS_RAW_PATH}/{TYPE_DATA}/manifest.json", True)
#
# This project:
#   No S3 -- manifest.json already lives in ADLS client_data/ written by s3_data_generator.
#   Here we do a simple ADLS-to-ADLS copy (instead of S3 to ADLS), no credentials needed.
#   Destination includes the filename explicitly to avoid ADLS creating a file named
#   after the folder (happens when destination directory does not yet exist).

src  = f"{CLIENT_DATA_PATH}/manifest.json"
dest = f"{ADLS_RAW_PATH}/{TYPE_DATA}/manifest.json"

dbutils.fs.cp(src, dest, True)

print(f"manifest.json copied successfully")
print(f"  FROM : {src}")
print(f"  TO   : {dest}")