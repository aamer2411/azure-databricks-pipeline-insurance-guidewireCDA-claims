-- =============================================================================
-- REFINED LAYER — Reference DDL for Databricks Delta Tables
-- =============================================================================
--
-- PURPOSE
--   These DDL statements document the schema of the 2 external Delta tables
--   in the refined layer of the GW CDA Claims practice pipeline.
--
-- IMPORTANT NOTES
--   - These are REFERENCE DDL files only.
--     The actual tables are created by notebook 12 ("Load Replica to Refined -
--     SCD Type 2") on first run when the table does not yet exist in Unity Catalog.
--     You do NOT need to run these statements manually.
--
--   - Both tables are created as EXTERNAL Delta tables (USING DELTA LOCATION).
--     Unity Catalog holds only the metadata pointer (table definition + location).
--     Dropping the UC table (DROP TABLE) does NOT delete the underlying ADLS files.
--     The Delta files persist at their ADLS location and can be re-registered.
--
--   - claim_detail:
--       Driving table: cc_claim (replica layer)
--       Joined to:     cc_policy on cc_claim.policyid = cc_policy.id
--       Grain:         1 claim per SCD2 version (each change to a claim creates a new row)
--
--   - claim_financial:
--       Driving table: cc_transaction (replica layer)
--       Joined to:     cc_claim on cc_transaction.claimid = cc_claim.id
--       Grain:         1 transaction per SCD2 version (each change to a transaction creates a new row)
--
--   - ADLS base path:
--       abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/refined/claims/
--
--   - SCD2 columns (ETL_ActiveRow_Flag, ETL_RecordEffective_Date, ETL_RecordExpiry_Date,
--     ETL_Ins_Cyc_SK, ETL_Lst_Updt_Cyc_Sk) are managed entirely by notebook 12.
--
-- =============================================================================

-- =============================================================================
-- 1. claim_detail
--    External Delta table - one row per claim version (SCD Type 2)
--    Location: abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/refined/claims/claim_detail/
-- =============================================================================
CREATE TABLE IF NOT EXISTS insurance_claims_domain.refined.claim_detail (
    claim_detail_sk          INT,
    ETL_Key_Hash             STRING,
    ETL_SCD2_Hash            STRING,
    id                       STRING,
    claim_publicid           STRING,
    claimnumber              STRING,
    policyid                 STRING,
    lossdate                 TIMESTAMP,
    reporteddate             TIMESTAMP,
    closeddate               TIMESTAMP,
    losscause                INT,
    losstype                 STRING,
    lossstate                STRING,
    catastropheid            STRING,
    assignedgroupid          STRING,
    assigneduserid           STRING,
    claim_createtime         TIMESTAMP,
    claim_updatetime         TIMESTAMP,
    policynumber             STRING,
    policytype               STRING,
    policy_effectivedate     TIMESTAMP,
    policy_expirationdate    TIMESTAMP,
    policy_status            STRING,
    premium                  DOUBLE,
    insuranceline            STRING,
    cyc_run_sk               INT,
    Soft_Delete              STRING,
    GW_FINGERPRINT           STRING,
    GW_TIMESTAMP             STRING,
    ABC_AUDIT_DATE_TIME      TIMESTAMP,
    ETL_ActiveRow_Flag       STRING,
    ETL_RecordEffective_Date TIMESTAMP,
    ETL_RecordExpiry_Date    TIMESTAMP,
    ETL_Created_Date         TIMESTAMP,
    ETL_Updated_Date         TIMESTAMP,
    ETL_Lst_Updt_Cyc_Sk      STRING,
    ETL_Ins_Cyc_SK           STRING
)
USING DELTA
LOCATION 'abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/refined/claims/claim_detail/';

-- =============================================================================
-- 2. claim_financial
--    External Delta table - one row per transaction version (SCD Type 2)
--    Location: abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/refined/claims/claim_financial/
-- =============================================================================
CREATE TABLE IF NOT EXISTS insurance_claims_domain.refined.claim_financial (
    claim_financial_sk       INT,
    ETL_Key_Hash             STRING,
    ETL_SCD2_Hash            STRING,
    id                       STRING,
    transaction_publicid     STRING,
    claimid                  STRING,
    amount                   DOUBLE,
    currency                 STRING,
    transactiondate          TIMESTAMP,
    transaction_status       STRING,
    costtype                 STRING,
    costcategory             STRING,
    reserveline              STRING,
    transaction_createtime   TIMESTAMP,
    transaction_updatetime   TIMESTAMP,
    claimnumber              STRING,
    lossdate                 TIMESTAMP,
    losstype                 STRING,
    lossstate                STRING,
    cyc_run_sk               INT,
    Soft_Delete              STRING,
    GW_FINGERPRINT           STRING,
    GW_TIMESTAMP             STRING,
    ABC_AUDIT_DATE_TIME      TIMESTAMP,
    ETL_ActiveRow_Flag       STRING,
    ETL_RecordEffective_Date TIMESTAMP,
    ETL_RecordExpiry_Date    TIMESTAMP,
    ETL_Created_Date         TIMESTAMP,
    ETL_Updated_Date         TIMESTAMP,
    ETL_Lst_Updt_Cyc_Sk      STRING,
    ETL_Ins_Cyc_SK           STRING
)
USING DELTA
LOCATION 'abfss://insurance-claims-domain@azurepractice68256.dfs.core.windows.net/refined/claims/claim_financial/';
