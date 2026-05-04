-- =============================================================================
-- File  : abc_tables_ddl.sql
-- DB    : insurance_claims_domain  (azurepractice68256.database.windows.net)
-- Schema: abc
-- Desc  : DDL for all abc schema tables + seed data.
--         Run this once on a fresh DB or re-run safely at any time
--         (all statements are IF NOT EXISTS guarded).
--
-- Run order:
--   1. Schema
--   2. CYC_CTRL_TBL   + seed
--   3. CYC_RUN_TBL
--   4. STEP_CTRL_TBL  + seed
--   5. STEP_RUN_TBL
--   6. JOB_CTRL_TBL   + seed
--   7. CDA_FILE_LOADS
--   8. MANIFEST
--   9. TABLE_LOAD_METADATA + seed
--  10. TABLES_TO_LOAD       -- staging table for ForEach input in Src->Raw
--  11. RAW_TABLE_LOAD        -- one row per parquet file; cleared on fresh run, updated as files are copied
--  12. JOB_RUN_TBL           -- one row per job execution; populated by PROC_UPDATE_JOB_START/END
--  13. TABLES_TO_LOAD_RPL    -- staging table for ForEach input in Raw->Replica
--  14. VALIDATION_CTRL_TBL   -- one row per validation rule; status reset to C at cycle end
--  15. BALANCE_METRICS        -- one row per metric per cycle run; written by notebook 10
--  16. VALIDATION_ERROR_LOG  -- one row per failed validation rule; written by notebook 11
--  17. VALIDATION_RUN_TBL    -- one row per validation rule execution; written by PROC_VALIDATION_START
--  18. TABLES_TO_LOAD_RFN    -- staging table for ForEach input in Replica->Refined
--  19. JOB_PARM_TBL          -- TODO: add when Replica->Refined notebook is built
-- =============================================================================


-- =============================================================================
-- 1. SCHEMA
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'abc')
    EXEC('CREATE SCHEMA abc');
GO


-- =============================================================================
-- 2. CYC_CTRL_TBL
-- One row per pipeline cycle. Never grows — stored procs UPDATE the same row.
-- CYC_STS_CD: C=Complete/idle, I=In-Progress, F=Failed
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'CYC_CTRL_TBL')
BEGIN
    CREATE TABLE abc.CYC_CTRL_TBL (
        CYC_SK         INT            NOT NULL PRIMARY KEY,
        CYC_NM         VARCHAR(100)   NOT NULL,
        CYC_DESC       VARCHAR(1000)  NULL,
        CYC_CUR_RUN_SK INT            NULL,
        CYC_STS_CD     VARCHAR(10)    NOT NULL DEFAULT 'C',
        FREQ_CD        VARCHAR(50)    NULL,
        ACTI_IND       VARCHAR(1)     NOT NULL DEFAULT 'Y',
        AUD_DT_TM      DATETIME2      NOT NULL DEFAULT SYSDATETIME()
    );
END
GO

-- Seed rows (one-time — real production values)
IF NOT EXISTS (SELECT 1 FROM abc.CYC_CTRL_TBL)
BEGIN
    INSERT INTO abc.CYC_CTRL_TBL (CYC_SK, CYC_NM, CYC_DESC, CYC_CUR_RUN_SK, CYC_STS_CD, FREQ_CD, ACTI_IND, AUD_DT_TM)
    VALUES
    (101, 'GW CDA',        'Loads Guidewire CDA Data For Claims',              12782, 'C', 'hourly',  'Y', '2026-02-10 05:37:50.460000'),
    (102, 'GW CDA',        'Loads Refined Guidewire CDA Data For Claims',       2933, 'C', 'daily',   'Y', '2025-01-10 03:09:01.243333'),
    (103, 'GW CDA',        'Loads Guidewire CDA Data For ContactManager',      12783, 'C', 'hourly',  'Y', '2026-02-10 05:14:30.430000'),
    (201, 'DuckCreek',     'Loads DuckCreek Data For Policy',                  12784, 'C', 'daily',   'Y', '2026-02-10 06:54:35.870000'),
    (301, 'DuckCreek',     'Loads DuckCreek Data For Billing',                 12785, 'C', 'daily',   'Y', '2026-02-10 07:47:58.753333'),
    (401, 'Network Drive', 'Loads Network Drive Data For Citizens PERS',       11867, 'C', 'monthly', 'Y', '2026-01-13 20:21:06.406666'),
    (402, 'Network Drive', 'Loads Network Drive Data For Citizens CRES',       12781, 'C', 'monthly', 'Y', '2026-02-10 02:27:16.010000'),
    (403, 'Network Drive', 'Loads Network Drive Data For Citizens Financial',  12780, 'F', 'monthly', 'Y', '2026-02-10 00:12:07.863333'),
    (404, 'IPX_StJohns',   'Loads IPX_StJohns Data For Policy',               12786, 'C', 'daily',   'Y', '2026-02-10 09:31:15.856666'),
    (405, 'IPX_StJohns',   'Loads IPX_StJohns Data For Billing',              12787, 'C', 'daily',   'Y', '2026-02-10 10:28:07.040000');
END
GO


-- =============================================================================
-- 3. CYC_RUN_TBL
-- One row per cycle execution — full run history. Grows every run.
-- CYC_STS_CD: I=In-Progress, S=Success, F=Failed
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'CYC_RUN_TBL')
BEGIN
    CREATE TABLE abc.CYC_RUN_TBL (
        CYC_RUN_SK      INT            NOT NULL PRIMARY KEY,
        CYC_SK          INT            NOT NULL,
        CYC_STRTDT_TM   DATETIME2      NULL,
        CYC_ENDDT_TM    DATETIME2      NULL,
        CYC_STS_CD      VARCHAR(10)    NOT NULL DEFAULT 'I',
        AUD_DT_TM       DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        PL_RUN_ID       VARCHAR(100)   NULL
    );
END
GO


-- =============================================================================
-- 4. STEP_CTRL_TBL
-- One row per step per cycle. Never grows — stored procs UPDATE the same row.
-- STEP_STS_CD: C=Complete/idle, I=In-Progress, S=Success, F=Failed
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'STEP_CTRL_TBL')
BEGIN
    CREATE TABLE abc.STEP_CTRL_TBL (
        STEP_SK          INT            NOT NULL PRIMARY KEY,
        STEP_NM          VARCHAR(100)   NOT NULL,
        CYC_SK           INT            NOT NULL,
        STEP_STS_CD      VARCHAR(10)    NOT NULL DEFAULT 'C',
        STEP_CUR_RUN_SK  INT            NULL,
        ACTI_IND         VARCHAR(1)     NOT NULL DEFAULT 'Y',
        AUD_DT_TM        DATETIME2      NOT NULL DEFAULT SYSDATETIME()
    );
END
GO

-- Seed rows (one-time — real production values)
IF NOT EXISTS (SELECT 1 FROM abc.STEP_CTRL_TBL)
BEGIN
    INSERT INTO abc.STEP_CTRL_TBL (STEP_SK, STEP_NM, CYC_SK, STEP_STS_CD, STEP_CUR_RUN_SK, ACTI_IND, AUD_DT_TM)
    VALUES
    (10110, 'GW_CDA_SRC_RAW',           101, 'C', 30726, 'Y', '2026-02-10 05:37:50.460000'),
    (10120, 'GW_CDA_RAW_RPL',           101, 'C', 30728, 'Y', '2026-02-10 05:37:50.460000'),
    (10130, 'GW_CDA_RPL_RFN',           101, 'C', 30729, 'Y', '2026-02-10 05:37:50.460000'),
    (10210, 'GW_CDA_RPL_RFN',           102, 'C',  5724, 'Y', '2025-01-10 03:09:01.243333'),
    (10310, 'GW_CDA_SRC_RAW_CM',        103, 'C', 30725, 'Y', '2026-02-10 05:14:30.426666'),
    (10320, 'GW_CDA_RAW_RPL_CM',        103, 'C', 30727, 'Y', '2026-02-10 05:14:30.426666'),
    (20110, 'DC_SRC_RAW_POLICY',        201, 'C', 30730, 'Y', '2026-02-10 06:54:35.870000'),
    (20120, 'DC_RAW_PRC_POLICY',        201, 'C', 30731, 'Y', '2026-02-10 06:54:35.870000'),
    (20130, 'DC_PRC_RFN_POLICY',        201, 'C', 30732, 'Y', '2026-02-10 06:54:35.870000'),
    (30110, 'DC_SRC_RAW_BILLING',       301, 'C', 30733, 'Y', '2026-02-10 07:47:58.753333'),
    (30120, 'DC_RAW_RPL_BILLING',       301, 'C', 30734, 'Y', '2026-02-10 07:47:58.753333'),
    (30130, 'DC_PRC_RFN_BILLING',       301, 'C', 30735, 'Y', '2026-02-10 07:47:58.753333'),
    (40110, 'ND_SRC_RAW_CITIZENS_PERS', 401, 'C', 28383, 'Y', '2026-01-13 20:21:06.336666'),
    (40120, 'ND_RAW_PRC_CITIZENS_PERS', 401, 'C', 28397, 'Y', '2026-01-13 20:21:06.336666'),
    (40130, 'ND_PRC_RFN_CITIZENS_PERS', 401, 'C', 28402, 'Y', '2026-01-13 20:21:06.336666');
END
GO


-- =============================================================================
-- 5. STEP_RUN_TBL
-- One row per step execution — full run history. Grows every run.
-- STEP_STS_CD: I=In-Progress, S=Success, F=Failed
-- CYC_RUN_SK copied from CYC_CTRL_TBL at step start for traceability.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'STEP_RUN_TBL')
BEGIN
    CREATE TABLE abc.STEP_RUN_TBL (
        STEP_RUN_SK     INT            NOT NULL PRIMARY KEY,   -- MAX+1 generated by proc
        CYC_RUN_SK      INT            NOT NULL,               -- CYC_CUR_RUN_SK from CYC_CTRL_TBL
        STEP_SK         INT            NOT NULL,               -- FK to STEP_CTRL_TBL
        STEP_STS_CD     VARCHAR(10)    NOT NULL DEFAULT 'I',   -- I/S/F
        STEP_STRTDT_TM  DATETIME2      NULL,                   -- set when step starts
        STEP_ENDDT_TM   DATETIME2      NULL,                   -- set when step ends
        AUD_DT_TM       DATETIME2      NOT NULL DEFAULT SYSDATETIME()
    );
END
GO


-- =============================================================================
-- 6. JOB_CTRL_TBL
-- One row per table per step. Never grows — stored procs UPDATE the same row.
-- JOB_TYP_CD:  RAW=Src→Raw, RPL=Raw→Replica, RFN=Replica→Refined
-- JOB_STS_CD:  C=Complete/idle, I=In-Progress, S=Success, F=Failed
-- CUR_CYC_VALID: Y=table had new data last cycle, N=skipped (no new files)
-- NOTEBOOK_PATH: only populated for RFN step (each refined table has its own notebook)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'JOB_CTRL_TBL')
BEGIN
    CREATE TABLE abc.JOB_CTRL_TBL (
        JOB_SK           INT            NOT NULL PRIMARY KEY,
        JOB_NM           VARCHAR(100)   NOT NULL,
        STEP_SK          INT            NOT NULL,
        JOB_DESC         VARCHAR(500)   NULL,
        JOB_TYP_CD       VARCHAR(10)    NOT NULL,
        SRC_PATH_SCHEM   VARCHAR(200)   NULL,
        TGT_PATH_SCHEM   VARCHAR(200)   NULL,
        SRC_FILE_TBL     VARCHAR(200)   NULL,
        TGT_FILE_TBL     VARCHAR(200)   NULL,
        JOB_STS_CD       VARCHAR(10)    NOT NULL DEFAULT 'C',
        ACTI_IND         VARCHAR(1)     NOT NULL DEFAULT 'Y',
        JOB_CUR_RUN_SK   INT            NULL,
        AUD_DT_TM        DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        SRC_NM           VARCHAR(100)   NULL,
        NOTEBOOK_PATH    VARCHAR(500)   NULL,
        CUR_CYC_VALID    VARCHAR(1)     NOT NULL DEFAULT 'Y'
    );
END
GO

-- Seed rows — 5 tables × 2 steps (Src→Raw and Raw→Replica)
-- NOTEBOOK_PATH left blank for RAW and RPL steps (single parameterised notebook driven by ADF)
-- RFN step jobs added here when refined tables are designed
IF NOT EXISTS (SELECT 1 FROM abc.JOB_CTRL_TBL)
BEGIN
    INSERT INTO abc.JOB_CTRL_TBL
        (JOB_SK, JOB_NM, STEP_SK, JOB_DESC, JOB_TYP_CD,
         SRC_PATH_SCHEM, TGT_PATH_SCHEM, SRC_FILE_TBL, TGT_FILE_TBL,
         JOB_STS_CD, ACTI_IND, JOB_CUR_RUN_SK, AUD_DT_TM, SRC_NM, NOTEBOOK_PATH, CUR_CYC_VALID)
    VALUES
    -- STEP 10110: Src → Raw (5 tables)
    (10110001, 'gw_cda_stg_raw_CC', 10110, 'gw_cda_stg_raw for the table CC_POLICY',      'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'cc_policy',      'cc_policy',      'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10110002, 'gw_cda_stg_raw_CC', 10110, 'gw_cda_stg_raw for the table CC_CLAIM',       'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'cc_claim',       'cc_claim',       'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10110003, 'gw_cda_stg_raw_CC', 10110, 'gw_cda_stg_raw for the table CC_EXPOSURE',    'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'cc_exposure',    'cc_exposure',    'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10110004, 'gw_cda_stg_raw_CC', 10110, 'gw_cda_stg_raw for the table CC_CONTACT',     'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'cc_contact',     'cc_contact',     'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10110005, 'gw_cda_stg_raw_CC', 10110, 'gw_cda_stg_raw for the table CC_TRANSACTION', 'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'cc_transaction', 'cc_transaction', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),

    -- STEP 10120: Raw → Replica (5 tables)
    (10120001, 'gw_cda_raw_rpl_CC', 10120, 'gw_cda_raw_rpl for the table CC_POLICY',      'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'cc_policy',      'cc_policy',      'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10120002, 'gw_cda_raw_rpl_CC', 10120, 'gw_cda_raw_rpl for the table CC_CLAIM',       'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'cc_claim',       'cc_claim',       'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10120003, 'gw_cda_raw_rpl_CC', 10120, 'gw_cda_raw_rpl for the table CC_EXPOSURE',    'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'cc_exposure',    'cc_exposure',    'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10120004, 'gw_cda_raw_rpl_CC', 10120, 'gw_cda_raw_rpl for the table CC_CONTACT',     'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'cc_contact',     'cc_contact',     'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10120005, 'gw_cda_raw_rpl_CC', 10120, 'gw_cda_raw_rpl for the table CC_TRANSACTION', 'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'cc_transaction', 'cc_transaction', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    -- STEP 10130: Replica→Refined for CYC_SK=101 (Claims)
    (10130001, 'gw_cda_rpl_rfn_CC', 10130, 'gw_cda_rpl_rfn for the table claim_detail',   'RFN', 'replica', 'refined', NULL, 'claim_detail',   'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10130002, 'gw_cda_rpl_rfn_CC', 10130, 'gw_cda_rpl_rfn for the table claim_financial', 'RFN', 'replica', 'refined', NULL, 'claim_financial', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),

    -- STEP 10310: Src→Raw ContactManager (dummy)
    (10310001, 'gw_cda_stg_raw_CM', 10310, 'gw_cda_stg_raw_cm for the table dummy_cm_table_a', 'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'dummy_cm_table_a', 'dummy_cm_table_a', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10310002, 'gw_cda_stg_raw_CM', 10310, 'gw_cda_stg_raw_cm for the table dummy_cm_table_b', 'RAW', 'GW_CDA_RAW.GW_CDA_STAGE', 'GW_CDA_RAW', 'dummy_cm_table_b', 'dummy_cm_table_b', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),

    -- STEP 10320: Raw→Replica ContactManager (dummy)
    (10320001, 'gw_cda_raw_rpl_CM', 10320, 'gw_cda_raw_rpl_cm for the table dummy_cm_table_a', 'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'dummy_cm_table_a', 'dummy_cm_table_a', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y'),
    (10320002, 'gw_cda_raw_rpl_CM', 10320, 'gw_cda_raw_rpl_cm for the table dummy_cm_table_b', 'RPL', 'GW_CDA_RAW', 'GW_CDA_RPL', 'dummy_cm_table_b', 'dummy_cm_table_b', 'C', 'Y', NULL, '2026-03-15 00:00:00.000000', 'GW_CDA', NULL, 'Y');
END
GO

-- 14. JOB_PARM_TBL   -- TODO: add when Replica->Refined step is built


-- =============================================================================
-- 7. CDA_FILE_LOADS
-- One row per parquet file discovered in the source (S3 / ADLS client_data/).
-- Populated by manifest notebook Part 3 after Part 2 lists the files.
-- FILE_LOAD_ID: IDENTITY auto-increment -- gaps are normal (inserts across runs).
-- IS_LOADED: 0 = file discovered but not yet copied to raw; 1 = copied.
-- IS_FINGERPRINT_UPDATED: 1 = schema fingerprint changed vs previous run.
-- Starts empty -- no seed data. First pipeline run inserts one row per file found.
-- Production has 400,000+ rows accumulated over many runs.
-- Our project: 5 rows per full load (one per table), grows with each incremental run.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'CDA_FILE_LOADS')
BEGIN
    CREATE TABLE abc.CDA_FILE_LOADS (
        FILE_LOAD_ID              INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
        TABLE_NAME                VARCHAR(100)   NOT NULL,               -- lowercase table name e.g. cc_claim
        GW_FINGERPRINT            VARCHAR(32)    NOT NULL,               -- 32-char MD5 schema fingerprint
        GW_TIMESTAMP              VARCHAR(50)    NOT NULL,               -- Unix epoch ms of the data folder
        CLOUD_PATH                VARCHAR(500)   NOT NULL,               -- relative path: table/fingerprint/timestamp
        IS_LOADED                 BIT            NOT NULL DEFAULT 0,     -- 0 = pending, 1 = loaded to raw
        INSERT_DATE_TIME          DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        UPDATE_DATE_TIME          DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        IS_FINGERPRINT_UPDATED    BIT            NOT NULL DEFAULT 0,     -- 1 = schema changed vs previous run
        XCENTER                   VARCHAR(10)    NULL,                   -- e.g. CC
        CYC_RUN_SK                INT            NULL                    -- FK (logical) to CYC_RUN_TBL
    );
END
GO


-- =============================================================================
-- 8. MANIFEST
-- Populated at runtime by manifest notebook Part 1 (load_manifest function).
-- One row per table per cycle run. Rows accumulate across cycles.
-- Before each run DELETE_MANIFEST_DATA proc deletes rows for the current
-- XCENTER, then Part 1 re-inserts fresh rows from manifest.json.
-- Starts empty — no manual seed needed. First pipeline run writes 5 rows
-- (one per practice table). Production has 1,029 rows (813 tables x cycles).
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'MANIFEST')
BEGIN
    CREATE TABLE abc.MANIFEST (
        DATAFILESPATH                    VARCHAR(500)   NULL,     -- relative path e.g. CC/CC_CLAIM
        LASTSUCCESSFULWRITETIMESTAMP     VARCHAR(50)    NULL,     -- Unix epoch ms of latest delivery
        SCHEMAHISTORY                    VARCHAR(2000)  NULL,     -- stringified dict {fingerprint: first_seen_ts}
        TOTALPROCESSEDRECORDSCOUNT       VARCHAR(50)    NULL,     -- cumulative record count as string
        XCENTER                          VARCHAR(10)    NULL,     -- e.g. CC
        AUDIT_DT_TM                      DATETIME2      NULL,     -- when this row was written
        CYC_RUN_SK                       INT            NULL      -- FK to CYC_RUN_TBL
    );
END
GO


-- =============================================================================
-- 9. TABLE_LOAD_METADATA
-- One row per table — static set, never grows.
-- Tracks latest and previous fingerprint + timestamp per table.
-- Used by Src->Raw step to determine which new timestamp folders to load.
-- TABLE_ID: seeded once at setup (production starts at 4295 because all 813
--   tables were seeded at go-live; our project uses 1-5 for 5 tables).
-- CYC_RUN_SK: NULL at seed time — updated by GET_GW_FILE_METADATA_MASTER
--   on every run to reflect the cycle when that table last had new data.
-- Timestamps/fingerprints: NULL at seed — populated on first pipeline run.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLE_LOAD_METADATA')
BEGIN
    CREATE TABLE abc.TABLE_LOAD_METADATA (
        TABLE_ID                              INT            NOT NULL PRIMARY KEY,
        TABLE_NAME                            VARCHAR(100)   NOT NULL,
        LATEST_LASTSUCCESSFULWRITETIMESTAMP   VARCHAR(50)    NULL,
        PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP VARCHAR(50)    NULL,
        LATEST_FINGERPRINT                    VARCHAR(32)    NULL,
        PREVIOUS_FINGERPRINT                  VARCHAR(32)    NULL,
        LOAD_STATUS                           VARCHAR(1)     NOT NULL DEFAULT 'C',
        INSERT_DATE_TIME                      DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        UPDATE_DATE_TIME                      DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        XCENTER                               VARCHAR(10)    NULL,
        CYC_RUN_SK                            INT            NULL
    );
END
GO

-- Seed rows -- one per practice table.
-- Timestamps and fingerprints are NULL initially; GET_GW_FILE_METADATA_MASTER
-- populates them on the first pipeline run.
IF NOT EXISTS (SELECT 1 FROM abc.TABLE_LOAD_METADATA)
BEGIN
    INSERT INTO abc.TABLE_LOAD_METADATA
        (TABLE_ID, TABLE_NAME, LATEST_LASTSUCCESSFULWRITETIMESTAMP, PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP,
         LATEST_FINGERPRINT, PREVIOUS_FINGERPRINT, LOAD_STATUS, INSERT_DATE_TIME, UPDATE_DATE_TIME, XCENTER, CYC_RUN_SK)
    VALUES
    (1, 'CC_POLICY',      NULL, NULL, NULL, NULL, 'C', '2026-03-15 00:00:00', '2026-03-15 00:00:00', 'CC', NULL),
    (2, 'CC_CLAIM',       NULL, NULL, NULL, NULL, 'C', '2026-03-15 00:00:00', '2026-03-15 00:00:00', 'CC', NULL),
    (3, 'CC_EXPOSURE',    NULL, NULL, NULL, NULL, 'C', '2026-03-15 00:00:00', '2026-03-15 00:00:00', 'CC', NULL),
    (4, 'CC_CONTACT',     NULL, NULL, NULL, NULL, 'C', '2026-03-15 00:00:00', '2026-03-15 00:00:00', 'CC', NULL),
    (5, 'CC_TRANSACTION', NULL, NULL, NULL, NULL, 'C', '2026-03-15 00:00:00', '2026-03-15 00:00:00', 'CC', NULL);
END
GO


-- =============================================================================
-- 10. TABLES_TO_LOAD  (added before RAW_TABLE_LOAD — both used in Src->Raw step)
-- Staging table populated by PROC_WRAPPER_SRC_RAW before ForEach in Src->Raw.
-- ADF does a Lookup on this table; ForEach iterates one row per table.
-- Cleared and re-populated each pipeline run — no seed data needed.
-- No PK: this is a disposable staging table, not a control table.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLES_TO_LOAD')
BEGIN
    CREATE TABLE abc.TABLES_TO_LOAD (
        TABLE_NAME        VARCHAR(100)  NOT NULL,
        START_TIMESTAMP   BIGINT        NOT NULL,   -- MIN(GW_TIMESTAMP) across pending files for this table (epoch ms)
        END_TIMESTAMP     BIGINT        NOT NULL,   -- MAX(GW_TIMESTAMP) across pending files for this table (epoch ms)
        STEP_SK           INT           NOT NULL,
        JOB_SK            INT           NOT NULL,
        XCENTER           VARCHAR(10)   NOT NULL
    );
END
GO


-- =============================================================================
-- 11. RAW_TABLE_LOAD
-- One row per parquet file discovered this cycle (mirrors production table name).
-- Cleared by ADF script activity at the start of each fresh run (Truncate=Y).
-- IS_LOADED updated to 1 as each file is copied to raw/ by the Src->Raw notebook.
-- Starts empty — no seed data.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'RAW_TABLE_LOAD')
BEGIN
    CREATE TABLE abc.RAW_TABLE_LOAD (
        FILE_LOAD_ID    INT            NOT NULL IDENTITY(1,1) PRIMARY KEY,
        TABLE_NAME      VARCHAR(100)   NOT NULL,               -- uppercase table name e.g. CC_CLAIM
        GW_FINGERPRINT  VARCHAR(32)    NOT NULL,               -- 32-char MD5 schema fingerprint
        GW_TIMESTAMP    VARCHAR(50)    NOT NULL,               -- Unix epoch ms of the data folder
        CLOUD_PATH      VARCHAR(500)   NOT NULL,               -- relative path: table/fingerprint/timestamp
        IS_LOADED       BIT            NOT NULL DEFAULT 0,     -- 0 = pending, 1 = copied to raw
        XCENTER         VARCHAR(10)    NOT NULL                -- e.g. CC
    );
END
GO


-- =============================================================================
-- 12. JOB_RUN_TBL
-- One row per job execution (one table per run). Inserted by PROC_UPDATE_JOB_START
-- with status 'I' and NULL counts; updated by PROC_UPDATE_JOB_END with final
-- row counts and status S/F.
-- No seed data — starts empty, grows every pipeline run.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'JOB_RUN_TBL')
BEGIN
    CREATE TABLE abc.JOB_RUN_TBL (
        JOB_RUN_SK      INT            NOT NULL PRIMARY KEY,   -- MAX+1 generated by proc
        STEP_RUN_SK     INT            NOT NULL,               -- FK (logical) to STEP_RUN_TBL
        JOB_SK          INT            NOT NULL,               -- FK (logical) to JOB_CTRL_TBL
        JOB_STRTDT_TM   DATETIME2      NULL,                   -- set when job starts
        JOB_ENDDT_TM    DATETIME2      NULL,                   -- set when job ends
        JOB_STS_CD      VARCHAR(10)    NOT NULL DEFAULT 'I',   -- I=In-Progress, S=Success, F=Failed
        RCRD_READ_CNT   INT            NULL,                   -- rows read from source
        RCRD_LD_CNT     INT            NULL,                   -- rows loaded to target
        RCRD_UPD_CNT    INT            NULL,                   -- rows updated
        RCRD_INS_CNT    INT            NULL,                   -- rows inserted
        RCRD_DEL_CNT    INT            NULL,                   -- rows deleted
        DATA_READ       INT            NULL,                   -- data volume read
        DATA_WRITTEN    INT            NULL,                   -- data volume written
        ERR_MSG         VARCHAR(2000)  NULL,                   -- error message if JOB_STS_CD='F'
        AUD_DT_TM       DATETIME2      NOT NULL DEFAULT SYSDATETIME()
    );
END
GO


-- =============================================================================
-- 13. TABLES_TO_LOAD_RPL
-- Staging table populated by PROC_WRAPPER_RAW_RPL before ForEach in Raw->Replica.
-- ADF does a Lookup on this table; ForEach iterates one row per table.
-- Cleared and re-populated each pipeline run — no seed data needed.
-- No PK: disposable staging table, not a control table.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLES_TO_LOAD_RPL')
BEGIN
    CREATE TABLE abc.TABLES_TO_LOAD_RPL (
        TGT_FILE_TBL  VARCHAR(100)  NOT NULL,   -- lowercase table name e.g. cc_claim
        JOB_SK        INT           NOT NULL,
        STEP_SK       INT           NOT NULL,
        CYC_RUN_SK    INT           NOT NULL,   -- current cycle run SK from CYC_CTRL_TBL
        XCENTER       VARCHAR(10)   NOT NULL
    );
END
GO


-- =============================================================================
-- 14. VALIDATION_CTRL_TBL
-- One row per validation rule. Each rule defines a query to run against a
-- replica table to validate data quality after the Raw->Replica load.
-- STATUS reset to 'C' by PROC_UPDATE_CYC_END at the end of each cycle.
-- VAL_RUN_SK: updated each time the validation rule is executed.
-- Starts empty — seed with validation rules as needed.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'VALIDATION_CTRL_TBL')
BEGIN
    CREATE TABLE abc.VALIDATION_CTRL_TBL (
        VAL_SK          INT            NOT NULL PRIMARY KEY,
        VAL_RULE        VARCHAR(200)   NULL,                    -- validation rule name/description
        VAL_QUERY       VARCHAR(2000)  NULL,                    -- SQL query to execute for this rule
        PRIMARY_KEY     VARCHAR(100)   NULL,                    -- PK column of the table being validated
        TABLE_NAME      VARCHAR(100)   NULL,                    -- table this rule applies to
        XCENTER         VARCHAR(10)    NULL,                    -- e.g. CC
        REPLICA_JOB_SK  INT            NULL,                    -- FK (logical) to JOB_CTRL_TBL
        STATUS          VARCHAR(10)    NOT NULL DEFAULT 'C',    -- C=idle, I=In-Progress, S=Success, F=Failed
        ACTI_IND        VARCHAR(1)     NOT NULL DEFAULT 'Y',    -- Y=active, N=inactive
        VAL_RUN_SK      INT            NULL,                    -- current/last run SK
        AUD_DT_TM       DATETIME2      NOT NULL DEFAULT SYSDATETIME()
    );
END
GO


-- =============================================================================
-- 15. BALANCE_METRICS
-- One row per metric per cycle run. Written by notebook 10 (balance recon).
-- Starts empty — grows 5 rows per pipeline run (one per financial metric).
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'BALANCE_METRICS')
BEGIN
    CREATE TABLE abc.BALANCE_METRICS (
        Cyc_SK          INT             NOT NULL,               -- e.g. 101
        Curr_Cyc_Run_SK INT             NOT NULL,               -- from CYC_CTRL_TBL
        Metric_Type     VARCHAR(50)     NOT NULL,               -- ClaimCount / LossPayment / ExpensePayment / LossReserve / ExpenseReserve
        Process         VARCHAR(50)     NOT NULL,               -- Raw To Replica
        Source_Value    DECIMAL(18, 2)  NULL,                   -- value from raw parquet (source)
        Target_Value    DECIMAL(18, 2)  NULL,                   -- value from replica Delta table
        Status          VARCHAR(10)     NOT NULL,               -- PASSED / FAILED
        Aud_Date_Time   DATETIME2       NOT NULL DEFAULT SYSDATETIME()
    );
END
GO


-- =============================================================================
-- 16. VALIDATION_ERROR_LOG
-- One row per failed validation rule per cycle. Written by notebook 11.
-- Starts empty — only grows when a validation rule finds failing rows.
-- Column order matches production table.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'VALIDATION_ERROR_LOG')
BEGIN
    CREATE TABLE abc.VALIDATION_ERROR_LOG (
        GW_TABLE_NAME    VARCHAR(100)   NULL,               -- replica table that was validated
        GW_PRIMARY_KEY   VARCHAR(100)   NULL,               -- PK column name
        GW_ERROR_VALUES  VARCHAR(MAX)   NULL,               -- comma-separated PKs of failed rows
        VAL_SK           VARCHAR(10)    NULL,               -- validation rule SK
        VAL_RUN_SK       VARCHAR(10)    NULL,               -- validation run SK
        INSERTED_DATE    DATETIME2      NULL                -- when the error was logged
    );
END
GO


-- =============================================================================
-- 17. VALIDATION_RUN_TBL
-- One row per validation rule execution. Inserted by PROC_VALIDATION_START,
-- updated by PROC_VALIDATION_END. Starts empty — grows every time a validation
-- rule runs (currently zero runs as VALIDATION_CTRL_TBL has no rules yet).
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'VALIDATION_RUN_TBL')
BEGIN
    CREATE TABLE abc.VALIDATION_RUN_TBL (
        VAL_RUN_SK      INT             NOT NULL PRIMARY KEY,    -- MAX+1 generated by PROC_VALIDATION_START
        VAL_SK          INT             NOT NULL,                -- FK (logical) to VALIDATION_CTRL_TBL
        VAL_RULE        VARCHAR(255)    NULL,                    -- rule description
        TABLE_NAME      VARCHAR(255)    NULL,                    -- replica table being validated
        PRIMARY_KEY     VARCHAR(255)    NULL,                    -- PK column of the table
        XCENTER         VARCHAR(50)     NULL,                    -- e.g. CC
        STEP_SK         INT             NULL,                    -- step this validation belongs to
        CYC_RUN_SK      INT             NULL,                    -- cycle run SK
        VAL_STATUS      VARCHAR(10)     NOT NULL DEFAULT 'I',   -- I=In-Progress, S=Success, F=Failed
        VAL_START_DTM   DATETIME2       NULL,                    -- set by PROC_VALIDATION_START
        VAL_END_DTM     DATETIME2       NULL,                    -- set by PROC_VALIDATION_END
        TOTAL_COUNT     INT             NOT NULL DEFAULT 0,      -- total rows checked
        PASSED_COUNT    INT             NOT NULL DEFAULT 0,      -- rows that passed
        FAILED_COUNT    INT             NOT NULL DEFAULT 0,      -- rows that failed
        AUD_DT_TM       DATETIME2       NOT NULL DEFAULT SYSDATETIME()
    );
END
GO


-- =============================================================================
-- 18. TABLES_TO_LOAD_RFN
-- Staging table populated by PROC_WRAPPER_RPL_RFN before ForEach in Replica->Refined.
-- ADF does a Lookup on this table; ForEach iterates one row per refined table.
-- Cleared and re-populated each pipeline run — no seed data needed.
-- No PK: disposable staging table, not a control table.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLES_TO_LOAD_RFN')
BEGIN
    CREATE TABLE abc.TABLES_TO_LOAD_RFN (
        TGT_FILE_TBL    VARCHAR(100)  NOT NULL,   -- lowercase refined table name e.g. claim_detail
        TGT_PATH_SCHEM  VARCHAR(100)  NOT NULL,   -- target schema e.g. refined
        SRC_FILE_TBL    VARCHAR(100)  NULL,        -- source replica table (can be NULL for multi-source)
        JOB_SK          INT           NOT NULL,
        STEP_SK         INT           NOT NULL,
        SRC_PATH_SCHEM  VARCHAR(100)  NOT NULL,   -- source schema e.g. replica
        XCENTER         VARCHAR(10)   NOT NULL
    );
END

-- =============================================================================
-- 19. JOB_PARM_TBL
-- One row per parameter per job. Stores transformation query and SCD2 configuration
-- for the Replica->Refined step. Read by notebook 12 at runtime.
-- PARM_NM values: src_keys, SurrogateKey, partition, scdexclusions,
--                 Driving_Table, Driving_Table_PK, tfm_query
-- Seed data: 7 parameters for JOB_SK=10130001 (claim_detail)
--            7 parameters for JOB_SK=10130002 (claim_financial)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'JOB_PARM_TBL')
BEGIN
    CREATE TABLE abc.JOB_PARM_TBL (
        PARM_SK   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        JOB_SK    INT          NOT NULL,  -- FK (logical) to JOB_CTRL_TBL
        PARM_NM   VARCHAR(100) NOT NULL,  -- parameter name e.g. src_keys, tfm_query
        PARM_VAL  VARCHAR(MAX) NULL,      -- parameter value; may be a full SQL query for tfm_query
        ACTI_IND  VARCHAR(1)   NOT NULL DEFAULT 'Y',
        AUD_DT_TM DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
        PARM_ZONE VARCHAR(50)  NULL       -- zone identifier e.g. CC
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM abc.JOB_PARM_TBL)
BEGIN
    INSERT INTO abc.JOB_PARM_TBL (JOB_SK, PARM_NM, PARM_VAL, PARM_ZONE) VALUES
    -- ============================================================
    -- JOB_SK=10130001 : claim_detail
    -- Joins cc_claim (driving) + cc_policy. One row per claim.
    -- scdexclusions: delivery metadata + ETL SCD2 cols excluded from change detection.
    -- ============================================================
    (10130001, 'src_keys',       'id',                           'CC'),
    (10130001, 'SurrogateKey',   'claim_detail_sk',              'CC'),
    (10130001, 'partition',      '',                             'CC'),
    (10130001, 'scdexclusions',  'cyc_run_sk,soft_delete,gw_fingerprint,gw_timestamp,abc_audit_date_time,ETL_ActiveRow_Flag,ETL_RecordEffective_Date,ETL_RecordExpiry_Date,ETL_Created_Date,ETL_Updated_Date,ETL_Lst_Updt_Cyc_SK,ETL_Ins_Cyc_SK', 'CC'),
    (10130001, 'Driving_Table',    'cc_claim',                   'CC'),
    (10130001, 'Driving_Table_PK', 'id',                         'CC'),
    (10130001, 'tfm_query',
               'SELECT c.id, c.publicid AS claim_publicid, c.claimnumber, c.policyid, c.lossdate, c.reporteddate, c.closeddate, c.losscause, c.losstype, c.state AS lossstate, c.catastropheid, c.assignedgroupid, c.assigneduserid, c.createtime AS claim_createtime, c.updatetime AS claim_updatetime, p.policynumber, p.policytype, p.effectivedate AS policy_effectivedate, p.expirationdate AS policy_expirationdate, p.status AS policy_status, p.premium, p.insuranceline, c.CYC_RUN_SK AS cyc_run_sk, c.Soft_Delete, c.GW_FINGERPRINT, c.GW_TIMESTAMP, c.ABC_AUDIT_DATE_TIME FROM @catalog.@src_schema.cc_claim c LEFT JOIN @catalog.@src_schema.cc_policy p ON c.policyid = p.id WHERE c.Soft_Delete = ''N''',
               'CC'),
    -- ============================================================
    -- JOB_SK=10130002 : claim_financial
    -- Joins cc_transaction (driving) + cc_claim for claim context. One row per transaction.
    -- ============================================================
    (10130002, 'src_keys',       'id',                           'CC'),
    (10130002, 'SurrogateKey',   'claim_financial_sk',           'CC'),
    (10130002, 'partition',      '',                             'CC'),
    (10130002, 'scdexclusions',  'cyc_run_sk,soft_delete,gw_fingerprint,gw_timestamp,abc_audit_date_time,ETL_ActiveRow_Flag,ETL_RecordEffective_Date,ETL_RecordExpiry_Date,ETL_Created_Date,ETL_Updated_Date,ETL_Lst_Updt_Cyc_SK,ETL_Ins_Cyc_SK', 'CC'),
    (10130002, 'Driving_Table',    'cc_transaction',             'CC'),
    (10130002, 'Driving_Table_PK', 'id',                         'CC'),
    (10130002, 'tfm_query',
               'SELECT t.id, t.publicid AS transaction_publicid, t.claimid, t.amount, t.currency, t.transactiondate, t.status AS transaction_status, t.costtype, t.costcategory, t.reserveline, t.createtime AS transaction_createtime, t.updatetime AS transaction_updatetime, c.claimnumber, c.lossdate, c.losstype, c.state AS lossstate, t.CYC_RUN_SK AS cyc_run_sk, t.Soft_Delete, t.GW_FINGERPRINT, t.GW_TIMESTAMP, t.ABC_AUDIT_DATE_TIME FROM @catalog.@src_schema.cc_transaction t LEFT JOIN @catalog.@src_schema.cc_claim c ON t.claimid = c.id WHERE t.Soft_Delete = ''N''',
               'CC');
END
GO


-- =============================================================================
-- 19. MANIFEST_ARCHIVE
-- Permanent accumulating history of every MANIFEST row ever processed.
-- Populated by DELETE_MANIFEST_DATA proc: before clearing MANIFEST each cycle,
-- all current MANIFEST rows are INSERT-selected into this table.
-- Never truncated or deleted from — rows accumulate indefinitely across all cycles.
-- Production has 5.9M+ rows. Practice: grows by 5 rows per cycle (one per table).
-- Same schema as MANIFEST — no IDENTITY, no seed data.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'MANIFEST_ARCHIVE')
BEGIN
    CREATE TABLE abc.MANIFEST_ARCHIVE (
        DATAFILESPATH                    VARCHAR(500)   NULL,     -- relative path e.g. CC/CC_CLAIM
        LASTSUCCESSFULWRITETIMESTAMP     VARCHAR(50)    NULL,     -- Unix epoch ms of latest delivery
        SCHEMAHISTORY                    VARCHAR(2000)  NULL,     -- stringified dict {fingerprint: first_seen_ts}
        TOTALPROCESSEDRECORDSCOUNT       VARCHAR(50)    NULL,     -- cumulative record count as string
        XCENTER                          VARCHAR(10)    NULL,     -- e.g. CC
        AUDIT_DT_TM                      DATETIME2      NULL,     -- when this row was written to MANIFEST
        CYC_RUN_SK                       INT            NULL      -- cycle run when the row was archived
    );
END
GO


-- =============================================================================
-- 20. CDA_FILE_LOADS_ARCHIVE
-- Long-term storage for CDA_FILE_LOADS rows older than 30 days.
-- Populated by ARCHIVE_OLD_CDA_FILE_LOADS proc which runs at end of each cycle.
-- Keeps CDA_FILE_LOADS lean (~recent rows only) while this table holds full history.
-- Production: 8.7M+ rows while CDA_FILE_LOADS has only ~3,500 active rows.
-- Same schema as CDA_FILE_LOADS but FILE_LOAD_ID has no IDENTITY (copied as-is).
-- No seed data — starts empty.
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'CDA_FILE_LOADS_ARCHIVE')
BEGIN
    CREATE TABLE abc.CDA_FILE_LOADS_ARCHIVE (
        FILE_LOAD_ID              INT            NOT NULL,        -- copied from CDA_FILE_LOADS (no IDENTITY)
        TABLE_NAME                VARCHAR(100)   NOT NULL,
        GW_FINGERPRINT            VARCHAR(32)    NOT NULL,
        GW_TIMESTAMP              VARCHAR(50)    NOT NULL,
        CLOUD_PATH                VARCHAR(500)   NOT NULL,
        IS_LOADED                 BIT            NOT NULL DEFAULT 0,
        INSERT_DATE_TIME          DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        UPDATE_DATE_TIME          DATETIME2      NOT NULL DEFAULT SYSDATETIME(),
        IS_FINGERPRINT_UPDATED    BIT            NOT NULL DEFAULT 0,
        XCENTER                   VARCHAR(10)    NULL,
        CYC_RUN_SK                INT            NULL
    );
END
GO
