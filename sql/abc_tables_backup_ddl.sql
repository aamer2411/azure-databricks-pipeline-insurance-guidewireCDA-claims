-- =============================================================================
-- File  : abc_tables_backup_ddl.sql
-- DB    : insurance_claims_domain  (azurepractice68256.database.windows.net)
-- Schema: abc
-- Desc  : DDL for all end-of-cycle snapshot backup tables.
--         Run AFTER abc_tables_ddl.sql.
--
-- These tables are populated by the ADF ForEach "Truncate Archive Tables"
-- activity at the end of every pipeline cycle. For each table in ARCHIVE_TABLES,
-- ADF runs:
--   Pre-copy script : TRUNCATE TABLE abc.<table>_BACKUP
--   Source query    : SELECT * FROM abc.<table> WHERE XCENTER = 'CC'
--   Sink            : Bulk insert into abc.<table>_BACKUP
--
-- Purpose: point-in-time snapshot of tables that are cleared/repopulated every
-- cycle. If a run fails, the _BACKUP table shows the last known good state.
--
-- Tables NOT backed up (static config or append-only audit logs — no need):
--   CYC_CTRL_TBL, STEP_CTRL_TBL, JOB_CTRL_TBL, JOB_PARM_TBL  → static config
--   CYC_RUN_TBL, STEP_RUN_TBL, JOB_RUN_TBL, VALIDATION_RUN_TBL → append-only logs
--   BALANCE_METRICS, VALIDATION_ERROR_LOG                        → append-only logs
--   VALIDATION_CTRL_TBL                                          → static config rules
--
-- Backup tables in this file (7):
--   1. MANIFEST_BACKUP
--   2. MANIFEST_ARCHIVE_BACKUP
--   3. TABLE_LOAD_METADATA_BACKUP
--   4. RAW_TABLE_LOAD_BACKUP
--   5. CDA_FILE_LOADS_BACKUP
--   6. TABLES_TO_LOAD_RPL_BACKUP
--   7. TABLES_TO_LOAD_BACKUP
--
-- All backup tables:
--   - Mirror the schema of their source table exactly
--   - No IDENTITY on any column (rows are copied as-is)
--   - No seed data — start empty, truncated and reloaded each cycle
--   - No PRIMARY KEY — duplicate rows allowed (snapshot, not control table)
-- =============================================================================


-- =============================================================================
-- 1. MANIFEST_BACKUP
-- Snapshot of MANIFEST taken at end of each cycle.
-- Mirrors: abc.MANIFEST
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'MANIFEST_BACKUP')
BEGIN
    CREATE TABLE abc.MANIFEST_BACKUP (
        DATAFILESPATH                    VARCHAR(500)   NULL,
        LASTSUCCESSFULWRITETIMESTAMP     VARCHAR(50)    NULL,
        SCHEMAHISTORY                    VARCHAR(2000)  NULL,
        TOTALPROCESSEDRECORDSCOUNT       VARCHAR(50)    NULL,
        XCENTER                          VARCHAR(10)    NULL,
        AUDIT_DT_TM                      DATETIME2      NULL,
        CYC_RUN_SK                       INT            NULL
    );
END
GO


-- =============================================================================
-- 2. MANIFEST_ARCHIVE_BACKUP
-- Snapshot of MANIFEST_ARCHIVE taken at end of each cycle.
-- Mirrors: abc.MANIFEST_ARCHIVE (same schema as MANIFEST)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'MANIFEST_ARCHIVE_BACKUP')
BEGIN
    CREATE TABLE abc.MANIFEST_ARCHIVE_BACKUP (
        DATAFILESPATH                    VARCHAR(500)   NULL,
        LASTSUCCESSFULWRITETIMESTAMP     VARCHAR(50)    NULL,
        SCHEMAHISTORY                    VARCHAR(2000)  NULL,
        TOTALPROCESSEDRECORDSCOUNT       VARCHAR(50)    NULL,
        XCENTER                          VARCHAR(10)    NULL,
        AUDIT_DT_TM                      DATETIME2      NULL,
        CYC_RUN_SK                       INT            NULL
    );
END
GO


-- =============================================================================
-- 3. TABLE_LOAD_METADATA_BACKUP
-- Snapshot of TABLE_LOAD_METADATA taken at end of each cycle.
-- Mirrors: abc.TABLE_LOAD_METADATA
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLE_LOAD_METADATA_BACKUP')
BEGIN
    CREATE TABLE abc.TABLE_LOAD_METADATA_BACKUP (
        TABLE_ID                              INT            NULL,   -- no PK — snapshot table
        TABLE_NAME                            VARCHAR(100)   NOT NULL,
        LATEST_LASTSUCCESSFULWRITETIMESTAMP   VARCHAR(50)    NULL,
        PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP VARCHAR(50)    NULL,
        LATEST_FINGERPRINT                    VARCHAR(32)    NULL,
        PREVIOUS_FINGERPRINT                  VARCHAR(32)    NULL,
        LOAD_STATUS                           VARCHAR(1)     NULL,
        INSERT_DATE_TIME                      DATETIME2      NULL,
        UPDATE_DATE_TIME                      DATETIME2      NULL,
        XCENTER                               VARCHAR(10)    NULL,
        CYC_RUN_SK                            INT            NULL
    );
END
GO


-- =============================================================================
-- 4. RAW_TABLE_LOAD_BACKUP
-- Snapshot of RAW_TABLE_LOAD taken at end of each cycle.
-- Mirrors: abc.RAW_TABLE_LOAD (FILE_LOAD_ID copied as-is, no IDENTITY)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'RAW_TABLE_LOAD_BACKUP')
BEGIN
    CREATE TABLE abc.RAW_TABLE_LOAD_BACKUP (
        FILE_LOAD_ID    INT            NULL,   -- copied as-is, no IDENTITY
        TABLE_NAME      VARCHAR(100)   NOT NULL,
        GW_FINGERPRINT  VARCHAR(32)    NOT NULL,
        GW_TIMESTAMP    VARCHAR(50)    NOT NULL,
        CLOUD_PATH      VARCHAR(500)   NOT NULL,
        IS_LOADED       BIT            NULL,
        XCENTER         VARCHAR(10)    NOT NULL
    );
END
GO


-- =============================================================================
-- 5. CDA_FILE_LOADS_BACKUP
-- Snapshot of CDA_FILE_LOADS taken at end of each cycle.
-- Mirrors: abc.CDA_FILE_LOADS (FILE_LOAD_ID copied as-is, no IDENTITY)
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'CDA_FILE_LOADS_BACKUP')
BEGIN
    CREATE TABLE abc.CDA_FILE_LOADS_BACKUP (
        FILE_LOAD_ID              INT            NULL,   -- copied as-is, no IDENTITY
        TABLE_NAME                VARCHAR(100)   NOT NULL,
        GW_FINGERPRINT            VARCHAR(32)    NOT NULL,
        GW_TIMESTAMP              VARCHAR(50)    NOT NULL,
        CLOUD_PATH                VARCHAR(500)   NOT NULL,
        IS_LOADED                 BIT            NULL,
        INSERT_DATE_TIME          DATETIME2      NULL,
        UPDATE_DATE_TIME          DATETIME2      NULL,
        IS_FINGERPRINT_UPDATED    BIT            NULL,
        XCENTER                   VARCHAR(10)    NULL,
        CYC_RUN_SK                INT            NULL
    );
END
GO


-- =============================================================================
-- 6. TABLES_TO_LOAD_RPL_BACKUP
-- Snapshot of TABLES_TO_LOAD_RPL taken at end of each cycle.
-- Mirrors: abc.TABLES_TO_LOAD_RPL
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLES_TO_LOAD_RPL_BACKUP')
BEGIN
    CREATE TABLE abc.TABLES_TO_LOAD_RPL_BACKUP (
        TGT_FILE_TBL  VARCHAR(100)  NOT NULL,
        JOB_SK        INT           NOT NULL,
        STEP_SK       INT           NOT NULL,
        CYC_RUN_SK    INT           NOT NULL,
        XCENTER       VARCHAR(10)   NOT NULL
    );
END
GO


-- =============================================================================
-- 7. TABLES_TO_LOAD_BACKUP
-- Snapshot of TABLES_TO_LOAD taken at end of each cycle.
-- Mirrors: abc.TABLES_TO_LOAD
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'abc' AND t.name = 'TABLES_TO_LOAD_BACKUP')
BEGIN
    CREATE TABLE abc.TABLES_TO_LOAD_BACKUP (
        TABLE_NAME        VARCHAR(100)  NOT NULL,
        START_TIMESTAMP   BIGINT        NOT NULL,
        END_TIMESTAMP     BIGINT        NOT NULL,
        STEP_SK           INT           NOT NULL,
        JOB_SK            INT           NOT NULL,
        XCENTER           VARCHAR(10)   NOT NULL
    );
END
GO
