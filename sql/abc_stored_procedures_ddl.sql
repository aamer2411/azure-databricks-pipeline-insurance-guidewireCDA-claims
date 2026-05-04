-- =============================================================================
-- File  : abc_stored_procedures_ddl.sql
-- DB    : insurance_claims_domain  (azurepractice68256.database.windows.net)
-- Schema: abc
-- Desc  : DDL for all abc schema stored procedures.
--         All procs use CREATE OR ALTER — safe to re-run at any time.
--         Run AFTER abc_tables_ddl.sql (procs depend on tables existing).
--
-- Procs in this file:
--   1. PROC_UPDATE_CYC_START         -- opens a cycle
--   2. INSERT_UPDATE_JOB_CTRL_TBL   -- syncs job list for current cycle
--   3. DELETE_MANIFEST_DATA          -- clears MANIFEST before re-insert
--   4. GET_GW_FILE_METADATA_MASTER   -- updates TABLE_LOAD_METADATA from MANIFEST
--   5. PROC_WRAPPER_SRC_RAW         -- populates TABLES_TO_LOAD for Src->Raw step
--   6. PROC_UPDATE_JOB_START        -- opens a job; inserts JOB_RUN_TBL row, marks JOB_STS_CD='I'
--   7. UPDATE_METADATA_LOAD         -- on success: marks CDA_FILE_LOADS loaded (IS_LOADED=1), sets TABLE_LOAD_METADATA LOAD_STATUS='C'
--   8. PROC_UPDATE_JOB_END          -- updates JOB_RUN_TBL with final status, counts, and error message
--   9. PROC_UPDATE_STEP_START        -- opens a step; inserts STEP_RUN_TBL row, marks STEP_STS_CD='I'
--  10. PROC_WRAPPER_RAW_RPL         -- populates TABLES_TO_LOAD_RPL for Raw->Replica step
--  11. PROC_UPDATE_STEP_END         -- closes a step; updates STEP_RUN_TBL and STEP_CTRL_TBL with status S/F
--  12. PROC_UPDATE_CYC_END          -- closes a cycle; updates CYC_RUN_TBL, CYC_CTRL_TBL, STEP_CTRL_TBL, JOB_CTRL_TBL, VALIDATION_CTRL_TBL
--  13. PROC_VALIDATION_START        -- opens a validation run; inserts VALIDATION_RUN_TBL row, returns VAL_RUN_SK
--  14. PROC_VALIDATION_END          -- closes a validation run; updates VALIDATION_RUN_TBL and VALIDATION_CTRL_TBL
--  15. PROC_WRAPPER_RPL_RFN         -- populates TABLES_TO_LOAD_RFN for Replica->Refined step
-- =============================================================================


-- =============================================================================
-- SEQUENCES
-- Must exist before any proc below is executed.
-- Create once; safe to skip if already present (check sys.sequences first).
-- Seed START WITH to MAX(existing SK)+1 if rows already exist in the run tables.
--
--   SELECT MAX(CYC_RUN_SK)  FROM abc.CYC_RUN_TBL;
--   SELECT MAX(STEP_RUN_SK) FROM abc.STEP_RUN_TBL;
--   SELECT MAX(JOB_RUN_SK)  FROM abc.JOB_RUN_TBL;
--   SELECT MAX(VAL_RUN_SK)  FROM abc.VALIDATION_RUN_TBL;
-- =============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE object_id = OBJECT_ID('abc.SEQ_CYC_RUN_SK'))
    CREATE SEQUENCE abc.SEQ_CYC_RUN_SK  AS INT START WITH 1 INCREMENT BY 1;
GO
IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE object_id = OBJECT_ID('abc.SEQ_STEP_RUN_SK'))
    CREATE SEQUENCE abc.SEQ_STEP_RUN_SK AS INT START WITH 1 INCREMENT BY 1;
GO
IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE object_id = OBJECT_ID('abc.SEQ_JOB_RUN_SK'))
    CREATE SEQUENCE abc.SEQ_JOB_RUN_SK  AS INT START WITH 1 INCREMENT BY 1;
GO
IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE object_id = OBJECT_ID('abc.SEQ_VAL_RUN_SK'))
    CREATE SEQUENCE abc.SEQ_VAL_RUN_SK  AS INT START WITH 1 INCREMENT BY 1;
GO


-- =============================================================================
-- 1. PROC_UPDATE_CYC_START
-- Called by: Master Pipeline (very first activity each cycle)
-- Parameters: @CYC_SK, @FORCE_IND (default 'N'), @PL_RUN_ID
-- Logic:
--   1. Check CYC_STS_CD = 'C' for the given CYC_SK — only then proceed
--   2. Generate new CYC_RUN_SK via SEQ_CYC_RUN_SK (atomic, parallel-safe)
--   3. Insert new row into CYC_RUN_TBL with status 'I'
--   4. Update CYC_CTRL_TBL: set CYC_STS_CD='I', CYC_CUR_RUN_SK=new SK
--   5. Reset child STEP_CTRL_TBL rows to 'C' (unless FORCE_IND='Y')
--   6. Return new CYC_RUN_SK to ADF for downstream use
-- =============================================================================
CREATE OR ALTER PROCEDURE [abc].[PROC_UPDATE_CYC_START]
    @CYC_SK      INT,
    @FORCE_IND   VARCHAR(1)   = 'N',
    @PL_RUN_ID   VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NEW_CYC_RUN_SK INT;
    DECLARE @CYC_STS_CD     VARCHAR(10);
    DECLARE @NOW            DATETIME2 = SYSDATETIME();

    -- 1. Get current cycle status
    SELECT @CYC_STS_CD = CYC_STS_CD
    FROM abc.CYC_CTRL_TBL
    WHERE CYC_SK = @CYC_SK;

    -- Only proceed if cycle is idle (C = Complete/ready)
    IF @CYC_STS_CD = 'C'
    BEGIN
        -- 2. Generate next CYC_RUN_SK — atomic sequence, safe for concurrent callers
        SET @NEW_CYC_RUN_SK = NEXT VALUE FOR abc.SEQ_CYC_RUN_SK;

        -- 3. Insert new cycle run row with status 'I'
        INSERT INTO abc.CYC_RUN_TBL
            (CYC_RUN_SK, CYC_SK, CYC_STRTDT_TM, CYC_ENDDT_TM, CYC_STS_CD, AUD_DT_TM, PL_RUN_ID)
        VALUES
            (@NEW_CYC_RUN_SK, @CYC_SK, @NOW, NULL, 'I', @NOW, @PL_RUN_ID);

        -- 4. Update CYC_CTRL_TBL — mark in-progress
        UPDATE abc.CYC_CTRL_TBL
        SET CYC_STS_CD     = 'I',
            CYC_CUR_RUN_SK = @NEW_CYC_RUN_SK,
            AUD_DT_TM      = @NOW
        WHERE CYC_SK = @CYC_SK;

        -- 5. Reset child steps to 'C' only on a fresh run (not a forced restart)
        --    FORCE_IND='Y' preserves already-completed steps so they are not re-run
        IF @FORCE_IND <> 'Y'
        BEGIN
            UPDATE abc.STEP_CTRL_TBL
            SET STEP_STS_CD = 'C',
                AUD_DT_TM   = @NOW
            WHERE CYC_SK = @CYC_SK;
        END

        -- 6. Return new CYC_RUN_SK for ADF downstream use
        SELECT @NEW_CYC_RUN_SK AS CYC_RUN_SK;
    END
    ELSE
    BEGIN
        -- Cycle not ready — return -1 to signal ADF that cycle was skipped
        SELECT -1 AS CYC_RUN_SK;
    END
END;
GO


-- =============================================================================
-- 2. INSERT_UPDATE_JOB_CTRL_TBL
-- Called by: Src->Raw Pipeline — after PROC_UPDATE_STEP_START, before ForEach
-- Parameters: @STEP_SK_RAW, @STEP_SK_PRC, @XCENTER
-- Logic:
--   For each table in TABLE_LOAD_METADATA (filtered by @XCENTER):
--   1. INSERT a job row for @STEP_SK_RAW if one does not already exist
--      (self-healing: adds rows if a new table was introduced)
--   2. INSERT a job row for @STEP_SK_PRC if one does not already exist
--   3. UPDATE CUR_CYC_VALID for all @STEP_SK_RAW jobs:
--      'Y' = table has new data this cycle (LOAD_STATUS='I') → ForEach will process it
--      'N' = no new data this cycle → ForEach skips it
--   4. UPDATE CUR_CYC_VALID for all @STEP_SK_PRC jobs (same logic)
--
-- In the practice project JOB_CTRL_TBL is pre-seeded with all 5 tables,
-- so the INSERT path will not fire on normal runs. It is included to mirror
-- production behaviour and handle any fresh-DB or new-table scenarios.
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.INSERT_UPDATE_JOB_CTRL_TBL
    @STEP_SK_RAW  INT,
    @STEP_SK_PRC  INT,
    @XCENTER      VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NOW         DATETIME2 = SYSDATETIME();
    DECLARE @MAX_RAW_SK  INT;
    DECLARE @MAX_PRC_SK  INT;

    -- Compute base JOB_SK for any new inserts (MAX existing + offset via ROW_NUMBER)
    SELECT @MAX_RAW_SK = ISNULL(MAX(JOB_SK), @STEP_SK_RAW * 1000)
    FROM abc.JOB_CTRL_TBL
    WHERE STEP_SK = @STEP_SK_RAW;

    SELECT @MAX_PRC_SK = ISNULL(MAX(JOB_SK), @STEP_SK_PRC * 1000)
    FROM abc.JOB_CTRL_TBL
    WHERE STEP_SK = @STEP_SK_PRC;

    -- 1. INSERT missing RAW step jobs
    INSERT INTO abc.JOB_CTRL_TBL
        (JOB_SK, JOB_NM, STEP_SK, JOB_DESC, JOB_TYP_CD,
         SRC_PATH_SCHEM, TGT_PATH_SCHEM, SRC_FILE_TBL, TGT_FILE_TBL,
         JOB_STS_CD, ACTI_IND, AUD_DT_TM, SRC_NM, CUR_CYC_VALID)
    SELECT
        @MAX_RAW_SK + ROW_NUMBER() OVER (ORDER BY t.TABLE_ID),
        'gw_cda_stg_raw_' + @XCENTER,
        @STEP_SK_RAW,
        'gw_cda_stg_raw for the table ' + t.TABLE_NAME,
        'RAW',
        'GW_CDA_RAW.GW_CDA_STAGE',
        'GW_CDA_RAW',
        LOWER(t.TABLE_NAME),
        LOWER(t.TABLE_NAME),
        'C', 'Y', @NOW, 'GW_CDA',
        CASE WHEN t.LOAD_STATUS = 'I' THEN 'Y' ELSE 'N' END
    FROM abc.TABLE_LOAD_METADATA t
    WHERE t.XCENTER = @XCENTER
      AND NOT EXISTS (
          SELECT 1 FROM abc.JOB_CTRL_TBL j
          WHERE j.STEP_SK = @STEP_SK_RAW
            AND LOWER(j.TGT_FILE_TBL) = LOWER(t.TABLE_NAME)
      );

    -- 2. INSERT missing PRC (Replica) step jobs
    INSERT INTO abc.JOB_CTRL_TBL
        (JOB_SK, JOB_NM, STEP_SK, JOB_DESC, JOB_TYP_CD,
         SRC_PATH_SCHEM, TGT_PATH_SCHEM, SRC_FILE_TBL, TGT_FILE_TBL,
         JOB_STS_CD, ACTI_IND, AUD_DT_TM, SRC_NM, CUR_CYC_VALID)
    SELECT
        @MAX_PRC_SK + ROW_NUMBER() OVER (ORDER BY t.TABLE_ID),
        'gw_cda_raw_rpl_' + @XCENTER,
        @STEP_SK_PRC,
        'gw_cda_raw_rpl for the table ' + t.TABLE_NAME,
        'RPL',
        'GW_CDA_RAW',
        'GW_CDA_RPL',
        LOWER(t.TABLE_NAME),
        LOWER(t.TABLE_NAME),
        'C', 'Y', @NOW, 'GW_CDA',
        CASE WHEN t.LOAD_STATUS = 'I' THEN 'Y' ELSE 'N' END
    FROM abc.TABLE_LOAD_METADATA t
    WHERE t.XCENTER = @XCENTER
      AND NOT EXISTS (
          SELECT 1 FROM abc.JOB_CTRL_TBL j
          WHERE j.STEP_SK = @STEP_SK_PRC
            AND LOWER(j.TGT_FILE_TBL) = LOWER(t.TABLE_NAME)
      );

    -- 3. UPDATE CUR_CYC_VALID for RAW step jobs
    --    'Y' = new data this cycle → ForEach will process
    --    'N' = no new data → ForEach skips
    UPDATE j
    SET j.CUR_CYC_VALID = CASE WHEN t.LOAD_STATUS = 'I' THEN 'Y' ELSE 'N' END,
        j.JOB_STS_CD    = 'C',
        j.AUD_DT_TM     = @NOW
    FROM abc.JOB_CTRL_TBL j
    INNER JOIN abc.TABLE_LOAD_METADATA t
        ON LOWER(j.TGT_FILE_TBL) = LOWER(t.TABLE_NAME)
    WHERE j.STEP_SK  = @STEP_SK_RAW
      AND t.XCENTER  = @XCENTER;

    -- 4. UPDATE CUR_CYC_VALID for PRC (Replica) step jobs
    UPDATE j
    SET j.CUR_CYC_VALID = CASE WHEN t.LOAD_STATUS = 'I' THEN 'Y' ELSE 'N' END,
        j.JOB_STS_CD    = 'C',
        j.AUD_DT_TM     = @NOW
    FROM abc.JOB_CTRL_TBL j
    INNER JOIN abc.TABLE_LOAD_METADATA t
        ON LOWER(j.TGT_FILE_TBL) = LOWER(t.TABLE_NAME)
    WHERE j.STEP_SK  = @STEP_SK_PRC
      AND t.XCENTER  = @XCENTER;

END;
GO


-- =============================================================================
-- 3. DELETE_MANIFEST_DATA
-- Called by manifest notebook Part 1 BEFORE re-inserting rows from manifest.json.
-- Removes all rows for the given XCENTER so each pipeline run starts fresh.
-- Production call: EXECUTE abc.DELETE_MANIFEST_DATA '{xcenter}'
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.DELETE_MANIFEST_DATA
    @XCENTER VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM abc.MANIFEST
    WHERE XCENTER = @XCENTER;
END
GO


-- =============================================================================
-- 4. GET_GW_FILE_METADATA_MASTER
-- Called by manifest notebook Part 1 AFTER abc.MANIFEST has been populated.
-- For each table visible in MANIFEST (filtered by @XCENTER):
--   1. Shift LATEST_* values to PREVIOUS_* in TABLE_LOAD_METADATA.
--   2. Write the new LATEST_LASTSUCCESSFULWRITETIMESTAMP from MANIFEST.
--   3. Extract LATEST_FINGERPRINT from the SCHEMAHISTORY string
--      (32-char hex key, first key in the dict e.g. {'abc123...': 1234567890}).
--   4. Set LOAD_STATUS = 'I', UPDATE_DATE_TIME = now, CYC_RUN_SK = @CYC_RUN_SK.
--
-- TABLE_NAME is derived from DATAFILESPATH: the last segment after the final '/'.
-- e.g. 'CC/CC_CLAIM'  ->  'CC_CLAIM'
--
-- Production call:
--   EXECUTE abc.GET_GW_FILE_METADATA_MASTER '{CDA_STAGE_URL}', '{xcenter}', {cyc_run_sk}
--
-- NOTE: @CDA_STAGE_URL is accepted as a parameter to mirror the production
-- signature but is not used in query logic here — the path is already stored
-- in MANIFEST.DATAFILESPATH.
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.GET_GW_FILE_METADATA_MASTER
    @CDA_STAGE_URL VARCHAR(500),
    @XCENTER       VARCHAR(10),
    @CYC_RUN_SK    INT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE tlm
    SET
        -- Shift current LATEST to PREVIOUS
        tlm.PREVIOUS_LASTSUCCESSFULWRITETIMESTAMP = tlm.LATEST_LASTSUCCESSFULWRITETIMESTAMP,
        tlm.PREVIOUS_FINGERPRINT                  = tlm.LATEST_FINGERPRINT,

        -- Write new LATEST values from MANIFEST
        tlm.LATEST_LASTSUCCESSFULWRITETIMESTAMP   = m.LASTSUCCESSFULWRITETIMESTAMP,

        -- Extract fingerprint: first 32-char hex key from SCHEMAHISTORY dict string
        -- SCHEMAHISTORY looks like: {'a1b2c3d4e5f6...32chars...': 1700000000000}
        -- We grab the 32 chars starting after the opening "'" character.
        tlm.LATEST_FINGERPRINT = SUBSTRING(
            m.SCHEMAHISTORY,
            CHARINDEX('''', m.SCHEMAHISTORY) + 1,
            32
        ),

        tlm.LOAD_STATUS      = 'I',  -- 'I' = new data detected, ready to load into raw
        tlm.UPDATE_DATE_TIME = SYSDATETIME(),
        tlm.CYC_RUN_SK       = @CYC_RUN_SK

    FROM abc.TABLE_LOAD_METADATA AS tlm
    INNER JOIN abc.MANIFEST AS m
        -- Match on table name extracted from the trailing segment of DATAFILESPATH
        ON tlm.TABLE_NAME = UPPER(RIGHT(m.DATAFILESPATH, CHARINDEX('/', REVERSE(m.DATAFILESPATH)) - 1))

    WHERE m.XCENTER = @XCENTER;
END
GO


-- =============================================================================
-- 5. PROC_WRAPPER_SRC_RAW
-- Called by: Src->Raw Pipeline (ADF Stored Procedure activity) — after
--            INSERT_UPDATE_JOB_CTRL_TBL.
-- Parameters: @STEP_SK, @XCENTER
-- Logic:
--   1. Clear existing TABLES_TO_LOAD rows for this STEP_SK (so re-runs are safe)
--   2. INSERT one row per job that is active and valid this cycle.
--      Joins CDA_FILE_LOADS (IS_LOADED=0) to derive:
--        START_TIMESTAMP = MIN(GW_TIMESTAMP) — earliest pending file epoch ms
--        END_TIMESTAMP   = MAX(GW_TIMESTAMP) — latest pending file epoch ms
--      Filters:
--        CUR_CYC_VALID = 'Y'  → table has new data this cycle
--        JOB_STS_CD    = 'C'  → job is idle (not already in-progress or done)
--        ACTI_IND      = 'Y'  → job is enabled
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_WRAPPER_SRC_RAW
    @STEP_SK  INT,
    @XCENTER  VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Clear stale entries for this step (idempotent — safe to re-run)
    DELETE FROM abc.TABLES_TO_LOAD
    WHERE STEP_SK = @STEP_SK;

    -- 2. Populate with jobs that have new data this cycle and are ready to run.
    --    Join CDA_FILE_LOADS (pending files only) to get the timestamp range the
    --    Src->Raw notebook will use to identify which parquet folders to copy.
    INSERT INTO abc.TABLES_TO_LOAD
        (TABLE_NAME, START_TIMESTAMP, END_TIMESTAMP, STEP_SK, JOB_SK, XCENTER)
    SELECT
        LOWER(j.TGT_FILE_TBL),                          -- lowercase table name (matches parquet naming)
        MIN(CAST(f.GW_TIMESTAMP AS BIGINT)),             -- earliest pending file timestamp
        MAX(CAST(f.GW_TIMESTAMP AS BIGINT)),             -- latest pending file timestamp
        j.STEP_SK,
        j.JOB_SK,
        @XCENTER
    FROM abc.JOB_CTRL_TBL j
    INNER JOIN abc.CDA_FILE_LOADS f
        ON  LOWER(f.TABLE_NAME) = LOWER(j.TGT_FILE_TBL)
        AND f.XCENTER           = @XCENTER
        AND f.IS_LOADED         = 0                      -- only files not yet copied to raw
    WHERE j.STEP_SK       = @STEP_SK
      AND j.CUR_CYC_VALID = 'Y'
      AND j.JOB_STS_CD    = 'C'
      AND j.ACTI_IND      = 'Y'
    GROUP BY j.STEP_SK, j.JOB_SK, LOWER(j.TGT_FILE_TBL);

END;
GO


-- =============================================================================
-- 6. PROC_UPDATE_JOB_START
-- Called by: Src->Raw Pipeline (ADF Stored Procedure activity) — inside ForEach,
--            first activity per table before the copy notebook runs.
-- Parameters: @JOB_SK, @STEP_SK
-- Logic:
--   1. Derive STEP_RUN_SK from STEP_CTRL_TBL (no need to pass as parameter)
--   2. Generate new JOB_RUN_SK via SEQ_JOB_RUN_SK (atomic, parallel-safe)
--   3. INSERT new row into JOB_RUN_TBL with status 'I', start time = now, counts = NULL
--   4. UPDATE JOB_CTRL_TBL: JOB_STS_CD='I'
--   5. Return JOB_RUN_SK to ADF for downstream use (e.g. passed to PROC_UPDATE_JOB_END)
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_UPDATE_JOB_START
    @JOB_SK   INT,
    @STEP_SK  INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NEW_JOB_RUN_SK  INT;
    DECLARE @STEP_RUN_SK     INT;
    DECLARE @NOW             DATETIME2 = SYSDATETIME();

    -- 1. Derive STEP_RUN_SK from STEP_CTRL_TBL — no need to pass it as a parameter
    SELECT @STEP_RUN_SK = STEP_CUR_RUN_SK
    FROM abc.STEP_CTRL_TBL
    WHERE STEP_SK = @STEP_SK;

    -- 2. Generate next JOB_RUN_SK — atomic sequence, safe for parallel ForEach
    SET @NEW_JOB_RUN_SK = NEXT VALUE FOR abc.SEQ_JOB_RUN_SK;

    -- 3. Insert new job run row with status 'I'; counts populated by PROC_UPDATE_JOB_END
    INSERT INTO abc.JOB_RUN_TBL
        (JOB_RUN_SK, STEP_RUN_SK, JOB_SK, JOB_STRTDT_TM, JOB_ENDDT_TM, JOB_STS_CD,
         RCRD_READ_CNT, RCRD_LD_CNT, RCRD_UPD_CNT, RCRD_INS_CNT, RCRD_DEL_CNT,
         DATA_READ, DATA_WRITTEN, ERR_MSG, AUD_DT_TM)
    VALUES
        (@NEW_JOB_RUN_SK, @STEP_RUN_SK, @JOB_SK, @NOW, NULL, 'I',
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @NOW);

    -- 4. Update JOB_CTRL_TBL — mark job in-progress
    UPDATE abc.JOB_CTRL_TBL
    SET JOB_STS_CD = 'I',
        AUD_DT_TM  = @NOW
    WHERE JOB_SK = @JOB_SK;

    -- 5. Return JOB_RUN_SK for ADF downstream use
    SELECT @NEW_JOB_RUN_SK AS JOB_RUN_SK;
END;
GO


-- =============================================================================
-- 7. UPDATE_METADATA_LOAD
-- Called by: Src->Raw Pipeline (ADF Stored Procedure activity) — inside ForEach,
--            on SUCCESS of the Src->Raw notebook.
-- Parameters: @CYC_RUN_SK, @END_TIMESTAMP, @TABLE_NAME, @XCENTER
-- Logic:
--   1. Mark all pending CDA_FILE_LOADS rows for this table/xcenter as loaded
--      (IS_LOADED=0 → 1) where GW_TIMESTAMP <= END_TIMESTAMP.
--      This ensures re-runs do not re-copy the same files.
--   2. Update TABLE_LOAD_METADATA: LOAD_STATUS='C' — signals downstream steps
--      (Raw->Replica) that the Src->Raw load for this table is complete.
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.UPDATE_METADATA_LOAD
    @CYC_RUN_SK    INT,
    @END_TIMESTAMP BIGINT,
    @TABLE_NAME    VARCHAR(100),
    @XCENTER       VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NOW DATETIME2 = SYSDATETIME();

    -- 1. Mark files as loaded — all pending rows for this table up to END_TIMESTAMP
    UPDATE abc.CDA_FILE_LOADS
    SET IS_LOADED        = 1,
        UPDATE_DATE_TIME = @NOW
    WHERE LOWER(TABLE_NAME)            = LOWER(@TABLE_NAME)
      AND XCENTER                      = @XCENTER
      AND IS_LOADED                    = 0
      AND CAST(GW_TIMESTAMP AS BIGINT) <= @END_TIMESTAMP;

    -- 2. Mark Src->Raw load complete in TABLE_LOAD_METADATA
    --    LOAD_STATUS 'I' → 'C' so the Raw->Replica step knows this table is ready
    UPDATE abc.TABLE_LOAD_METADATA
    SET LOAD_STATUS      = 'C',
        UPDATE_DATE_TIME = @NOW
    WHERE UPPER(TABLE_NAME) = UPPER(@TABLE_NAME)
      AND XCENTER           = @XCENTER;

END;
GO


-- =============================================================================
-- 8. PROC_UPDATE_JOB_END
-- Called by: Src->Raw Pipeline (ADF Stored Procedure activity) — inside ForEach,
--            on FAILURE of the Src->Raw notebook.
-- Parameters: @JOB_SK, @STATUS, @ROWS_READ, @ROWS_LOADED, @RCRD_UPD, @RCRD_INS,
--             @RCRD_DEL, @DATA_READ, @DATA_WRITTEN, @ERR_MSG
-- ADF passes:
--   STATUS      = 'F'
--   DATA_READ   = null (treat as null)
--   DATA_WRITTEN= null (treat as null)
--   ERR_MSG     = concat(runPageUrl, ' | ', runError)
--   JOB_SK      = @item().JOB_SK
--   RCRD_DEL/INS/UPD = 0
--   ROWS_LOADED/READ  = 0
-- Logic:
--   1. Close the open JOB_RUN_TBL row — set final status, end time, all counts.
--   2. Update JOB_CTRL_TBL: JOB_STS_CD = STATUS (F).
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_UPDATE_JOB_END
    @JOB_SK        INT,
    @STATUS        VARCHAR(10),
    @ROWS_READ     INT           = NULL,
    @ROWS_LOADED   INT           = NULL,
    @RCRD_UPD      INT           = NULL,
    @RCRD_INS      INT           = NULL,
    @RCRD_DEL      INT           = NULL,
    @DATA_READ     INT           = NULL,
    @DATA_WRITTEN  INT           = NULL,
    @ERR_MSG       VARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NOW DATETIME2 = SYSDATETIME();

    -- 1. Close the open JOB_RUN_TBL row with final counts and status
    UPDATE abc.JOB_RUN_TBL
    SET JOB_STS_CD    = @STATUS,
        JOB_ENDDT_TM  = @NOW,
        RCRD_READ_CNT = @ROWS_READ,
        RCRD_LD_CNT   = @ROWS_LOADED,
        RCRD_UPD_CNT  = @RCRD_UPD,
        RCRD_INS_CNT  = @RCRD_INS,
        RCRD_DEL_CNT  = @RCRD_DEL,
        DATA_READ     = @DATA_READ,
        DATA_WRITTEN  = @DATA_WRITTEN,
        ERR_MSG       = @ERR_MSG,
        AUD_DT_TM     = @NOW
    WHERE JOB_SK     = @JOB_SK
      AND JOB_STS_CD = 'I';

    -- 2. Update JOB_CTRL_TBL — mark job with final status (F on failure path)
    UPDATE abc.JOB_CTRL_TBL
    SET JOB_STS_CD = @STATUS,
        AUD_DT_TM  = @NOW
    WHERE JOB_SK = @JOB_SK;

END;
GO


-- =============================================================================
-- 9. PROC_UPDATE_STEP_START
-- Called by: Raw->Replica Pipeline — first activity of each step (before ForEach)
-- Parameters: @STEP_SK, @FORCE_IND (default 'N')
-- Logic:
--   1. Get STEP_STS_CD and CYC_SK from STEP_CTRL_TBL for the given STEP_SK
--   2. Only proceed if STEP_STS_CD = 'C' (idle/ready)
--   3. Get CYC_RUN_SK (CYC_CUR_RUN_SK) from CYC_CTRL_TBL
--   4. Generate new STEP_RUN_SK via SEQ_STEP_RUN_SK (atomic, parallel-safe)
--   5. INSERT new row into STEP_RUN_TBL with status 'I', start time = now
--   6. UPDATE STEP_CTRL_TBL: STEP_STS_CD='I', STEP_CUR_RUN_SK=new SK
--   7. Reset child JOB_CTRL_TBL rows to 'C' ONLY IF FORCE_IND <> 'Y'
--      (FORCE_IND='Y' preserves already-completed jobs for restartability)
--   8. Return STEP_RUN_SK + CYC_RUN_SK to ADF for downstream use
--      Returns -1 for both if step is not ready
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_UPDATE_STEP_START
    @STEP_SK    INT,
    @FORCE_IND  VARCHAR(1) = 'N'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NEW_STEP_RUN_SK INT;
    DECLARE @CYC_SK          INT;
    DECLARE @CYC_RUN_SK      INT;
    DECLARE @STEP_STS_CD     VARCHAR(10);
    DECLARE @NOW             DATETIME2 = SYSDATETIME();

    -- 1. Get current step status and parent CYC_SK from STEP_CTRL_TBL
    SELECT @STEP_STS_CD = STEP_STS_CD,
           @CYC_SK      = CYC_SK
    FROM abc.STEP_CTRL_TBL
    WHERE STEP_SK = @STEP_SK;

    -- Only proceed if step is idle (C = Complete/ready)
    IF @STEP_STS_CD = 'C'
    BEGIN
        -- 3. Get CYC_RUN_SK from CYC_CTRL_TBL
        SELECT @CYC_RUN_SK = CYC_CUR_RUN_SK
        FROM abc.CYC_CTRL_TBL
        WHERE CYC_SK = @CYC_SK;

        -- 4. Generate next STEP_RUN_SK — atomic sequence, safe for concurrent callers
        SET @NEW_STEP_RUN_SK = NEXT VALUE FOR abc.SEQ_STEP_RUN_SK;

        -- 5. Insert new step run row with status 'I'
        INSERT INTO abc.STEP_RUN_TBL
            (STEP_RUN_SK, CYC_RUN_SK, STEP_SK, STEP_STS_CD, STEP_STRTDT_TM, STEP_ENDDT_TM, AUD_DT_TM)
        VALUES
            (@NEW_STEP_RUN_SK, @CYC_RUN_SK, @STEP_SK, 'I', @NOW, NULL, @NOW);

        -- 6. Update STEP_CTRL_TBL — mark in-progress
        UPDATE abc.STEP_CTRL_TBL
        SET STEP_STS_CD     = 'I',
            STEP_CUR_RUN_SK = @NEW_STEP_RUN_SK,
            AUD_DT_TM       = @NOW
        WHERE STEP_SK = @STEP_SK;

        -- 7. Reset child jobs to 'C' only on a fresh run (not a forced restart)
        --    FORCE_IND='Y' preserves already-completed jobs so they are not re-run
        IF @FORCE_IND <> 'Y'
        BEGIN
            UPDATE abc.JOB_CTRL_TBL
            SET JOB_STS_CD = 'C',
                AUD_DT_TM  = @NOW
            WHERE STEP_SK = @STEP_SK;
        END

        -- 8. Return STEP_RUN_SK and CYC_RUN_SK for ADF downstream use
        SELECT @NEW_STEP_RUN_SK AS STEP_RUN_SK, @CYC_RUN_SK AS CYC_RUN_SK;
    END
    ELSE
    BEGIN
        -- Step not ready — return -1 to signal ADF that step was skipped
        SELECT -1 AS STEP_RUN_SK, -1 AS CYC_RUN_SK;
    END
END;
GO


-- =============================================================================
-- 10. PROC_WRAPPER_RAW_RPL
-- Called by: Raw->Replica Pipeline (ADF Stored Procedure activity) — after
--            PROC_UPDATE_STEP_START, before ForEach.
-- Parameters: @CYC_SK, @STEP_SK, @XCENTER
-- Logic:
--   1. Derive CYC_RUN_SK from CYC_CTRL_TBL using @CYC_SK
--   2. Clear existing TABLES_TO_LOAD_RPL rows for this STEP_SK (idempotent re-runs)
--   3. INSERT one row per job that is active and valid this cycle.
--      Filters:
--        LOAD_STATUS   = 'C'  → Src->Raw completed successfully for this table
--        CUR_CYC_VALID = 'Y'  → table had new data this cycle
--        JOB_STS_CD    = 'C'  → job is idle
--        ACTI_IND      = 'Y'  → job is enabled
--        JOB_TYP_CD    = 'RPL' → Raw->Replica jobs only
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_WRAPPER_RAW_RPL
    @CYC_SK   INT,
    @STEP_SK  INT,
    @XCENTER  VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @CYC_RUN_SK INT;

    -- 1. Derive CYC_RUN_SK from CYC_CTRL_TBL
    SELECT @CYC_RUN_SK = CYC_CUR_RUN_SK
    FROM abc.CYC_CTRL_TBL
    WHERE CYC_SK = @CYC_SK;

    -- 2. Clear stale entries for this step (idempotent — safe to re-run)
    DELETE FROM abc.TABLES_TO_LOAD_RPL
    WHERE STEP_SK = @STEP_SK;

    -- 3. Populate with jobs that have completed Src->Raw and are ready for replica load.
    INSERT INTO abc.TABLES_TO_LOAD_RPL
        (TGT_FILE_TBL, JOB_SK, STEP_SK, CYC_RUN_SK, XCENTER)
    SELECT
        LOWER(j.TGT_FILE_TBL),
        j.JOB_SK,
        j.STEP_SK,
        @CYC_RUN_SK,
        @XCENTER
    FROM abc.JOB_CTRL_TBL j
    INNER JOIN abc.TABLE_LOAD_METADATA m
        ON  UPPER(m.TABLE_NAME) = UPPER(j.TGT_FILE_TBL)
        AND m.XCENTER           = @XCENTER
        AND m.LOAD_STATUS       = 'C'              -- Src->Raw completed for this table
    WHERE j.STEP_SK       = @STEP_SK
      AND j.CUR_CYC_VALID = 'Y'
      AND j.JOB_STS_CD    = 'C'
      AND j.ACTI_IND      = 'Y'
      AND j.JOB_TYP_CD    = 'RPL';

END;
GO


-- =============================================================================
-- 11. PROC_UPDATE_STEP_END
-- Called by: Src->Raw and Raw->Replica Pipelines — after ForEach completes,
--            on failure path.
-- Parameters: @STEP_SK, @STATUS
-- Logic:
--   1. Derive STEP_RUN_SK from STEP_CTRL_TBL (STEP_CUR_RUN_SK)
--   2. Update STEP_RUN_TBL: set final status and end time
--   3. Update STEP_CTRL_TBL: set STEP_STS_CD to final status
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_UPDATE_STEP_END
    @STEP_SK  INT,
    @STATUS   VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @STEP_RUN_SK INT;
    DECLARE @NOW         DATETIME2 = SYSDATETIME();

    -- 1. Derive STEP_RUN_SK from STEP_CTRL_TBL
    SELECT @STEP_RUN_SK = STEP_CUR_RUN_SK
    FROM abc.STEP_CTRL_TBL
    WHERE STEP_SK = @STEP_SK;

    -- 2. Close the open STEP_RUN_TBL row with final status
    UPDATE abc.STEP_RUN_TBL
    SET STEP_STS_CD   = @STATUS,
        STEP_ENDDT_TM = @NOW,
        AUD_DT_TM     = @NOW
    WHERE STEP_RUN_SK = @STEP_RUN_SK;

    -- 3. Update STEP_CTRL_TBL with final status
    UPDATE abc.STEP_CTRL_TBL
    SET STEP_STS_CD = @STATUS,
        AUD_DT_TM   = @NOW
    WHERE STEP_SK = @STEP_SK;

END;
GO


-- =============================================================================
-- 12. PROC_UPDATE_CYC_END
-- Called by: Src->Raw and Raw->Replica Pipelines — after ForEach completes,
--            in parallel with PROC_UPDATE_STEP_END.
-- Parameters: @CYC_SK
-- Logic:
--   1. Determine final cycle status: 'F' if any child step has STEP_STS_CD='F',
--      otherwise 'S'
--   2. Get CYC_CUR_RUN_SK from CYC_CTRL_TBL
--   3. Update CYC_RUN_TBL: set final status and end time
--   4. Update CYC_CTRL_TBL: set CYC_STS_CD='C' (idle — ready for next run)
--   5. Reset STEP_CTRL_TBL rows to 'C' for next cycle
--   6. Reset JOB_CTRL_TBL rows to 'C' for next cycle
--   7. Reset VALIDATION_CTRL_TBL STATUS to 'C' for next cycle
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_UPDATE_CYC_END
    @CYC_SK  INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @CYC_RUN_SK  INT;
    DECLARE @FINAL_STS   VARCHAR(10);
    DECLARE @NOW         DATETIME2 = SYSDATETIME();

    -- 1. Determine final cycle status based on child step outcomes
    SELECT @FINAL_STS = CASE WHEN COUNT(CASE WHEN STEP_STS_CD = 'F' THEN 1 END) > 0 THEN 'F' ELSE 'S' END
    FROM abc.STEP_CTRL_TBL
    WHERE CYC_SK = @CYC_SK;

    -- 2. Get current cycle run SK
    SELECT @CYC_RUN_SK = CYC_CUR_RUN_SK
    FROM abc.CYC_CTRL_TBL
    WHERE CYC_SK = @CYC_SK;

    -- 3. Close CYC_RUN_TBL row with final status
    UPDATE abc.CYC_RUN_TBL
    SET CYC_STS_CD   = @FINAL_STS,
        CYC_ENDDT_TM = @NOW,
        AUD_DT_TM    = @NOW
    WHERE CYC_RUN_SK = @CYC_RUN_SK;

    -- 4. Mark cycle complete/idle in CYC_CTRL_TBL — ready for next run
    UPDATE abc.CYC_CTRL_TBL
    SET CYC_STS_CD = 'C',
        AUD_DT_TM  = @NOW
    WHERE CYC_SK = @CYC_SK;

    -- 5. Reset child steps to 'C' for next cycle
    UPDATE abc.STEP_CTRL_TBL
    SET STEP_STS_CD = 'C',
        AUD_DT_TM   = @NOW
    WHERE CYC_SK = @CYC_SK;

    -- 6. Reset child jobs to 'C' for next cycle
    UPDATE abc.JOB_CTRL_TBL
    SET JOB_STS_CD = 'C',
        AUD_DT_TM  = @NOW
    WHERE STEP_SK IN (SELECT STEP_SK FROM abc.STEP_CTRL_TBL WHERE CYC_SK = @CYC_SK);

    -- 7. Reset validation rules to 'C' for next cycle
    UPDATE abc.VALIDATION_CTRL_TBL
    SET STATUS    = 'C',
        AUD_DT_TM = @NOW
    WHERE XCENTER IN (SELECT DISTINCT XCENTER FROM abc.STEP_CTRL_TBL WHERE CYC_SK = @CYC_SK);

END;
GO


-- =============================================================================
-- PROC_VALIDATION_START
-- Called by: Data Validation pipeline — ADF Lookup activity before notebook 11
--            Runs once per validation rule (inside ForEach over VALIDATION_CTRL_TBL)
-- Parameters:
--   @val_sk      INT            -- PK of the rule in VALIDATION_CTRL_TBL
--   @val_rule    VARCHAR(255)   -- rule description (for logging)
--   @table_name  VARCHAR(255)   -- replica table being validated
--   @primary_key VARCHAR(255)   -- PK column of that table
--   @xcenter     VARCHAR(50)    -- source system e.g. CC
--   @step_sk     INT            -- step this validation belongs to (10120)
--   @cyc_run_sk  INT            -- current cycle run SK
-- Returns: VAL_RUN_SK — ADF passes this as widget to notebook 11
-- =============================================================================
CREATE OR ALTER PROCEDURE [abc].[PROC_VALIDATION_START]
    @val_sk      INT,
    @val_rule    VARCHAR(255),
    @table_name  VARCHAR(255),
    @primary_key VARCHAR(255),
    @xcenter     VARCHAR(50),
    @step_sk     INT,
    @cyc_run_sk  INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @NEW_VAL_RUN_SK INT;
    DECLARE @NOW DATETIME2 = SYSDATETIME();

    -- 1. Generate new VAL_RUN_SK — atomic sequence, safe for parallel ForEach
    SET @NEW_VAL_RUN_SK = NEXT VALUE FOR abc.SEQ_VAL_RUN_SK;

    -- 2. Insert new validation run row with status 'I' (In-Progress)
    --    TOTAL_COUNT / PASSED_COUNT / FAILED_COUNT start at 0 — updated by PROC_VALIDATION_END
    INSERT INTO abc.VALIDATION_RUN_TBL (
        VAL_RUN_SK, VAL_SK, VAL_RULE, TABLE_NAME,
        PRIMARY_KEY, XCENTER, STEP_SK, CYC_RUN_SK,
        VAL_STATUS, VAL_START_DTM, VAL_END_DTM,
        TOTAL_COUNT, PASSED_COUNT, FAILED_COUNT,
        AUD_DT_TM
    )
    VALUES (
        @NEW_VAL_RUN_SK, @val_sk, @val_rule, @table_name,
        @primary_key, @xcenter, @step_sk, @cyc_run_sk,
        'I', @NOW, NULL, 0, 0, 0, @NOW
    );

    -- 3. Update VALIDATION_CTRL_TBL — record current run SK and mark in-progress
    UPDATE abc.VALIDATION_CTRL_TBL
    SET VAL_RUN_SK = @NEW_VAL_RUN_SK,
        STATUS     = 'I',
        AUD_DT_TM  = @NOW
    WHERE VAL_SK = @val_sk;

    -- 4. Return new VAL_RUN_SK — ADF reads this via Lookup and passes it as
    --    the val_run_sk widget to notebook 11
    SELECT @NEW_VAL_RUN_SK AS VAL_RUN_SK;
END;
GO


-- =============================================================================
-- PROC_VALIDATION_END
-- Called by: Data Validation pipeline — ADF Stored Procedure activity after notebook 11
--            Runs once per validation rule (inside ForEach over VALIDATION_CTRL_TBL)
-- Parameters:
--   @val_sk        INT           -- PK of the rule in VALIDATION_CTRL_TBL
--   @status        VARCHAR(1)    -- S=Success (no failures), F=Failed (violations found)
--   @total_count   INT           -- total rows checked (from notebook 11 exit output)
--   @passed_count  INT           -- rows that passed (from notebook 11 exit output)
--   @failed_count  INT           -- rows that failed (from notebook 11 exit output)
-- Note: Failures are advisory — they do not stop the pipeline
-- =============================================================================
CREATE OR ALTER PROCEDURE [abc].[PROC_VALIDATION_END]
    @val_sk        INT,
    @status        VARCHAR(1),
    @total_count   INT,
    @passed_count  INT,
    @failed_count  INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @VAL_RUN_SK INT;
    DECLARE @NOW DATETIME2 = SYSDATETIME();

    -- 1. Get the current VAL_RUN_SK for this rule — set by PROC_VALIDATION_START
    SELECT @VAL_RUN_SK = VAL_RUN_SK
    FROM abc.VALIDATION_CTRL_TBL
    WHERE VAL_SK = @val_sk;

    -- 2. Close the validation run — record final counts and end time
    UPDATE abc.VALIDATION_RUN_TBL
    SET VAL_STATUS   = @status,
        VAL_END_DTM  = @NOW,
        TOTAL_COUNT  = @total_count,
        PASSED_COUNT = @passed_count,
        FAILED_COUNT = @failed_count,
        AUD_DT_TM    = @NOW
    WHERE VAL_RUN_SK = @VAL_RUN_SK;

    -- 3. Update VALIDATION_CTRL_TBL — mark rule as complete with final status
    UPDATE abc.VALIDATION_CTRL_TBL
    SET STATUS    = @status,
        AUD_DT_TM = @NOW
    WHERE VAL_SK = @val_sk;
END;
GO


-- =============================================================================
-- 15. PROC_WRAPPER_RPL_RFN
-- Called by: Replica->Refined Pipeline (ADF Stored Procedure activity) — after
--            PROC_UPDATE_STEP_START, before ForEach.
-- Parameters: @STEP_SK, @X_CENTER
-- Logic:
--   1. Derive CYC_SK from STEP_CTRL_TBL using @STEP_SK
--   2. Derive CYC_RUN_SK from CYC_CTRL_TBL using @CYC_SK
--   3. Clear existing TABLES_TO_LOAD_RFN rows for this STEP_SK (idempotent re-runs)
--   4. INSERT one row per active idle RFN job for this step into TABLES_TO_LOAD_RFN
--      Filters:
--        JOB_STS_CD = 'C'   → job is idle and ready
--        ACTI_IND   = 'Y'   → job is enabled
--        JOB_TYP_CD = 'RFN' → Replica->Refined jobs only
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.PROC_WRAPPER_RPL_RFN
    @STEP_SK  INT,
    @X_CENTER VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @CYC_SK     INT;
    DECLARE @CYC_RUN_SK INT;

    -- 1. Derive CYC_SK from STEP_CTRL_TBL
    SELECT @CYC_SK = CYC_SK
    FROM abc.STEP_CTRL_TBL
    WHERE STEP_SK = @STEP_SK;

    -- 2. Derive CYC_RUN_SK from CYC_CTRL_TBL
    SELECT @CYC_RUN_SK = CYC_CUR_RUN_SK
    FROM abc.CYC_CTRL_TBL
    WHERE CYC_SK = @CYC_SK;

    -- 3. Clear stale entries for this step (idempotent — safe to re-run)
    DELETE FROM abc.TABLES_TO_LOAD_RFN
    WHERE STEP_SK = @STEP_SK;

    -- 4. Populate with active, idle Replica->Refined jobs for this step.
    --    No TABLE_LOAD_METADATA join needed — Raw->Replica has already completed
    --    by the time this proc runs.
    INSERT INTO abc.TABLES_TO_LOAD_RFN
        (TGT_FILE_TBL, TGT_PATH_SCHEM, SRC_FILE_TBL, JOB_SK, STEP_SK, SRC_PATH_SCHEM, XCENTER)
    SELECT
        LOWER(j.TGT_FILE_TBL),
        j.TGT_PATH_SCHEM,
        j.SRC_FILE_TBL,
        j.JOB_SK,
        j.STEP_SK,
        j.SRC_PATH_SCHEM,
        @X_CENTER
    FROM abc.JOB_CTRL_TBL j
    WHERE j.STEP_SK    = @STEP_SK
      AND j.JOB_STS_CD = 'C'
      AND j.ACTI_IND   = 'Y'
      AND j.JOB_TYP_CD = 'RFN';
END;
GO


-- =============================================================================
-- 3. DELETE_MANIFEST_DATA  (updated)
-- Called by manifest notebook (02) BEFORE re-inserting rows from manifest.json.
-- Updated to first archive current MANIFEST rows into MANIFEST_ARCHIVE, then
-- delete them. This builds a permanent history of every manifest row processed.
-- Production call: EXECUTE abc.DELETE_MANIFEST_DATA '{xcenter}'
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.DELETE_MANIFEST_DATA
    @XCENTER VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Archive current MANIFEST rows before clearing.
    -- MANIFEST_ARCHIVE accumulates all rows ever processed (never truncated).
    INSERT INTO abc.MANIFEST_ARCHIVE (
        DATAFILESPATH,
        LASTSUCCESSFULWRITETIMESTAMP,
        SCHEMAHISTORY,
        TOTALPROCESSEDRECORDSCOUNT,
        XCENTER,
        AUDIT_DT_TM,
        CYC_RUN_SK
    )
    SELECT
        DATAFILESPATH,
        LASTSUCCESSFULWRITETIMESTAMP,
        SCHEMAHISTORY,
        TOTALPROCESSEDRECORDSCOUNT,
        XCENTER,
        AUDIT_DT_TM,
        CYC_RUN_SK
    FROM abc.MANIFEST
    WHERE XCENTER = @XCENTER;

    -- Step 2: Clear MANIFEST so notebook 02 can re-insert fresh rows for this cycle.
    DELETE FROM abc.MANIFEST
    WHERE XCENTER = @XCENTER;

END;
GO


-- =============================================================================
-- 16. ARCHIVE_OLD_CDA_FILE_LOADS
-- Called by ADF at end of each cycle after the ForEach archive step.
-- Moves CDA_FILE_LOADS rows older than 30 days to CDA_FILE_LOADS_ARCHIVE,
-- then deletes them from the main table. Keeps CDA_FILE_LOADS lean so that
-- the anti-join in notebook 04 stays fast.
-- Production: CDA_FILE_LOADS has ~3,500 active rows;
--             CDA_FILE_LOADS_ARCHIVE has 8.7M+ archived rows.
-- Parameter: @x_center — filters rows by XCENTER (e.g. 'CC')
-- =============================================================================
CREATE OR ALTER PROCEDURE abc.ARCHIVE_OLD_CDA_FILE_LOADS
    @x_center VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ARCHIVE_DAYS  INT        = 30;
    DECLARE @CUTOFF_DATE   DATETIME2  = DATEADD(DAY, -@ARCHIVE_DAYS, SYSUTCDATETIME());
    DECLARE @ROWS_ARCHIVED INT        = 0;

    -- Step 1: Copy eligible rows to archive table.
    -- Rows are old (INSERT_DATE_TIME older than cutoff) and already loaded (IS_LOADED=1).
    INSERT INTO abc.CDA_FILE_LOADS_ARCHIVE (
        FILE_LOAD_ID,
        TABLE_NAME,
        GW_FINGERPRINT,
        GW_TIMESTAMP,
        CLOUD_PATH,
        IS_LOADED,
        INSERT_DATE_TIME,
        UPDATE_DATE_TIME,
        IS_FINGERPRINT_UPDATED,
        XCENTER,
        CYC_RUN_SK
    )
    SELECT
        FILE_LOAD_ID,
        TABLE_NAME,
        GW_FINGERPRINT,
        GW_TIMESTAMP,
        CLOUD_PATH,
        IS_LOADED,
        INSERT_DATE_TIME,
        UPDATE_DATE_TIME,
        IS_FINGERPRINT_UPDATED,
        XCENTER,
        CYC_RUN_SK
    FROM abc.CDA_FILE_LOADS
    WHERE XCENTER          = @x_center
      AND IS_LOADED        = 1
      AND INSERT_DATE_TIME < @CUTOFF_DATE;

    SET @ROWS_ARCHIVED = @@ROWCOUNT;

    -- Step 2: Delete the archived rows from the main table.
    DELETE FROM abc.CDA_FILE_LOADS
    WHERE XCENTER          = @x_center
      AND IS_LOADED        = 1
      AND INSERT_DATE_TIME < @CUTOFF_DATE;

    PRINT 'Archived ' + CAST(@ROWS_ARCHIVED AS VARCHAR(10)) + ' CDA_FILE_LOADS records for XCENTER=' + @x_center;

END;
GO
