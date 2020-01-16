/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvTypes.sql
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
14/01/2020  | M Revitt      | Changes to fix the array boundaries when doing > 61 materialised views per table
            |               | Added BITMAP_OFFSET
05/11/2019  | M Revitt      | Changes to allow bitmap column to be manipulated as an array
29/10/2019  | M Revitt      | Create data types used by this solution
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    PostGre does not support database packages and therefore database package variables so to overcome this I have
                created a new type to hold all of the constants in memory to improve performance.

                This script contains a total of two functions and one type
                o   One for the database triggers, with the minimum number of variables required to reduce processing time
                o   One for everything else that is called from the public functions only and then passed on as a parameter
                o   The data type that holds all of the predefined variables

Notes:          The functions in this script are references by bot hthe simple and complex functions and so must be run first
                when setting up the environment

                All of the constants in this file are categorised and kept in alphabetic order within these categories,
                this dicipline should be retained with all updates

                The trigger function must be defined first as it is called by the main function to remove the need to define things
                twice

                Help can be invoked by running the rollowing command from within PostGre

                DO $$ DECLARE rConst mv$allConstants;BEGIN rConst:=mv$buildAllConstants();RAISE NOTICE '%',rConst.HELP_TEXT;END $$;

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in all
                other versions that I have tested, including Aurora

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

*************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U cdl_pgmview -q -f SaveSet/mvTypes.sql

-- -------------------- Write DROP-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP TYPE   IF EXISTS mv$allConstants   CASCADE;
DROP TYPE   IF EXISTS mv$bitValue       CASCADE;

SET CLIENT_MIN_MESSAGES = NOTICE;

CREATE
TYPE    mv$allConstants
AS
(
-- Database Role used to access materialized views
------------------------------------------------------------------------------------------------------------------------------------
    PGMV_SELECT_ROLE                TEXT,
--
-- Characters used FOR string delimination
------------------------------------------------------------------------------------------------------------------------------------
    INNER_TOKEN                     TEXT,
    FROM_TOKEN                      TEXT,
    JOIN_TOKEN                      TEXT,
    LEFT_TOKEN                      TEXT,
    ON_TOKEN                        TEXT,
    OUTER_TOKEN                     TEXT,
    RIGHT_TOKEN                     TEXT,
    WHERE_TOKEN                     TEXT,
    NO_INNER_TOKEN                  TEXT,
    COMMA_INNER_TOKEN               TEXT,
    COMMA_LEFT_TOKEN                TEXT,
    COMMA_RIGHT_TOKEN               TEXT,
--
-- Maths Commands
------------------------------------------------------------------------------------------------------------------------------------
    ARRAY_LOWER_VALUE               SMALLINT,
    BASE_TWO                        SMALLINT,
    BITAND_COMMAND                  TEXT,
    BITMAP_NOT_SET                  SMALLINT,
    BITMAP_OFFSET                   SMALLINT,
    EQUALS_COMMAND                  TEXT,
    FIRST_PGMVIEW_BIT               SMALLINT,
    LESS_THAN_EQUAL                 TEXT,
    MAX_BITMAP_SIZE                 BIGINT,
    MAX_PGMVIEWS_ROWS               SMALLINT,
    MAX_PGMVIEWS_PER_ROW            SMALLINT,
    MAX_PGMVIEWS_PER_TABLE          SMALLINT,
    MV_MAX_BASE_TABLE_LEN           SMALLINT,
    SUBTRACT_COMMAND                TEXT,
    TWO_TO_THE_POWER_OF             TEXT,
--
-- Characters used in string constructs
------------------------------------------------------------------------------------------------------------------------------------
    EMPTY_STRING                    TEXT,
    NEW_LINE                        TEXT,
    CARRIAGE_RETURN                 TEXT,
    SPACE_CHARACTER                 TEXT,
    SINGLE_QUOTE_CHARACTER          TEXT,
    OPEN_BRACKET                    TEXT,
    CLOSE_BRACKET                   TEXT,
    COMMA_CHARACTER                 TEXT,
    DOT_CHARACTER                   TEXT,
    UNDERSCORE_CHARACTER            TEXT,
    LEFT_BRACE_CHARACTER            TEXT,
    RIGHT_BRACE_CHARACTER           TEXT,
    REGEX_MULTIPLE_SPACES           TEXT,
    SUBSTITUTION_CHARACTER_ONE      TEXT,
    TAB_CHARACTER                   TEXT,
    TYPECAST_AS_BIGINT              TEXT,
    DOUBLE_SPACE_CHARACTERS         TEXT,
    QUOTE_COMMA_CHARACTERS          TEXT,
--
-- Date Fuctions
------------------------------------------------------------------------------------------------------------------------------------
    DATE_TIME_MASK                  TEXT,
--
-- SQL Statement commands
------------------------------------------------------------------------------------------------------------------------------------
    ADD_COLUMN                      TEXT,
    ALTER_TABLE                     TEXT,
    AND_COMMAND                     TEXT,
    AS_COMMAND                      TEXT,
    CONSTRAINT_COMMAND              TEXT,
    CREATE_INDEX                    TEXT,
    CREATE_TABLE                    TEXT,
    DELETE_COMMAND                  TEXT,
    DELETE_FROM                     TEXT,
    DROP_COLUMN                     TEXT,
    DROP_TABLE                      TEXT,
    EQUALS_NULL                     TEXT,
    FROM_COMMAND                    TEXT,
    GRANT_SELECT_ON                 TEXT,
    IN_ROWID_LIST                   TEXT,
    IN_SELECT_COMMAND               TEXT,
    INSERT_COMMAND                  TEXT,
    INSERT_INTO                     TEXT,
    NOT_NULL                        TEXT,
    ON_COMMAND                      TEXT,
    OR_COMMAND                      TEXT,
    ORDER_BY_COMMAND                TEXT,
    SELECT_COMMAND                  TEXT,
    SELECT_TRUE_FROM                TEXT,
    SET_COMMAND                     TEXT,
    TO_COMMAND                      TEXT,
    TRUNCATE_TABLE                  TEXT,
    UNIQUE_COMMAND                  TEXT,
    UPDATE_COMMAND                  TEXT,
    WHERE_COMMAND                   TEXT,
    WHERE_NO_DATA                   TEXT,
    LEFT_OUTER_JOIN                 TEXT,
    RIGHT_OUTER_JOIN                TEXT,
--
-- Table and column name definitions
------------------------------------------------------------------------------------------------------------------------------------
    BITMAP_COLUMN                   TEXT,
    BITMAP_COLUMN_FORMAT            TEXT,
    DMLTYPE_COLUMN                  TEXT,
    DMLTYPE_COLUMN_FORMAT           TEXT,
    ALL_BITMAP_VALUE                TEXT,
    MV_LOG_TABLE_PREFIX             TEXT,
    MV_INDEX_SUFFIX                 TEXT,
    MV_M_ROW$_COLUMN                TEXT,
    MV_M_ROW$_DEFAULT_VALUE         TEXT,
    MV_M_ROW$_COLUMN_FORMAT         TEXT,
    MV_M_ROW$_NOT_NULL_FORMAT       TEXT,
    MV_M_ROW$_SOURCE_COLUMN         TEXT,
    MV_M_ROW$_SOURCE_COLUMN_FORMAT  TEXT,
    MV_SEQUENCE$_COLUMN             TEXT,
    MV_TIMESTAMP_COLUMN             TEXT,
    MV_TIMESTAMP_COLUMN_FORMAT      TEXT,
    MV_TRIGGER_PREFIX               TEXT,
    SEQUENCE$_PK_COLUMN_FORMAT      TEXT,
--
-- Materialied View Log Table commands
------------------------------------------------------------------------------------------------------------------------------------
    AND_TABLE_NAME_EQUALS           TEXT,
    FROM_PG$MVIEW_LOGS              TEXT,
    MV_LOG$_INSERT_COLUMNS          TEXT,
    MV_LOG$_SELECT_M_ROW$           TEXT,
    MV_LOG$_WHERE_BITMAP$           TEXT,
    MV_LOG$_SELECT_M_ROWS_ORDER_BY  TEXT,
    MV_LOG$_DECREMENT_BITMAP        TEXT,
    MV_LOG$_WHERE_BITMAP_ZERO       TEXT,
    PG_MVIEW_BITMAP                 TEXT,
    WHERE_OWNER_EQUALS              TEXT,
--
-- SQL String Passing commands
------------------------------------------------------------------------------------------------------------------------------------
    DELETE_DML_TYPE                 TEXT,
    INNER_DML_TYPE                  TEXT,
    INSERT_DML_TYPE                 TEXT,
    FROM_DML_TYPE                   TEXT,
    LEFT_DML_TYPE                   TEXT,
    JOIN_DML_TYPE                   TEXT,
    ON_DML_TYPE                     TEXT,
    OUTER_DML_TYPE                  TEXT,
    RIGHT_DML_TYPE                  TEXT,
    SELECT_DML_TYPE                 TEXT,
    UPDATE_DML_TYPE                 TEXT,
    WHERE_DML_TYPE                  TEXT,
--
-- Row Identification manipulation
------------------------------------------------------------------------------------------------------------------------------------
    AND_M_ROW$_EQUALS               TEXT,
    WHERE_M_ROW$_EQUALS             TEXT,
    AND_SEQUENCE_EQUALS             TEXT,
    SELECT_M_ROW$_SOURCE_COLUMN     TEXT,
--
-- Commands to modify source table
------------------------------------------------------------------------------------------------------------------------------------
    ADD_M_ROW$_COLUMN_TO_TABLE      TEXT,
    DROP_M_ROW$_COLUMN_FROM_TABLE   TEXT,
--
-- Materialied View Trigger commands
------------------------------------------------------------------------------------------------------------------------------------
    TRIGGER_AFTER_DML               TEXT,
    TRIGGER_CREATE                  TEXT,
    TRIGGER_DROP                    TEXT,
    TRIGGER_FOR_EACH_ROW            TEXT,
--
-- Structure of the Materialized View Log
------------------------------------------------------------------------------------------------------------------------------------
    MV_LOG_COLUMNS                  TEXT,
--
    HELP_TEXT                       TEXT
);

CREATE
TYPE    mv$bitValue
AS
(
    BIT_VALUE                       SMALLINT,
    BIT_ROW                         SMALLINT,
    ROW_BIT                         SMALLINT,
    BIT_MAP                         BIGINT
);
