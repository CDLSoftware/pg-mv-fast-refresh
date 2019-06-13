/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvConstants.sql
Author:       Mike Revitt
Date:         12/011/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
04/06/2019  | M Revitt      | Add drop commands
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    PostGre does not support database packages and therefore database package variables so to overcome this I have
                created four functions to return both string and integer constants.

                This script contains a total of four functions
                o   One for string constants
                o   One for integer constants
                o   Two that are shortened, and overloaded versions of these to make the code easier to read

Notes:          The functions in this script are references by bot hthe simple and complex functions and so must be run first
                when setting up the environment

                All of the constants in this file are categorised and kept in alphabetic order within these categories,
                this dicipline should be retained with all updates

                The two overloaded and shortened function names must appear at the end of the file

                Help can be invoked by running the rollowing command from within PostGre

                DO $$ BEGIN RAISE NOTICE '%', mv$stringConstants('HELP_TEXT'; END $$;

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

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

-- psql -h localhost -p 5432 -d postgres -U mike_pgmview -q -f SaveSet/mvConstants.sql

-- -------------------- Write DROP-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP TYPE     IF EXISTS mv$allConstants CASCADE;
DROP FUNCTION IF EXISTS mv$buildAllConstants;
DROP FUNCTION IF EXISTS mv$buildTriggerConstants;

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
    NO_PARENT_TOKEN                 TEXT,
    COMMA_INNER_TOKEN               TEXT,
    COMMA_LEFT_TOKEN                TEXT,
    RIGHT_COMMA_TOKEN               TEXT,
--
-- Maths Commands
------------------------------------------------------------------------------------------------------------------------------------
    BASE_TWO                        INTEGER,
    BITAND_COMMAND                  TEXT,
    BIT_NOT_SET                     TEXT,
    BITMAP_NOT_SET                  INTEGER,
    EQUALS_COMMAND                  TEXT,
    FIRST_PGMVIEW_BIT               SMALLINT,
    LESS_THAN_EQUAL                 TEXT,
    MAX_INTEGER_SIZE                INTEGER,
    MAX_PGMVIEWS_PER_TABLE          INTEGER,
    MV_MAX_BASE_TABLE_LEN           INTEGER,
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
    SUBSTITUTION_CHARACTER_TWO      TEXT,
    TYPECAST_AS_INTEGER             TEXT,
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
    CREATE_VIEW                     TEXT,
    CREATE_VIEW_AS                  TEXT,
    DELETE_COMMAND                  TEXT,
    DELETE_FROM                     TEXT,
    DROP_COLUMN                     TEXT,
    DROP_TABLE                      TEXT,
    DROP_VIEW                       TEXT,
    FROM_COMMAND                    TEXT,
    GRANT_SELECT_ON                 TEXT,
    IN_ROWID_LIST                   TEXT,
    IN_SELECT_COMMAND               TEXT,
    INSERT_COMMAND                  TEXT,
    INSERT_INTO                     TEXT,
    NOT_NULL                        TEXT,
    ON_COMMAND                      TEXT,
    ORDER_BY_COMMAND                TEXT,
    PRIMARY_KEY_COMMAND             TEXT,
    SELECT_COMMAND                  TEXT,
    SELECT_TRUE_FROM                TEXT,
    SET_COMMAND                     TEXT,
    TO_COMMAND                      TEXT,
    TRUNCATE_TABLE                  TEXT,
    UNIQUE_COMMAND                  TEXT,
    UPDATE_COMMAND                  TEXT,
    WHERE_COMMAND                   TEXT,
    WHERE_NO_DATA                   TEXT,
--
-- Table and column name definitions
------------------------------------------------------------------------------------------------------------------------------------
    BITMAP_COLUMN                   TEXT,
    BITMAP_COLUMN_FORMAT            TEXT,
    DMLTYPE_COLUMN                  TEXT,
    DMLTYPE_COLUMN_FORMAT           TEXT,
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
    MV_PGMV_TABLE_PREFIX            TEXT,
    MV_TRIGGER_PREFIX               TEXT,
    SEQUENCE$_PK_COLUMN_FORMAT      TEXT,
--
-- Materialied View Log Table commands
------------------------------------------------------------------------------------------------------------------------------------
    MV_LOG$_INSERT_COLUMNS          TEXT,
    MV_LOG$_INSERT_VALUES_START     TEXT,
    MV_LOG$_INSERT_VALUES_END       TEXT,
    MV_LOG$_SELECT_M_ROW$           TEXT,
    MV_LOG$_WHERE_BITMAP$           TEXT,
    MV_LOG$_SELECT_M_ROWS_ORDER_BY  TEXT,
    MV_LOG$_DECREMENT_BITMAP        TEXT,
    MV_LOG$_WHERE_BITMAP_ZERO       TEXT,
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

----------------------- Write CREATE-FUNCTION-stage scripts ------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$buildTriggerConstants()
    RETURNS mv$allConstants
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$buildTriggerConstants
Author:       Mike Revitt
Date:         07/05/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
07/05/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Populates all of teh constant variables to be used by teh program

Arguments:      IN      NONE
Returns:                mv$allConstants     The type that contains all of the CONSTANTS

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rMvConstants    mv$allConstants;

BEGIN

-- Characters used in string constructs
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.SINGLE_QUOTE_CHARACTER         := CHR(39);
    rMvConstants.OPEN_BRACKET                   := CHR(40);
    rMvConstants.CLOSE_BRACKET                  := CHR(41);
    rMvConstants.COMMA_CHARACTER                := CHR(44);
    rMvConstants.DOT_CHARACTER                  := CHR(46);
    rMvConstants.QUOTE_COMMA_CHARACTERS         :=  rMvConstants.SINGLE_QUOTE_CHARACTER || rMvConstants.COMMA_CHARACTER;

-- Maths Commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.BITMAP_NOT_SET                 := 0;

-- SQL String Passing commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.DELETE_DML_TYPE                := 'DELETE';

-- Table and column name definitions
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.BITMAP_COLUMN                  := 'bitmap$';
    rMvConstants.DMLTYPE_COLUMN                 := 'dmltype$';
    rMvConstants.MV_M_ROW$_COLUMN               := 'm_row$';

-- SQL Statement commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.INSERT_INTO                    := 'INSERT INTO ';

-- Materialied View Log Table commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.MV_LOG$_INSERT_COLUMNS         :=  rMvConstants.OPEN_BRACKET               ||
                                                        rMvConstants.MV_M_ROW$_COLUMN       || rMvConstants.COMMA_CHARACTER ||
                                                        rMvConstants.BITMAP_COLUMN          || rMvConstants.COMMA_CHARACTER ||
                                                        rMvConstants.DMLTYPE_COLUMN         ||
                                                    rMvConstants.CLOSE_BRACKET;
    rMvConstants.MV_LOG$_INSERT_VALUES_START    := 'VALUES( ''';
    rMvConstants.MV_LOG$_INSERT_VALUES_END      := ''')';

    RETURN( rMvConstants );
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

----------------------- Write CREATE-FUNCTION-stage scripts ------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$buildAllConstants()
    RETURNS mv$allConstants
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$buildAllConstants
Author:       Mike Revitt
Date:         26/04/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
26/04/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Populates all of teh constant variables to be used by teh program

Arguments:      IN      NONE
Returns:                mv$allConstants     The type that contains all of the CONSTANTS

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rMvConstants    mv$allConstants;

BEGIN

    rMvConstants := mv$buildTriggerConstants();

-- Database Role used to access materialized views
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.PGMV_SELECT_ROLE               := 'pgmv$_role';

-- Characters used FOR string delimination
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.INNER_TOKEN                    := CHR(1);
    rMvConstants.FROM_TOKEN                     := CHR(2);
    rMvConstants.JOIN_TOKEN                     := CHR(3);
    rMvConstants.LEFT_TOKEN                     := CHR(4);
    rMvConstants.ON_TOKEN                       := CHR(5);
    rMvConstants.OUTER_TOKEN                    := CHR(6);
    rMvConstants.RIGHT_TOKEN                    := CHR(7);
    rMvConstants.WHERE_TOKEN                    := CHR(8);
    rMvConstants.NO_PARENT_TOKEN                := CHR(17);
    rMvConstants.COMMA_INNER_TOKEN              := CHR(44)  || rMvConstants.INNER_TOKEN;
    rMvConstants.COMMA_LEFT_TOKEN               := CHR(44)  || rMvConstants.LEFT_TOKEN;
    rMvConstants.RIGHT_COMMA_TOKEN              := rMvConstants.RIGHT_TOKEN || CHR(44);

-- Maths Commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.BASE_TWO                       := 2;
    rMvConstants.BITAND_COMMAND                 := ' & ';
    rMvConstants.BIT_NOT_SET                    := ' 0';
    rMvConstants.EQUALS_COMMAND                 := ' = ';
    rMvConstants.FIRST_PGMVIEW_BIT              := 0;
    rMvConstants.LESS_THAN_EQUAL                := ' <= ';
    rMvConstants.MAX_INTEGER_SIZE               := 2147483647; -- 2^31
    rMvConstants.MAX_PGMVIEWS_PER_TABLE         := 20;
    rMvConstants.MV_MAX_BASE_TABLE_LEN          := 22;
    rMvConstants.SUBTRACT_COMMAND               := ' - ';
    rMvConstants.TWO_TO_THE_POWER_OF            := ' POWER( 2, ';

-- Characters used in string constructs
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.EMPTY_STRING                   := '';
    rMvConstants.NEW_LINE                       := CHR(10);
    rMvConstants.CARRIAGE_RETURN                := CHR(13);
    rMvConstants.SPACE_CHARACTER                := CHR(32);
    rMvConstants.UNDERSCORE_CHARACTER           := CHR(95);
    rMvConstants.LEFT_BRACE_CHARACTER           := CHR(123);
    rMvConstants.RIGHT_BRACE_CHARACTER          := CHR(125);
    rMvConstants.REGEX_MULTIPLE_SPACES          := '\s+';
    rMvConstants.SUBSTITUTION_CHARACTER_ONE     := '$1';
    rMvConstants.SUBSTITUTION_CHARACTER_TWO     := '$2';
    rMvConstants.TYPECAST_AS_INTEGER            := '::INTEGER';
    rMvConstants.DOUBLE_SPACE_CHARACTERS        :=  rMvConstants.SPACE_CHARACTER || rMvConstants.SPACE_CHARACTER;

-- Date Fuctions
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.DATE_TIME_MASK                 := '''DD-MON-YYYY HH24:MI:SS''';

-- SQL Statement commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.ADD_COLUMN                     := ' ADD COLUMN ';
    rMvConstants.ALTER_TABLE                    := 'ALTER TABLE ';
    rMvConstants.AND_COMMAND                    := ' AND ';
    rMvConstants.AS_COMMAND                     := ' AS ';
    rMvConstants.CONSTRAINT_COMMAND             := ' CONTRAINT ';
    rMvConstants.CREATE_INDEX                   := 'CREATE INDEX ';
    rMvConstants.CREATE_TABLE                   := 'CREATE TABLE ';
    rMvConstants.CREATE_VIEW                    := 'CREATE VIEW ';
    rMvConstants.CREATE_VIEW_AS                 := ' AS SELECT * FROM ';
    rMvConstants.DELETE_COMMAND                 := 'DELETE ';
    rMvConstants.DELETE_FROM                    := 'DELETE FROM ';
    rMvConstants.DROP_COLUMN                    := ' DROP COLUMN ';
    rMvConstants.DROP_TABLE                     := 'DROP TABLE ';
    rMvConstants.DROP_VIEW                      := 'DROP VIEW ';
    rMvConstants.FROM_COMMAND                   := ' FROM ';
    rMvConstants.GRANT_SELECT_ON                := 'GRANT SELECT ON ';
    rMvConstants.IN_ROWID_LIST                  := ' IN ( SELECT UNNEST($1))';
    rMvConstants.IN_SELECT_COMMAND              := ' IN ( SELECT ';
    rMvConstants.INSERT_COMMAND                 := 'INSERT ';
    rMvConstants.INSERT_INTO                    := 'INSERT INTO ';
    rMvConstants.NOT_NULL                       := ' NOT NULL ';
    rMvConstants.ON_COMMAND                     := ' ON ';
    rMvConstants.ORDER_BY_COMMAND               := ' ORDER BY ';
    rMvConstants.PRIMARY_KEY_COMMAND            := ' PRIMARY KEY ';
    rMvConstants.SELECT_COMMAND                 := 'SELECT ';
    rMvConstants.SELECT_TRUE_FROM               := 'SELECT TRUE FROM ';
    rMvConstants.SET_COMMAND                    := ' SET ';
    rMvConstants.TO_COMMAND                     := ' TO ';
    rMvConstants.TRUNCATE_TABLE                 := 'TRUNCATE TABLE ';
    rMvConstants.UNIQUE_COMMAND                 := ' UNIQUE ';
    rMvConstants.UPDATE_COMMAND                 := 'UPDATE ';
    rMvConstants.WHERE_COMMAND                  := ' WHERE ';
    rMvConstants.WHERE_NO_DATA                  := ' WHERE 1 = 2 ';

-- Table and column name definitions
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.BITMAP_COLUMN_FORMAT           := ' INTEGER NOT NULL DEFAULT 0 ';
    rMvConstants.DMLTYPE_COLUMN_FORMAT          := ' CHAR(7) NOT NULL ';
    rMvConstants.MV_INDEX_SUFFIX                := '_key';
    rMvConstants.MV_LOG_TABLE_PREFIX            := 'log$_';
    rMvConstants.MV_M_ROW$_COLUMN_FORMAT        := ' UUID ';
    rMvConstants.MV_M_ROW$_NOT_NULL_FORMAT      := rMvConstants.MV_M_ROW$_COLUMN_FORMAT || rMvConstants.NOT_NULL;
    rMvConstants.MV_M_ROW$_DEFAULT_VALUE        := ' DEFAULT uuid_generate_v4()';
    rMvConstants.MV_M_ROW$_SOURCE_COLUMN        := rMvConstants.MV_M_ROW$_COLUMN || rMvConstants.SPACE_CHARACTER;
    rMvConstants.MV_M_ROW$_SOURCE_COLUMN_FORMAT := ' INTEGER ';
    rMvConstants.MV_PGMV_TABLE_PREFIX           := 'pgmv$_';
    rMvConstants.MV_TIMESTAMP_COLUMN            := 'snaptime$';
    rMvConstants.MV_TIMESTAMP_COLUMN_FORMAT     := ' DATE NOT NULL DEFAULT CLOCK_TIMESTAMP() ';
    rMvConstants.MV_TRIGGER_PREFIX              := 'trig$_';
    rMvConstants.MV_SEQUENCE$_COLUMN            := 'sequence$';
    rMvConstants.SEQUENCE$_PK_COLUMN_FORMAT     := ' BIGSERIAL NOT NULL PRIMARY KEY';

-- Materialied View Log Table commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.MV_LOG$_INSERT_VALUES_START    := 'VALUES( ''';
    rMvConstants.MV_LOG$_INSERT_VALUES_END      := '''))';
    rMvConstants.MV_LOG$_SELECT_M_ROW$          :=  rMvConstants.SELECT_COMMAND             ||
                                                        rMvConstants.MV_M_ROW$_COLUMN       || rMvConstants.COMMA_CHARACTER ||
                                                        rMvConstants.MV_SEQUENCE$_COLUMN    || rMvConstants.COMMA_CHARACTER ||
                                                        rMvConstants.DMLTYPE_COLUMN         ||
                                                    rMvConstants.FROM_COMMAND;
    rMvConstants.MV_LOG$_WHERE_BITMAP$          :=  rMvConstants.WHERE_COMMAND              ||
                                                    rMvConstants.BITMAP_COLUMN              || rMvConstants.BITAND_COMMAND  ||
                                                    rMvConstants.TWO_TO_THE_POWER_OF        ||
                                                    rMvConstants.SUBSTITUTION_CHARACTER_ONE || rMvConstants.CLOSE_BRACKET   ||
                                                    rMvConstants.TYPECAST_AS_INTEGER        || rMvConstants.EQUALS_COMMAND  ||
                                                    rMvConstants.TWO_TO_THE_POWER_OF        ||
                                                    rMvConstants.SUBSTITUTION_CHARACTER_TWO || rMvConstants.CLOSE_BRACKET   ||
                                                    rMvConstants.TYPECAST_AS_INTEGER;
    rMvConstants.MV_LOG$_SELECT_M_ROWS_ORDER_BY :=  rMvConstants.ORDER_BY_COMMAND           || rMvConstants.MV_SEQUENCE$_COLUMN;
    rMvConstants.MV_LOG$_DECREMENT_BITMAP       :=  rMvConstants.BITMAP_COLUMN              || rMvConstants.EQUALS_COMMAND  ||
                                                    rMvConstants.BITMAP_COLUMN              || rMvConstants.SUBTRACT_COMMAND||
                                                    rMvConstants.TWO_TO_THE_POWER_OF;
    rMvConstants.MV_LOG$_WHERE_BITMAP_ZERO      :=  rMvConstants.WHERE_COMMAND              ||
                                                    rMvConstants.BITMAP_COLUMN              || rMvConstants.EQUALS_COMMAND  ||
                                                    rMvConstants.BIT_NOT_SET;

-- SQL String Passing commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.INNER_DML_TYPE                 := 'INNER';
    rMvConstants.INSERT_DML_TYPE                := 'INSERT';
    rMvConstants.FROM_DML_TYPE                  := 'FROM';
    rMvConstants.LEFT_DML_TYPE                  := 'LEFT';
    rMvConstants.JOIN_DML_TYPE                  := 'JOIN';
    rMvConstants.ON_DML_TYPE                    := 'ON';
    rMvConstants.OUTER_DML_TYPE                 := 'OUTER';
    rMvConstants.RIGHT_DML_TYPE                 := 'RIGHT';
    rMvConstants.SELECT_DML_TYPE                := 'SELECT';
    rMvConstants.UPDATE_DML_TYPE                := 'UPDATE';
    rMvConstants.WHERE_DML_TYPE                 := 'WHERE';

-- Row Identification manipulation
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.AND_M_ROW$_EQUALS              :=  rMvConstants.AND_COMMAND             ||
                                                    rMvConstants.MV_M_ROW$_COLUMN        || rMvConstants.EQUALS_COMMAND;
    rMvConstants.AND_SEQUENCE_EQUALS            :=  rMvConstants.AND_COMMAND             ||
                                                    rMvConstants.MV_SEQUENCE$_COLUMN     || rMvConstants.EQUALS_COMMAND;
    rMvConstants.SELECT_M_ROW$_SOURCE_COLUMN    :=  rMvConstants.SELECT_COMMAND          || rMvConstants.MV_M_ROW$_SOURCE_COLUMN ||
                                                    rMvConstants.FROM_COMMAND;
    rMvConstants.WHERE_M_ROW$_EQUALS            :=  rMvConstants.WHERE_COMMAND           ||
                                                    rMvConstants.MV_M_ROW$_COLUMN        || rMvConstants.EQUALS_COMMAND;

-- Commands to modify source table
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.ADD_M_ROW$_COLUMN_TO_TABLE     :=  rMvConstants.ADD_COLUMN              ||
                                                    rMvConstants.MV_M_ROW$_COLUMN        || rMvConstants.MV_M_ROW$_NOT_NULL_FORMAT||
                                                    rMvConstants.MV_M_ROW$_DEFAULT_VALUE || rMvConstants.UNIQUE_COMMAND;
    rMvConstants.DROP_M_ROW$_COLUMN_FROM_TABLE  :=  rMvConstants.DROP_COLUMN             || rMvConstants.MV_M_ROW$_COLUMN;

-- Materialied View Trigger commands
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.TRIGGER_AFTER_DML              := ' AFTER DELETE OR INSERT OR UPDATE ON ';
    rMvConstants.TRIGGER_CREATE                 := 'CREATE TRIGGER ';
    rMvConstants.TRIGGER_DROP                   := 'DROP TRIGGER ';
    rMvConstants.TRIGGER_FOR_EACH_ROW           := ' FOR EACH ROW EXECUTE PROCEDURE mv$insertMaterializedViewLogRow();';

-- Structure of the Materialized View Log
------------------------------------------------------------------------------------------------------------------------------------
    rMvConstants.MV_LOG_COLUMNS     :=
        rMvConstants.OPEN_BRACKET   ||
            rMvConstants.MV_SEQUENCE$_COLUMN    || rMvConstants.SEQUENCE$_PK_COLUMN_FORMAT  || rMvConstants.COMMA_CHARACTER ||
            rMvConstants.MV_M_ROW$_COLUMN       || rMvConstants.MV_M_ROW$_NOT_NULL_FORMAT   || rMvConstants.COMMA_CHARACTER ||
            rMvConstants.BITMAP_COLUMN          || rMvConstants.BITMAP_COLUMN_FORMAT        || rMvConstants.COMMA_CHARACTER ||
            rMvConstants.MV_TIMESTAMP_COLUMN    || rMvConstants.MV_TIMESTAMP_COLUMN_FORMAT  || rMvConstants.COMMA_CHARACTER ||
            rMvConstants.DMLTYPE_COLUMN         || rMvConstants.DMLTYPE_COLUMN_FORMAT       ||
        rMvConstants.CLOSE_BRACKET;

    rMvConstants.HELP_TEXT              := '
+---------------------------------------------------------------------------------------------------------------------------------+
| The program is devided into two sections                                                                                        |
| o  Functions that are designed to be used internally to facilitate the running of the program                                   |
| o  Functions that are designed to be called to manage the Materilaised views and within this category there are three further   |
|    sections                                                                                                                     |
|       o   Functions that are used for the management of Materialized View Objects                                               |
|       o   Functions that refresh the materialized views                                                                         |
|       o   Functions that provide help                                                                                           |
|                                                                                                                                 |
| o  Management Functions, of which there are two commands;                                                                       |
|       o   mv$createMaterializedViewlog                                                                                          |
|           This function creates a materialized view log, which is mandatory for fast refresh materialized views, sets up the    |
|           PostGre equivalent rowid tracking on the base table, adds a database trigger to the base table and populates the      |
|           data dictionary tables                                                                                                |
|       o   mv$createMaterializedView                                                                                             |
|           This function creates the underlying table for the materialized view and then creates a view on top of it, populates  |
|           the data dictionary tables before calling the full refresh routine to populate it.                                    |
|                                                                                                                                 |
| o  Refresh Function, of which there is one that is designed to be called by external functions.                                 |
|       o   mv$refreshMaterializedView                                                                                            |
|                                                                                                                                 |
| o  Help Message                                                                                                                 |
|       o   RAISE NOTICE ''%'', rMvConstants.HELP_TEXT;                                                                             |
|           This message                                                                                                          |
+---------------------------------------------------------------------------------------------------------------------------------+
';

    RETURN( rMvConstants );
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

DO $$ DECLARE
    rMvConstants    mv$allConstants;
BEGIN
    rMvConstants := mv$buildAllConstants();
    RAISE NOTICE '% %', rMvConstants.HELP_TEXT, rMvConstants.NEW_LINE;
END $$;
