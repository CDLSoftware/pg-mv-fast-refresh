/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: CreateCdlSnapshot.sql
Author:       Mike Revitt
Date:         08/04/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
08/04/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This script creates a number of materialized views that cover every possible column type combination.

                It does this by creating 13 base tables that are then used in a number of differnt materialzed views

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
***********************************************************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U mike_pgmview -q -f createCdlSnapshot.sql

SET CLIENT_MIN_MESSAGES = ERROR;

DROP VIEW       IF EXISTS cdl_mv1;
DROP VIEW       IF EXISTS cdl_mv2;
DROP VIEW       IF EXISTS cdl_mv3;
DROP VIEW       IF EXISTS cdl_mv4;
DROP TABLE      IF EXISTS pgmv$_cdl_mv1     CASCADE;
DROP TABLE      IF EXISTS pgmv$_cdl_mv2     CASCADE;
DROP TABLE      IF EXISTS pgmv$_cdl_mv3     CASCADE;
DROP TABLE      IF EXISTS pgmv$_cdl_mv4     CASCADE;

DO $DROP$
DECLARE
    iLoopCounter    INTEGER;
    iNoOfTables     INTEGER := 13;
    tSqlStatement   TEXT;
BEGIN

    FOR iLoopCounter IN 1..iNoOfTables
    LOOP
        tSqlStatement := 'DROP SEQUENCE IF EXISTS seq$_c'  || iLoopCounter;
        EXECUTE tSqlStatement;

        tSqlStatement := 'DROP TRIGGER  IF EXISTS trig$_c' || iLoopCounter || ' ON c' || iLoopCounter;
        EXECUTE tSqlStatement;

        tSqlStatement := 'DROP TABLE    IF EXISTS log$_c'  || iLoopCounter || ' CASCADE';
        EXECUTE tSqlStatement;

        tSqlStatement := 'DROP TABLE    IF EXISTS c'       || iLoopCounter || ' CASCADE';
        EXECUTE tSqlStatement;
    END LOOP;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END $DROP$
LANGUAGE  plpgsql;

SET CLIENT_MIN_MESSAGES = NOTICE;

\prompt "Create base tables c1 thru c13 and then populate them with data" mike

CREATE  TABLE
        c1
(
    code            INTEGER NOT NULL    PRIMARY     KEY,
    created         DATE    NOT NULL    DEFAULT     clock_timestamp(),
    description     TEXT        NULL,
    updated         DATE        NULL
);

CREATE  OR  REPLACE
FUNCTION    updateC1Updated()
    RETURNS TRIGGER
AS
$TRIG$
BEGIN
    NEW.updated = clock_timestamp();
    RETURN  NEW;
END;
$TRIG$
LANGUAGE  plpgsql;

CREATE  TRIGGER updateC1UpdatedCol
    BEFORE  UPDATE  ON  c1
    FOR     EACH    ROW
    EXECUTE PROCEDURE   updateC1Updated();

DO $$
DECLARE
    iLoopCounter    INTEGER;
    iNoOfTables     INTEGER := 13;
    tSqlStatement   TEXT;
BEGIN

    FOR iLoopCounter IN 2..iNoOfTables
    LOOP
        tSqlStatement :=
            'CREATE  TABLE c' || iLoopCounter ||
            '(
                code            INTEGER NOT NULL    PRIMARY     KEY,
                parent          INTEGER NOT NULL    REFERENCES  c' || iLoopCounter - 1 ||',
                created         DATE    NOT NULL    DEFAULT     clock_timestamp(),
                description     TEXT        NULL,
                updated         DATE        NULL
            )';


        EXECUTE tSqlStatement;

        tSqlStatement :=
            'CREATE  OR  REPLACE
            FUNCTION    updateC' || iLoopCounter || 'Updated()
                RETURNS TRIGGER
            AS
            $TRIG$
            BEGIN
                NEW.updated = clock_timestamp();
                RETURN  NEW;
            END;
            $TRIG$
            LANGUAGE  plpgsql';

        EXECUTE tSqlStatement;

        tSqlStatement :=
            'CREATE  TRIGGER updateC'       || iLoopCounter || 'UpdatedCol
                BEFORE  UPDATE  ON c'       || iLoopCounter || '
                FOR     EACH    ROW
                EXECUTE PROCEDURE  updateC' || iLoopCounter ||'Updated()';

        EXECUTE tSqlStatement;
    END LOOP;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END $$
LANGUAGE  plpgsql;

\prompt "Create materialized view logs" mike

DO $$
DECLARE
    cResult         CHAR(1) := NULL;
    iNoOfTables     INTEGER := 13;
BEGIN

    FOR iLoopCounter IN 1..iNoOfTables
    LOOP
        cResult := mv$createMaterializedViewlog( 'c' || iLoopCounter );
    END LOOP;
END $$;

\prompt "Create materialized view cdl_mv1 using INNER JOIN based on mv_busevent" mike

DO $$
DECLARE
    tStartTime      TIMESTAMP   := clock_timestamp();
    cResult         CHAR(1)     := NULL;
    tSqlStatement   TEXT;
BEGIN
    tSqlStatement := '
        SELECT
                c1.code  c1_code,  c1.description  c1_description,
                c2.code  c2_code,  c2.description  c2_description,
                c3.code  c3_code,  c3.description  c3_description,
                c4.code  c4_code,  c4.description  c4_description,
                c5.code  c5_code,  c5.description  c5_description,
                c6.code  c6_code,  c6.description  c6_description,
                c7.code  c7_code,  c7.description  c7_description,
                c8.code  c8_code,  c8.description  c8_description,
                c9.code  c9_code,  c9.description  c9_description,
                c10.code c10_code, c10.description c10_description,
                c11.code c11_code, c11.description c11_description,
                c12.code c12_code, c12.description c12_description,
                c13.code c13_code, c13.description c13_description
        FROM        c1
        INNER JOIN  c2  c2  ON c1.code   = c2.parent
        INNER JOIN  c3  c3  ON c2.code   = c3.parent
        INNER JOIN  c4      ON c3.code   = c4.parent
        INNER JOIN  c5      ON c4.code   = c5.parent
        INNER JOIN  c6      ON c5.code   = c6.parent
        INNER JOIN  c7      ON c6.code   = c7.parent
        INNER JOIN  c8      ON c7.code   = c8.parent
        INNER JOIN  c9      ON c8.code   = c9.parent
        INNER JOIN  c10     ON c9.code   = c10.parent
        INNER JOIN  c11     ON c10.code  = c11.parent
        INNER JOIN  c12     ON c11.code  = c12.parent
        INNER JOIN  c13     ON c12.code  = c13.parent
        WHERE c1.created > TO_DATE(''20170103'',''YYYYMMDD'')';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'cdl_mv1',
        pSelectStatement    =>  tSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Data Dictionary Tables cdl_mv1'
SELECT
        view_name,
        select_columns
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv1';

SELECT
        view_name,
        where_clause,
        log_array,
        bit_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv1';

SELECT
        view_name,
        table_array,
        alias_array,
        rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv1';

SELECT
        view_name,
        outer_table_array,
        parent_table_array,
        parent_alias_array,
        parent_rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv1';

\prompt "Create materialized view cdl_mv2 using INNER and LEFT JOIN based on mv_named_driver" mike

DO $$
DECLARE
    tStartTime      TIMESTAMP   := clock_timestamp();
    cResult         CHAR(1)     := NULL;
    tSqlStatement   TEXT;
BEGIN
    tSqlStatement := '
        SELECT
                a1.code  c1_code,  a1.description  c1_description,
                a2.code  c2_code,  a2.description  c2_description,
                a3.code  c3_code,  a3.description  c3_description,
                a4.code  c4_code,  a4.description  c4_description,
                a5.code  c5_code,  a5.description  c5_description,
                a6.code  c6_code,  a6.description  c6_description,
                a7.code  c7_code,  a7.description  c7_description,
                a8.code  c8_code,  a8.description  c8_description,
                a9.code  c9_code,  a9.description  c9_description,
                a10.code c10_code, a10.description c10_description,
                a11.code c11_code, a11.description c11_description,
                a12.code c12_code, a12.description c12_description
        FROM        c1  a1
        INNER JOIN  c2  a2  ON a1.code   = a2.parent
        INNER JOIN  c3  a3  ON a2.code   = a3.parent
        INNER JOIN  c4  a4  ON a3.code   = a4.parent
        INNER JOIN  c5  a5  ON a4.code   = a5.parent
        LEFT  JOIN  c6  a6  ON a5.code   = a6.parent
        LEFT  JOIN  c7  a7  ON a6.code   = a7.parent
        LEFT  JOIN  c8  a8  ON a7.code   = a8.parent
        LEFT  JOIN  c9  a9  ON a8.code   = a9.parent
        LEFT  JOIN  c10 a10 ON a9.code   = a10.parent
        LEFT  JOIN  c11 a11 ON a10.code  = a11.parent
        LEFT  JOIN  c12 a12 ON a11.code  = a12.parent
        WHERE
            a1.code IS NOT NULL
        AND a1.created > TO_DATE(''20170103'',''YYYYMMDD'')';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'cdl_mv2',
        pSelectStatement    =>  tSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Data Dictionary Tables cdl_mv2'
SELECT
        view_name,
        select_columns
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv2';

SELECT
        view_name,
        where_clause,
        log_array,
        bit_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv2';

SELECT
        view_name,
        table_array,
        alias_array,
        rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv2';

SELECT
        view_name,
        outer_table_array,
        parent_table_array,
        parent_alias_array,
        parent_rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv2';

\prompt "Create materialized view cdl_mv3 using INNER JOIN based on mv_telephone" mike

DO $$
DECLARE
    tStartTime      TIMESTAMP   := clock_timestamp();
    cResult         CHAR(1)     := NULL;
    tSqlStatement   TEXT;
BEGIN
    tSqlStatement := '
        SELECT
                c1.code,
                c1.created,
                c1.description      AS c1_description,
                c1.updated,
                c2.parent,
                c2.description,
                c3.code             AS c3_code,
                c3.description      AS c3_description,
                c4.code             AS c4_code,
                c4.created          AS c4_created,
                c4.updated          AS c4_updated,
                c5.created          AS c5_created,
                c5.updated          AS c5_updated
        FROM        c1
        INNER JOIN  c2  c2  ON c1.code   = c2.parent
        INNER JOIN  c3  c3  ON c2.code   = c3.parent
        INNER JOIN  c4      ON c3.code   = c4.parent
        INNER JOIN  c5      ON c4.code   = c5.parent
        WHERE
            c1.code IS NOT NULL
        AND c2.created > TO_DATE(''20170103'',''YYYYMMDD'')';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'cdl_mv3',
        pSelectStatement    =>  tSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Data Dictionary Tables cdl_mv3'
SELECT
        view_name,
        select_columns
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv3';

SELECT
        view_name,
        where_clause,
        log_array,
        bit_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv3';

SELECT
        view_name,
        table_array,
        alias_array,
        rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv3';

SELECT
        view_name,
        outer_table_array,
        parent_table_array,
        parent_alias_array,
        parent_rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv3';

\prompt "Create materialized view cdl_mv4 using LEFT, RIGHT, INNER and OUTER JOIN" mike

DO $$
DECLARE
    tStartTime      TIMESTAMP   := clock_timestamp();
    cResult         CHAR(1)     := NULL;
    tSqlStatement   TEXT;
BEGIN
    tSqlStatement := '
        SELECT
                c1.code  c1_code,  c1.description  c1_description,
                c2.code  c2_code,  c2.description  c2_description,
                c3.code  c3_code,  c3.description  c3_description,
                c4.code  c4_code,  c4.description  c4_description,
                c5.code  c5_code,  c5.description  c5_description,
                c6.code  c6_code,  c6.description  c6_description,
                c7.code  c7_code,  c7.description  c7_description,
                c8.code  c8_code,  c8.description  c8_description,
                c9.code  c9_code,  c9.description  c9_description,
                c10.code c10_code, c10.description c10_description,
                c11.code c11_code, c11.description c11_description,
                c12.code c12_code, c12.description c12_description,
                c13.code c13_code, c13.description c13_description
        FROM              c1
        LEFT  OUTER JOIN  c2  c2  ON c1.code   = c2.parent
        LEFT        JOIN  c3  c3  ON c2.code   = c3.parent
        INNER       JOIN  c4  c4  ON c3.code   = c4.parent
        INNER       JOIN  c5      ON c4.code   = c5.parent
        LEFT        JOIN  c6      ON c5.code   = c6.parent
        RIGHT OUTER JOIN  c7      ON c7.parent = c6.code
        RIGHT       JOIN  c8      ON c8.parent = c7.code
        RIGHT       JOIN  c9      ON c9.parent = c8.code
        LEFT        JOIN  c10     ON c9.code   = c10.parent
        LEFT        JOIN  c11     ON c10.code  = c11.parent
        LEFT        JOIN  c12     ON c11.code  = c12.parent
        LEFT        JOIN  c13     ON c12.code  = c13.parent
        WHERE c1.created > TO_DATE(''20170103'',''YYYYMMDD'')';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'cdl_mv4',
        pSelectStatement    =>  tSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\prompt "Query data dictionary table mike$_snapshots and describe the materialized view and base pgmv$ table" mike

\C 'Data Dictionary Tables cdl_mv4'
SELECT
        view_name,
        select_columns
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv4';

SELECT
        view_name,
        where_clause,
        log_array,
        bit_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv4';

SELECT
        view_name,
        table_array,
        alias_array,
        rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv4';

SELECT
        view_name,
        outer_table_array,
        parent_table_array,
        parent_alias_array,
        parent_rowid_array
FROM
        mike$_pgmviews
WHERE
        view_name   = 'cdl_mv4';

\d cdl_mv1
\d pgmv$_cdl_mv1

