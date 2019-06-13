/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: testHarness.sql
Author:       Mike Revitt
Date:         12/011/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This script creates the SCHEMA and USER to hold the Materialized View Fast Refresh code along with the necessary
                data dictionary views

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Help:           Help can be invoked by running the rollowing command from within PostGre

                DO $$ BEGIN RAISE NOTICE '%', mv$stringConstants('HELP_TEXT'); END $$;

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
/*
psql -h localhost -p 5432 -d postgres -U mike_data -q -f testHarness.sql
*/

SET CLIENT_MIN_MESSAGES = NOTICE;
\set VERBOSITY terse

\prompt "INSERT INTO t2 WHERE code = 100" mike

INSERT  INTO
t2(     code,   parent, description,          created )
VALUES( 100,    1,     'Desctiption 100',     current_timestamp );

SELECT  *
FROM
        log$_t2;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "INSERT INTO t3 WHERE code = 1000" mike

INSERT  INTO
t3(     code,   parent, description,          created )
VALUES( 1000,   100,   'Desctiption 1000',    current_timestamp );

SELECT  *
FROM
        log$_t3;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "UPDATE t1 WHERE code = 1" mike

UPDATE  t1
SET     description = 'First Description'
WHERE   code        = 1;

SELECT  *
FROM
        log$_t1;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "UPDATE t2 WHERE code = 5" mike

UPDATE  t2
SET     description = 'Second Description'
WHERE   code        =  5;

SELECT  *
FROM
        log$_t2;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "UPDATE t3 WHERE code = 18" mike

UPDATE  t3
SET     description = 'Third Description'
WHERE   code        =  18;

SELECT  *
FROM
        log$_t3;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "DELETE FROM t3 WHERE code = 17" mike

DELETE
FROM    t3
WHERE   code    = 17;

SELECT  *
FROM
        log$_t3;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "DELETE FROM t3 WHERE parent = 6" mike

DELETE
FROM    t3
WHERE   parent  = 6;

SELECT  *
FROM
        log$_t3;

\prompt "DELETE FROM t2 WHERE code = 6" mike

DELETE
FROM    t2
WHERE   code    = 6;

SELECT  *
FROM
        log$_t2;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "DELETE FROM t3 WHERE parent IN ( SELECT code FROM t2 WHERE parent = 1 )" mike

DELETE
FROM    t3
WHERE   parent
IN(     SELECT  code
        FROM    t2
        WHERE   parent = 1
);

SELECT  *
FROM
        log$_t3;

\prompt "DELETE FROM t2 WHERE parent = 1" mike

DELETE
FROM    t2
WHERE   parent  = 1;

SELECT  *
FROM
        log$_t2;

\prompt "DELETE FROM t1 WHERE code = 1" mike

DELETE
FROM    t1
WHERE   code    = 1;

SELECT  *
FROM
        log$_t1;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\C 'Materialized View'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Base Tables'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   = t1.code
ORDER
BY      t1.code, t2.code, t3.code;

\prompt "Create materialized views 1 on each base table" mike

DO $$
DECLARE
    tStartTime      TIMESTAMP   := clock_timestamp();
    cResult         CHAR(1)     := NULL;
    pSqlStatement   TEXT;
BEGIN
    pSqlStatement := 'SELECT * FROM t1';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'mv1',
        pSelectStatement    =>  pSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );

    pSqlStatement := 'SELECT * FROM t2';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'mv2',
        pSelectStatement    =>  pSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );

    pSqlStatement := 'SELECT * FROM t3';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'mv3',
        pSelectStatement    =>  pSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );

    RAISE NOTICE 'Simple Snapshot Creation took %', clock_timestamp() - tStartTime;
END $$;

\C 'Materialized View mv1'
SELECT
        code, description, created, updated, m_row$
FROM
        mv1;

\C 'Materialized View mv2'
SELECT
        code, description, created, updated, m_row$
FROM
        mv2;

\C 'Materialized View mv3'
SELECT
        code, description, created, updated, m_row$
FROM
        mv3;

\C 'Materialized View mv4'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\prompt "INSERT INTO t2 WHERE code = 100 and refresh mv2 only" mike

INSERT  INTO
t2(     code,   parent, description,          created )
VALUES( 100,    2,     'Desctiption 100',     current_timestamp );

SELECT  *
FROM
        log$_t2;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv2',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took %', clock_timestamp() - tStartTime;
END $$;

\C 'Materialized View mv2'
SELECT
        code, description, created, updated, m_row$
FROM
        mv2;

\C 'Materialized View mv4'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Materialized View Log log$_t2'
SELECT  *
FROM
        log$_t2;

\prompt "Now Refresh mv4" mike

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took %', clock_timestamp() - tStartTime;
END $$;

\C 'Materialized View mv2'
SELECT
        code, description, created, updated, m_row$
FROM
        mv2;

\C 'Materialized View mv4'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
ORDER
BY      t1_code, t2_code, t3_code;

\C 'Materialized View Log log$_t2'
SELECT  *
FROM
        log$_t2;