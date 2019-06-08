/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: BulkHarness.sql
Author:       Mike Revitt
Date:         09/04/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
09/04/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This script creates 1,000,000 of data to stress test the materialized view process talong with the necessary
                materialized views to run the test scenarios

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

-- psql -h localhost -p 5432 -d postgres -U mike_data -q -f bulkHarness.sql -v v1=100 -v v2=25100 -v v3=6275100

SET CLIENT_MIN_MESSAGES = NOTICE;
\set VERBOSITY terse

\prompt "Refresh mv1 Full" mike

\C 'Materialized View log changs in log$_t1'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t1;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv1',
        pOwner          => 'mike_view',
        pFastRefresh    =>  FALSE
    );
    RAISE NOTICE 'Full Snapshot Refresh took %', clock_timestamp() - tStartTime;
END $$;

\echo
\prompt "Refresh mv2 Full" mike

\C 'Materialized View changs in log log$_t2'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t2;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv2',
        pOwner          => 'mike_view',
        pFastRefresh    =>  FALSE
    );
    RAISE NOTICE 'Full Snapshot Refresh took %', clock_timestamp() - tStartTime;
END $$;

\echo
\prompt "Refresh mv3 Full" mike

\C 'Materialized View changs in log log$_t3'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t3;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv3',
        pOwner          => 'mike_view',
        pFastRefresh    =>  FALSE
    );
    RAISE NOTICE 'Full Snapshot Refresh took %', clock_timestamp() - tStartTime;
END $$;

\echo
\prompt "Check log contents, then refresh mv4 Full" mike

\C 'Materialized View log log$_t1'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t1;

\C 'Materialized View log log$_t2'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t2;

\C 'Materialized View log log$_t3'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t3;

DO $$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv$refreshMaterializedView
    (
        pViewName       => 'mv4',
        pOwner          => 'mike_view',
        pFastRefresh    =>  FALSE
    );
    RAISE NOTICE 'Full Snapshot Refresh took %', clock_timestamp() - tStartTime;
END $$;

\C 'Materialized View log log$_t1'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t1;

\C 'Materialized View log log$_t2'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t2;

\C 'Materialized View log log$_t3'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total" FROM log$_t3;

\echo
\prompt "Add some indexes to the complex materialized view" mike
\echo

\c postgres mike_pgmview

CREATE
INDEX   t1_code_ind
ON      pgmv$_mv4( t1_code );

CREATE
INDEX   t2_code_ind
ON      pgmv$_mv4( t2_code );

CREATE
INDEX   t3_code_ind
ON      pgmv$_mv4( t3_code );

\prompt "UPDATE t1 WHERE code = :v1" mike
\echo
\c postgres mike_data

\C 'Base Table Row sample based on t1 & t3 table row'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   =  t2.code
WHERE
        t2.parent   =  t1.code
AND     t1.code     = :v1
AND     t3.code     = :v3
ORDER
BY      t1.code, t2.code, t3.code;

\C 'Total rows to be refreshed in view mv4'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total"
FROM
        mv4
WHERE   t1_code     = :v1;

UPDATE  t1
SET     description = 'First Description'
WHERE   code        = :v1;

\C 'Materialized View changs in log log$_t1'
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

\C 'Materialized View Row sample based on t1 & t3 table row'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
WHERE
        t1_code = :v1
AND     t3_code = :v3;

\echo
\prompt "UPDATE t2 WHERE code = :v2" mike
\echo

\C 'Total rows to be refreshed in view mv4'
SELECT TO_CHAR( COUNT(*), '9,999,990' ) "Row Total"
FROM
        mv4
WHERE   t2_code     = :v2;

UPDATE  t2
SET     description = 'Second Description'
WHERE   code        =  :v2;

\C 'Materialized View changs in log log$_t2'
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

\C 'Materialized View Row sample based on t2 & t3 table row'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
WHERE
        t2_code = :v2
AND     t3_code = :v3;

\C 'Base Tables Row sample based on t2 & t3 table row'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   =  t1.code
AND     t2.code     = :v2
AND     t3.code     = :v3
ORDER
BY      t1.code, t2.code, t3.code;

\echo
\prompt "UPDATE t3 WHERE code = :v3" mike
\echo

UPDATE  t3
SET     description = 'Third Description'
WHERE   code        =  :v3;

\C 'Materialized View changs in log log$_t3'
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

\C 'Materialized View Row sample based on t3 table row'
SELECT
        t1_code, t1_description, t2_code, t2_description, t3_code, t3_description
FROM
        mv4
WHERE
        t3_code = :v3;

\C 'Base Tables Row sample based on t3 table row'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        t1, t2
LEFT    OUTER JOIN t3
ON      t3.parent   = t2.code
WHERE
        t2.parent   =  t1.code
AND     t3.code     = :v3
ORDER
BY      t1.code, t2.code, t3.code;

\C 'Materialized Views mv1, mv2 & mv3 Row sample based on t3 table row'
SELECT
        t1.code, t1.description, t2.code, t2.description, t3.code, t3.description
FROM
        mv1 t1, mv2 t2
LEFT    OUTER JOIN mv3 t3
ON      t3.parent   =  t2.code
WHERE
        t2.parent   =  t1.code
AND     t3.code     = :v3
ORDER
BY      t1.code, t2.code, t3.code;

