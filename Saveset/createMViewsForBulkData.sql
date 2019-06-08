/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: CreateMViewsForBulkData.sql
Author:       Mike Revitt
Date:         06/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
06/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    Sample script to create the following database objects

                o   Table to run a test case against
                o   Create a Materialized View Log on the base table
                o   Create a Materialized View on the table previously created
                o   Select the data from the data dictionary tables

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
***********************************************************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U mike_data -q -f CreateMaterialisedViewsForBulkData.sql

SET CLIENT_MIN_MESSAGES = NOTICE;
\set VERBOSITY terse

\echo "Create Materialized views for Bulk Data Test"
\echo

DO $$
DECLARE
    cResult         CHAR(1)     := NULL;
    tStartTime      TIMESTAMP   := clock_timestamp();
    pSqlStatement   TEXT;
BEGIN
    cResult := mv$createMaterializedViewlog( 't1' );
    cResult := mv$createMaterializedViewlog( 't2' );
    cResult := mv$createMaterializedViewlog( 't3' );

    pSqlStatement := '
        SELECT
                t1.code t1_code, t1.description t1_description, t2.code t2_code, t2.description t2_description,
                t3.code t3_code, t3.description t3_description
        FROM
                t1, t2
        LEFT    JOIN t3 ON  t3.parent   = t2.code
        WHERE
                t2.parent   = t1.code';

    cResult := mv$createMaterializedView
    (
        pViewName           => 'mv4',
        pSelectStatement    =>  pSqlStatement,
        pOwner              => 'mike_view',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END $$;

\echo "Create materialized views 1 on each base table"
\echo

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



