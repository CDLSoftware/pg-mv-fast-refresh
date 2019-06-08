/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: removeMaterializedViews.sql
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
04/06/2019  | M Revitt      | Initial version
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

-- psql -h localhost -p 5432 -d postgres -U mike_data -q -f removeMaterializedViews.sql

SET CLIENT_MIN_MESSAGES = NOTICE;

\prompt "Get list of all Dictionary Objects" mike
\echo

\d

\prompt "Get list of all Matarialsed Views from Data Dictionary Table" mike

SELECT
        owner, view_name, pgmv$_name, table_names, bit_array
FROM
        mike_pgmview.mike$_pgmviews;

\prompt "Remove materialized view mv4" mike
\echo

DO $$
DECLARE
    cResult         CHAR(1)     := NULL;
BEGIN
    cResult := mv$removeMaterializedView( 'mv4', 'mike_view' );
END $$;

\prompt "Get list of all Matarialsed Views from Data Dictionary Table" mike
\echo

SELECT
        owner, view_name, pgmv$_name, table_names, bit_array
FROM
        mike_pgmview.mike$_pgmviews;

\prompt "Get list of all Dictionary Objects" mike

\d

\prompt "Remove materialized view log on t2" mike
\echo

DO $$
DECLARE
    cResult         CHAR(1)     := NULL;
BEGIN
    cResult := mv$removeMaterializedViewLog( 't2', 'mike_data' );
END $$;

\echo
\prompt "Get list of all Matarialsed View Logs from Data Dictionary Table to see why this failed" mike

SELECT
        owner, pglog$_name, table_name, pg_mview_bitmap
FROM
        mike_pgmview.mike$_pgmview_logs;

\prompt "Get list of all Matarialsed Views from Data Dictionary Table" mike

SELECT
        owner, view_name, pgmv$_name, table_names, bit_array
FROM
        mike_pgmview.mike$_pgmviews;

\prompt "Describe t2 prior to removing the log" mike
\echo
\d t2

\prompt "Remove materialized view mv2, then remove the materialized view log on t2" mike
\echo

DO $$
DECLARE
    cResult         CHAR(1)     := NULL;
BEGIN
    cResult := mv$removeMaterializedView( 'mv2', 'mike_view' );
    cResult := mv$removeMaterializedViewLog( 't2', 'mike_data' );
END $$;

\prompt "Describe t2 again, note that the row$ column has been removed" mike
\echo
\d t2

\prompt "Get list of all Dictionary Objects" mike
\echo
\d

\prompt "Get list of all Matarialsed Views from Data Dictionary Table" mike

SELECT
        owner, view_name, pgmv$_name, table_names, bit_array
FROM
        mike_pgmview.mike$_pgmviews;

\prompt "Get list of all Matarialsed View Logs from Data Dictionary Table to see why this failed" mike

SELECT
        owner, pglog$_name, table_name, pg_mview_bitmap
FROM
        mike_pgmview.mike$_pgmview_logs;
