/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvBESFunctions.sql
Author:       Rohan Port
Date:         17/08/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This is the build script for the custom Beyond Essential Systems database functions that are required to support the 
                Materialized View fast refresh process.

                This script contains custom functions should be run after the base application functions have been created.

Notes:          The functions in this script should be maintained in alphabetic order.

                All functions must be created with SECURITY DEFINER to ensure they run with the privileges of the owner.

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents queries against the information_schema,
                this bug is fixed in versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Debug:          Add a variant of the following command anywhere you need some debug information
                RAISE NOTICE '<Funciton Name> % %',  CHR(10), <Variable to be examined>;

************************************************************************************************************************************
Copyright 2021 Beyond Essential Systems Pty Ltd

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
***********************************************************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U pgrs_mview -q -f mvBESFunctions.sql

----------------------- Write DROP-FUNCTION-stage scripts ----------------------
SET     CLIENT_MIN_MESSAGES = ERROR;

DROP FUNCTION IF EXISTS mv$addIndexToMv$Table;
DROP FUNCTION IF EXISTS mv$removeIndexFromMv$Table;
DROP FUNCTION IF EXISTS mv$renameMaterializedView;
DROP FUNCTION IF EXISTS mv$version;

----------------------- Write CREATE-FUNCTION-stage scripts --------------------
SET CLIENT_MIN_MESSAGES = NOTICE;

CREATE OR REPLACE
FUNCTION    mv$addIndexToMv$Table
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgMv$Name      IN      TEXT,
                pIndexName      IN      TEXT,
                pIndexCols      IN      TEXT
            )

    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$addIndexToMv$Table
Author:       Rohan Port
Date:         15/06/2021
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This function creates an index on the materialized view table

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pPgMv$Name          The name of the materialized view table
                IN      pIndexName          The name of the index to create
                IN      pIndexCols          The columns of the index to create
Returns:                VOID
************************************************************************************************************************************
Copyright 2021 Beyond Essential Systems Pty Ltd
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT;

BEGIN

    tSqlStatement   :=  pConst.CREATE_INDEX || pIndexName           ||
                        pConst.ON_COMMAND   || pOwner               || pConst.DOT_CHARACTER     || pPgMv$Name ||
                                               pConst.OPEN_BRACKET  || pIndexCols               || pConst.CLOSE_BRACKET;

    EXECUTE tSqlStatement;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$addIndexToMv$Table';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$removeIndexFromMv$Table
            (
                pConst          IN      mv$allConstants,
                pIndexName      IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$removeIndexFromMv$Table
Author:       Rohan Port
Date:         15/06/2021
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This function removes an index from the materialized view table

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pIndexName          The name of the index to remove
Returns:                VOID
************************************************************************************************************************************
Copyright 2021 Beyond Essential Systems Pty Ltd
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT;

BEGIN

    tSqlStatement   :=  pConst.DROP_INDEX   || pIndexName;

    EXECUTE tSqlStatement;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$removeIndexFromMv$Table';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$renameMaterializedView
            (
                pOldViewName           IN      TEXT,
                pNewViewName           IN      TEXT,
                pOwner                 IN      TEXT        DEFAULT USER
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$renameMaterializedView
Author:       Rohan Port
Date:         17/08/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Renames a materialized view, edits the view_name in the pg$mviews table

                This function performs the following steps
                1)  Edits the view_name in the pg$mviews table to be the new name
                2)  Alters the materialized view table name to be the new name

Arguments:      IN      pOldViewName        The existing name of the materialized view
                IN      pNewViewName        The new name of the materialized view
                IN      pOwner              Optional, the owner of the materialized view, defaults to user
Returns:                VOID
************************************************************************************************************************************
Copyright 2021 Beyond Essential Systems Pty Ltd
***********************************************************************************************************************************/
DECLARE

    aPgMview    pg$mviews;
    rConst      mv$allConstants;

    tUpdatePgMviewsSqlStatement   TEXT := '';
    tRenameTableSqlStatement      TEXT := '';
    tRenameIndexSqlStatement      TEXT := '';
   
    rIndex	         RECORD;
    tOldIndexName    TEXT;
    tNewIndexName    TEXT;
begin
	
	rConst      := mv$buildAllConstants();

    tUpdatePgMviewsSqlStatement   :=  rConst.UPDATE_COMMAND || 'pg$mviews' || rConst.SET_COMMAND || 'view_name = ' 
                                           || rConst.SINGLE_QUOTE_CHARACTER || pNewViewName || rConst.SINGLE_QUOTE_CHARACTER 
                                           || rConst.WHERE_COMMAND || 'view_name = ' || rConst.SINGLE_QUOTE_CHARACTER || pOldViewName || rConst.SINGLE_QUOTE_CHARACTER;
    
    tRenameTableSqlStatement   :=  rConst.ALTER_TABLE || pOldViewName || rConst.RENAME_TO_COMMAND || pNewViewName;

    EXECUTE tUpdatePgMviewsSqlStatement;
    EXECUTE tRenameTableSqlStatement;

    FOR rIndex IN 
        SELECT indexname FROM pg_indexes WHERE schemaname = pOwner AND tablename = pNewViewName AND indexname like '%' || rConst.MV_M_ROW$_COLUMN || '%'
    LOOP
        tOldIndexName := rIndex.indexname;
        tNewIndexName := REPLACE(tOldIndexName, pOldViewName, pNewViewName);
        tRenameIndexSqlStatement :=  rConst.ALTER_INDEX || tOldIndexName || rConst.RENAME_TO_COMMAND || tNewIndexName;
        execute tRenameIndexSqlStatement;
    END LOOP;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$renameMaterializedView';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      E'Error Context:% % \n % \n %',CHR(10),  tUpdatePgMviewsSqlStatement, tRenameTableSqlStatement, tRenameIndexSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$version()
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$help
Author:       Rohan Port
Date:         18/10/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Displays the version

Arguments:      IN      None
Returns:                TEXT    The version 

************************************************************************************************************************************
Copyright 2021 Beyond Essential Systems Pty Ltd
***********************************************************************************************************************************/
DECLARE

BEGIN

    RETURN '1_0_0';

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$help';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;