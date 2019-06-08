/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvApplicationFunctions.sql
Author:       Mike Revitt
Date:         28/04/2019
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

Description:    This is the build script for the complex database functions that are required to support the Materialized View
                fast refresh process.

                This script contains functions that rely on other database functions having been previously created and must
                therefore be run last in the build process.

Notes:          Some of the functions in this file rely on functions that are created within this file and so whilst the functions
                should be maintained in alphabetic order, this is not always possible.

                More importantly the order of functions in this file should not be altered

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Debug:          Add a variant of the following command anywhere you need some debug inforaiton
                RAISE NOTICE '<Funciton Name> % %',  CHR(10), <Variable to be examined>;

************************************************************************************************************************************
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
***********************************************************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U mike_pgmview -q -f mvComplexFunctions.sql

-- -------------------- Write DROP-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP FUNCTION IF EXISTS mv$createMaterializedView;
DROP FUNCTION IF EXISTS mv$createMaterializedViewlog;
DROP FUNCTION IF EXISTS mv$refreshMaterializedView;
DROP FUNCTION IF EXISTS mv$insertMaterializedViewLogRow;
DROP FUNCTION IF EXISTS mv$removeMaterializedView;
DROP FUNCTION IF EXISTS mv$removeMaterializedViewLog;

SET CLIENT_MIN_MESSAGES = NOTICE;

-- -------------------- Write CREATE-FUNCTION-stage scripts --------------------
CREATE OR REPLACE
FUNCTION    mv$createMaterializedView
            (
                pViewName           IN      TEXT,
                pSelectStatement    IN      TEXT,
                pOwner              IN      TEXT        DEFAULT USER,
                pViewColumns        IN      TEXT        DEFAULT NULL,
                pStorageClause      IN      TEXT        DEFAULT NULL,
                pFastRefresh        IN      BOOLEAN     DEFAULT FALSE
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createMaterializedView
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
Description:    Creates a materialized view which requires the rollowing steps to take place
            1)  A base table is created based on the select statement provided
            2)  A view is created based on the base table
            3)  The "ROWID" column is added to the base table
            4)  A record of the materialized view is entered into the data dictionary table

Notes:          If a materialized view with fast refresh is requested then a materialized view log table must have been pre-created

Arguments:      IN      pViewName           The name of the materialized view to be created
                IN      pSelectStatement    The SQL query that will be used to create the view
                IN      pOwner              Where the view is to be created, defaults to current schema
                IN      pViewColumns        Allow the view to be created with different names to the base table
                                            This list is positional so must match the position and number of columns in the
                                            select statment
                IN      pStorageClause      Optional, storage clause for the materialized view
                IN      pFastRefresh        If set to yes then materialized view fast refresh wil be supported
Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    cResult             CHAR(1)     := NULL;

    rConst              mike_pgmview.mv$allConstants;

    tSelectColumns      TEXT        := NULL;
    tSnapName           TEXT        := NULL;
    tTableNames         TEXT        := NULL;
    tViewColumns        TEXT        := NULL;
    tSnapColumns        TEXT        := NULL;
    tWhereClause        TEXT        := NULL;
    tRowidArray         TEXT[];
    tTableArray         TEXT[];
    tAliasArray         TEXT[];
    tOuterTableArray    TEXT[];
    tParentTableArray   TEXT[];
    tParentAliasArray   TEXT[];
    tParentRowidArray   TEXT[];

BEGIN

    rConst      := mv$buildAllConstants();
    tSnapName   := rConst.MV_PGMV_TABLE_PREFIX || SUBSTRING( pViewName, 1, rConst.MV_MAX_BASE_TABLE_LEN);

    SELECT
            pTableNames,
            pSelectColumns,
            pWhereClause
    FROM
            mv$deconstructSqlStatement( rConst, pSelectStatement )
    INTO
            tTableNames,
            tSelectColumns,
            tWhereClause;

    SELECT
            pTableArray,
            pAliasArray,
            pRowidArray,
            pOuterTableArray,
            pParentTableArray,
            pParentAliasArray,
            pParentRowidArray

    FROM
            mv$extractCompoundViewTables( rConst, tTableNames )
    INTO
            tTableArray,
            tAliasArray,
            tRowidArray,
            tOuterTableArray,
            tParentTableArray,
            tParentAliasArray,
            tParentRowidArray;

    cResult :=  mv$createPgMv$Table
                (
                    rConst,
                    pOwner,
                    tSnapName,
                    pViewColumns,
                    tSelectColumns,
                    tTableNames,
                    pStorageClause
                );

    tViewColumns    :=  mv$createPgMview
                        (
                            rConst,
                            pOwner,
                            pViewName,
                            tSnapName
                        );

     SELECT
            pPgMvColumns,
            pSelectColumns
    FROM
            mv$addRow$ToMv$Table
            (
                rConst,
                pOwner,
                tSnapName,
                tAliasArray,
                tRowidArray,
                tViewColumns,
                tSelectColumns
            )
    INTO
        tSnapColumns,
        tSelectColumns;

    cResult :=  mv$insertMike$PgMview
                (
                    rConst,
                    pOwner,
                    pViewName,
                    tSnapName,
                    tSnapColumns,
                    tSelectColumns,
                    tTableNames,
                    tWhereClause,
                    tTableArray,
                    tAliasArray,
                    tRowidArray,
                    tOuterTableArray,
                    tParentTableArray,
                    tParentAliasArray,
                    tParentRowidArray,
                    pFastRefresh
                );

    cResult := mv$refreshMaterializedView( pViewName, pOwner, FALSE );
    RETURN;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$createMaterializedViewlog
            (
                pTableName          IN      TEXT,
                pOwner              IN      TEXT     DEFAULT USER,
                pStorageClause      IN      TEXT     DEFAULT NULL
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createMaterializedViewlog
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
Description:    Creates the materialized view log against the base table which holds the record all changes appled to the table

Notes:          This is manddatory for a Fast Refresh Materialized View

Arguments:      IN      pTableName          The name of the base table upon which the materialized view is createded
                IN      pOwner         Where the table exists, defaults to current schema
                IN      pStorageClause      Optional, storage clause for the materialized view log
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rConst          mike_pgmview.mv$allConstants;

    tSqlStatement   TEXT    := NULL;
    tLog$Name       TEXT    := NULL;
    tTriggerName    TEXT    := NULL;
    cResult         CHAR(1) := NULL;

BEGIN

    rConst          := mv$buildAllConstants();
    tLog$Name       := rConst.MV_LOG_TABLE_PREFIX   || SUBSTRING( pTableName, 1, rConst.MV_MAX_BASE_TABLE_LEN );
    tTriggerName    := rConst.MV_TRIGGER_PREFIX     || SUBSTRING( pTableName, 1, rConst.MV_MAX_BASE_TABLE_LEN );

    cResult :=  mv$addRow$ToSourceTable(    rConst, pOwner, pTableName );
    cResult :=  mv$createMvLog$Table(       rConst, pOwner, tLog$Name,  pStorageClause );
    cResult :=  mv$addIndexToMvLog$Table(   rConst, pOwner, tLog$Name                  );
    cResult :=  mv$createMvLogTrigger(      rConst, pOwner, pTableName, tTriggerName   );
    cResult :=  mv$insertMikePgMviewLogs
                (
                    rConst,
                    pOwner,
                    tLog$Name,
                    pTableName,
                    tTriggerName
                );
    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$createMaterializedViewlog';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$insertMaterializedViewLogRow()
    RETURNS TRIGGER
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertMaterializedViewLogRow
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
Description:    This is the function that is called by the trigger on the base table.

Notes:          If the trigger is activated via a delete command then we have to get the original value of m_row$, otherwise
                we must use the new value

                If no materialized view has registered an interest in this table, no rows will be created

Arguments:      NONE
Returns:                TRIGGER     PostGre required return arry for all functions called from a trigger

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement       TEXT;
    uRow$               UUID;
    aMikePgMviewLogs    mike_pgmview.mike$_pgmview_logs;
    rConst              mike_pgmview.mv$allConstants;

BEGIN

    rConst              := mv$buildTriggerConstants();
    aMikePgMviewLogs    := mv$getPgMviewLogTableData( rConst, TG_TABLE_SCHEMA::TEXT, TG_TABLE_NAME::TEXT );

    IF aMikePgMviewLogs.pg_mview_bitmap > rConst.BITMAP_NOT_SET
    THEN
        IF TG_OP = rConst.DELETE_DML_TYPE
        THEN
            uRow$ := OLD.m_row$;
        ELSE
            uRow$ := NEW.m_row$;
        END IF;

        tSqlStatement := rConst.INSERT_INTO                 || aMikePgMviewLogs.pglog$_name     ||
                         rConst.MV_LOG$_INSERT_COLUMNS      ||
                         rConst.MV_LOG$_INSERT_VALUES_START || uRow$                            || rConst.QUOTE_COMMA_CHARACTERS ||
                         rConst.SINGLE_QUOTE_CHARACTER      || aMikePgMviewLogs.pg_mview_bitmap || rConst.QUOTE_COMMA_CHARACTERS ||
                         rConst.SINGLE_QUOTE_CHARACTER      || TG_OP                            || rConst.MV_LOG$_INSERT_VALUES_END;

        EXECUTE tSqlStatement;
    END IF;

    RETURN  NULL;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$insertMaterializedViewLogRow';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$refreshMaterializedView
            (
                pViewName           IN      TEXT,
                pOwner              IN      TEXT    DEFAULT USER,
                pFastRefresh        IN      BOOLEAN DEFAULT FALSE
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedView
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
Description:    Loops through each of the base tables that this materialised view is based on and refreshes them in turn

Notes:          This function must come after the creation of the 2 functions it calls
                o   mv$refreshMaterializedViewFast;
                o   mv$refreshMaterializedViewFull;

Arguments:      IN      pViewName           The name of the materialized view
                IN      pOwner              The owner of the object
                IN      pFastRefresh        Whether or not to perform a fast refresh, TRUE indicates a fast refresh is required
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult     CHAR(1) := NULL;

    rConst      mike_pgmview.mv$allConstants;

BEGIN

    rConst  := mv$buildAllConstants();

    IF TRUE = pFastRefresh
    THEN
        cResult :=  mv$refreshMaterializedViewFast( rConst, pOwner, pViewName );
    ELSE
        cResult :=  mv$refreshMaterializedViewFull( rConst, pOwner, pViewName );
    END IF;

    RETURN;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$removeMaterializedView
            (
                pViewName           IN      TEXT,
                pOwner              IN      TEXT        DEFAULT USER
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$removeMaterializedView
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
Description:    Removes a materialized view which requires the rollowing steps to take place
            1)  A base table is created based on the select statement provided
            2)  A view is created based on the base table
            3)  The "ROWID" column is added to the base table
            4)  A record of the materialized view is entered into the data dictionary table

Notes:          If a materialized view with fast refresh is requested then a materialized view log table must have been pre-created

Arguments:      IN      pViewName           The name of the materialized view to be created
                IN      pSelectStatement    The SQL query that will be used to create the view
                IN      pOwner              Where the view is to be created, defaults to current schema
                IN      pViewColumns        Allow the view to be created with different names to the base table
                                            This list is positional so must match the position and number of columns in the
                                            select statment
                IN      pStorageClause      Optional, storage clause for the materialized view
                IN      pFastRefresh        If set to yes then materialized view fast refresh wil be supported
Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMview    mike_pgmview.mike$_pgmviews;
    rConst      mike_pgmview.mv$allConstants;

    cResult     CHAR(1);

BEGIN

    rConst      := mv$buildAllConstants();
    aPgMview    := mv$getPgMviewTableData(      rConst, pOwner, pViewName           );
    cResult     := mv$clearAllPgMvLogTableBits( rConst, pOwner, pViewName           );
    cResult     := mv$clearPgMviewLogBit(       rConst, pOwner, pViewName           );
    cResult     := mv$dropView(                 rConst, pOwner, pViewName           );
    cResult     := mv$dropTable(                rConst, pOwner, aPgMview.pgmv$_name );
    cResult     := mv$deleteMike$PgMview(               pOwner, pViewName           );

    RETURN;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$removeMaterializedViewLog
            (
                pTableName          IN      TEXT,
                pOwner              IN      TEXT        DEFAULT USER
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$removeMaterializedViewLog
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
Description:    Removes a materialized view which requires the rollowing steps to take place
            1)  A base table is created based on the select statement provided
            2)  A view is created based on the base table
            3)  The "ROWID" column is added to the base table
            4)  A record of the materialized view is entered into the data dictionary table

Notes:          If a materialized view with fast refresh is requested then a materialized view log table must have been pre-created

Arguments:      IN      pViewName           The name of the materialized view to be created
                IN      pSelectStatement    The SQL query that will be used to create the view
                IN      pOwner              Where the view is to be created, defaults to current schema
                IN      pViewColumns        Allow the view to be created with different names to the base table
                                            This list is positional so must match the position and number of columns in the
                                            select statment
                IN      pStorageClause      Optional, storage clause for the materialized view
                IN      pFastRefresh        If set to yes then materialized view fast refresh wil be supported
Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rConst          mike_pgmview.mv$allConstants;
    aViewLog        mike_pgmview.mike$_pgmview_logs;

    tSqlStatement       TEXT;
    tLog$Name           TEXT        := NULL;
    tMvTriggerName      TEXT        := NULL;
    cResult             CHAR(1)     := NULL;

BEGIN

    rConst   := mv$buildAllConstants();
    aViewLog := mv$getPgMviewLogTableData( rConst, pTableName );

    IF aViewLog.pg_mview_bitmap = rConst.BITMAP_NOT_SET
    THEN
        cResult  := mv$dropTrigger(                 rConst, pOwner, aViewLog.trigger_name, pTableName   );
        cResult  := mv$dropTable(                   rConst, pOwner, aViewLog.pglog$_name                );
        cResult  := mv$removeRow$FromSourceTable(   rConst, pOwner, pTableName                          );
        cResult  := mv$deleteMike$PgMviewLog(               pOwner, pTableName                          );
    ELSE
        RAISE EXCEPTION 'The Naterialized View Log on Table % is still in use', pTableName;
    END IF;
    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$removeMaterializedViewLog';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

------------------------------------------------------------------------------------------------------------------------------------

GRANT   EXECUTE ON  FUNCTION    mv$createMaterializedViewlog    TO  pgmv$_execute;
GRANT   EXECUTE ON  FUNCTION    mv$createMaterializedView       TO  pgmv$_execute;
GRANT   EXECUTE ON  FUNCTION    mv$refreshMaterializedView      TO  pgmv$_execute;
GRANT   EXECUTE ON  FUNCTION    mv$removeMaterializedView       TO  pgmv$_execute;
GRANT   EXECUTE ON  FUNCTION    mv$removeMaterializedViewLog    TO  pgmv$_execute;
