/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvComplexFunctions.sql
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

DROP FUNCTION IF EXISTS mv$clearAllPgMvLogTableBits;
DROP FUNCTION IF EXISTS mv$clearAllPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$clearPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$createPgMview;
DROP FUNCTION IF EXISTS mv$insertMaterializedViewRows;
DROP FUNCTION IF EXISTS mv$insertMike$PgMview;
DROP FUNCTION IF EXISTS mv$insertOuterJoinRows;
DROP FUNCTION IF EXISTS mv$executeMVFastRefresh;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewFast;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewFull;
DROP FUNCTION IF EXISTS mv$setPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$updateMaterializedViewRows;

SET CLIENT_MIN_MESSAGES = NOTICE;

--------------------------------------------- Write CREATE-FUNCTION-stage scripts --------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$clearAllPgMvLogTableBits
            (
                pConst      IN      mike_pgmview.mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearAllPgMvLogTableBits
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
Description:    Performs a full refresh of the materialized view, which consists of truncating the table and then re-populating it.

                This activity also requires that every row in the materialized view log is updated to remove the interest from this
                materialized view, then as with the fast refresh once all the rows have been processed the materialized view log is
                cleaned up, in that all rows with a bitmap of zero are deleted as they are then no longer required.

Note:           This function requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres functions is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult         CHAR(1);
    aViewLog        mike_pgmview.mike$_pgmview_logs;
    aPgMview        mike_pgmview.mike$_pgmviews;

BEGIN
    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.table_array, 1 ) .. ARRAY_UPPER( aPgMview.table_array, 1 )
    LOOP
        aViewLog := mv$getPgMviewLogTableData( pConst, aPgMview.table_array[i] );

        cResult :=  mv$clearPgMvLogTableBits
                    (
                        pConst,
                        aViewLog.owner,
                        aViewLog.pglog$_name,
                        aPgMview.bit_array[i],
                        pConst.MAX_INTEGER_SIZE
                    );

        cResult := mv$clearSpentPgMviewLogs( pConst, aViewLog.owner, aViewLog.pglog$_name );

    END LOOP;
    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$clearAllPgMvLogTableBits';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$clearPgMviewLogBit
            (
                pConst      IN      mike_pgmview.mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearPgMviewLogBit
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Determins which which bit has been assigned to the base table and then adds that to the PgMview bitmap in the
                materialized view log data dictionary table to record all of the materialized views that are using the rows created
                in this table.

Notes:          This is how we determine which materialized views require an update when the fast refresh function is called

Arguments:      IN      pTableName          The name of the materialized view source table
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult     CHAR(1);
    iBitValue   INTEGER     := NULL;
    aViewLog    mike_pgmview.mike$_pgmview_logs;
    aPgMview    mike_pgmview.mike$_pgmviews;

BEGIN
    aPgMview    := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.log_array, 1 ) .. ARRAY_UPPER( aPgMview.log_array, 1 )
    LOOP
        aViewLog := mv$getPgMviewLogTableData( pConst, aPgMview.table_array[i] );

        iBitValue := mv$getBitValue( pConst, aPgMview.bit_array[i] );

        UPDATE  mike_pgmview.mike$_pgmview_logs
        SET     pg_mview_bitmap = pg_mview_bitmap - iBitValue
        WHERE   owner           = aViewLog.owner
        AND     table_name      = aViewLog.table_name;

    END LOOP;
    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$clearAllPgMviewLogBit';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$createPgMview
            (
                pConst      IN      mike_pgmview.mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
                pPgMvName   IN      TEXT
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createPgMview
Author:       Mike Revitt
Date:         16/01/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
16/01/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Creates the materialised view as a select * from the PgMv$ base table before the rowid columns are added

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pPgMvName           The name of the materialized view base table (pgmv$ table)
Returns:                TEXT                The list of columns in the newly created view
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult         CHAR(1);
    tSqlStatement   TEXT    := NULL;
    tViewColumns    TEXT    := NULL;

BEGIN

    tSqlStatement   :=  pConst.CREATE_VIEW     || pOwner  || pConst.DOT_CHARACTER  || pViewName   ||
                        pConst.CREATE_VIEW_AS  || pOwner  || pConst.DOT_CHARACTER  || pPgMvName;

    EXECUTE tSqlStatement;

    cResult         := mv$grantSelectPrivileges( pConst, pOwner, pViewName );
    tViewColumns    := mv$getPgMviewViewColumns( pConst, pOwner, pViewName );

    RETURN tViewColumns;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$createPgMview';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$insertMaterializedViewRows
            (
                pConst          IN      mike_pgmview.mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT    DEFAULT NULL,
                pRowIDs         IN      UUID[]  DEFAULT NULL
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertMaterializedViewRows
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
Description:    Gets called to insert a new row into the Materialized View when an insert is detected

Note:           This function requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres functions is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pTableAlias         The alias for the base table in the original select statement
                IN      pRowID              The unique identifier to locate the new row
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT;
    aPgMview        mike_pgmview.mike$_pgmviews;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.pgmv$_name  ||
                     pConst.OPEN_BRACKET   || aPgMview.pgmv_columns             || pConst.CLOSE_BRACKET     ||
                     pConst.SELECT_COMMAND || aPgMview.select_columns           ||
                     pConst.FROM_COMMAND   || aPgMview.table_names;

    IF aPgMview.where_clause != pConst.EMPTY_STRING
    THEN
        tSqlStatement := tSqlStatement || pConst.WHERE_COMMAND || aPgMview.where_clause ;
    END IF;

    IF pRowIDs IS NOT NULL
    THEN
        IF aPgMview.where_clause != pConst.EMPTY_STRING
        THEN
            tSqlStatement := tSqlStatement  || pConst.AND_COMMAND;
        ELSE
            tSqlStatement := tSqlStatement  || pConst.WHERE_COMMAND;
        END IF;

        tSqlStatement :=  tSqlStatement || pTableAlias || pConst.MV_M_ROW$_SOURCE_COLUMN || pConst.IN_ROWID_LIST;
    END IF;

    EXECUTE tSqlStatement
    USING   pRowIDs;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$insertMaterializedViewRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$insertMike$PgMview
            (
                pConst              IN      mike_pgmview.mv$allConstants,
                pOwner              IN      TEXT,
                pViewName           IN      TEXT,
                pPgMvName           IN      TEXT,
                pPgMvColumns        IN      TEXT,
                pSelectColumns      IN      TEXT,
                pTableNames         IN      TEXT,
                pWhereClause        IN      TEXT,
                pTableArray         IN      TEXT[],
                pAliasArray         IN      TEXT[],
                pRowidArray         IN      TEXT[],
                pOuterTableArray    IN      TEXT[],
                pParentTableArray   IN      TEXT[],
                pParentAliasArray   IN      TEXT[],
                pParentRowidArray   IN      TEXT[],
                pFastRefresh        IN      BOOLEAN
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertMike$PgMview
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
Description:    Every time a new materialized view is created, a record of that view is also created in the data dictionary table
                mike$_pgmviews.

                This table holds all of the pertinent information about the materialized view which is later used in the management
                of that view.

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pPgMvName           The name of the "hidden" base table containing the data
                IN      pPgMvColumns        The comma delimited list of columns in the base pgmv$ table
                IN      pSelectColumns      The comma delimited list of columns from the select statement
                IN      pTableNames         The comma delimited list of tables from the select statement
                IN      pWhereClause        The where clause from the select statement, this may be an empty string
                IN      pOuterTableArray    An array that holds the list of outer joined tables in a multi table materialized view
                IN      pTableArray         An array that holds the list of tables that make up the pgmv$ table
                IN      pAliasArray         An array that holds the list of table alias that make up the pgmv$ table
                IN      pRowidArray         An array that holds the list of rowid columns in the pgmv$ table
                IN      pFastRefresh        TRUE or FALSE, does this materialized view support fast refreshes
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMviewLogData mike_pgmview.mike$_pgmview_logs;

    iBit            SMALLINT    := NULL;
    tLogArray       TEXT[];
    iBitArray       INTEGER[];

BEGIN
    IF TRUE = pFastRefresh
    THEN
        FOR i IN array_lower( pTableArray, 1 ) .. array_upper( pTableArray, 1 )
        LOOP
            aPgMviewLogData     :=  mv$getPgMviewLogTableData( pConst, pTableArray[i] );
            iBit                :=  mv$setPgMviewLogBit
                                    (
                                        pConst,
                                        aPgMviewLogData.owner,
                                        aPgMviewLogData.pglog$_name,
                                        aPgMviewLogData.pg_mview_bitmap
                                    );
            tLogArray[i]        :=  aPgMviewLogData.pglog$_name;
            iBitArray[i]        :=  iBit;
        END LOOP;
    END IF;

    INSERT
    INTO    mike_pgmview.mike$_pgmviews
    (
            owner,
            view_name,
            pgmv$_name,
            pgmv_columns,
            select_columns,
            table_names,
            where_clause,
            table_array,
            alias_array,
            rowid_array,
            log_array,
            bit_array,
            outer_table_array,
            parent_table_array,
            parent_alias_array,
            parent_rowid_array
    )
    VALUES
    (
            pOwner,
            pViewName,
            pPgMvName,
            pPgMvColumns,
            pSelectColumns,
            pTableNames,
            pWhereClause,
            pTableArray,
            pAliasArray,
            pRowidArray,
            tLogArray,
            iBitArray,
            pOuterTableArray,
            pParentTableArray,
            pParentAliasArray,
            pParentRowidArray
    );
    RETURN;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$executeMVFastRefresh
            (
                pConst          IN      mike_pgmview.mv$allConstants,
                pDmlType        IN      TEXT,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pPgMvName       IN      TEXT,
                pRowidColumn    IN      TEXT,
                pTableAlias     IN      TEXT,
                pOuterTable     IN      BOOLEAN,
                pParentAlias    IN      TEXT,
                pParentRowid    IN      TEXT,
                pRowIDArray     IN      UUID[]
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$executeMVFastRefresh
Author:       Mike Revitt
Date:         08/05/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Selects all of the data from the materialized view log, in the order it was created, and applies the changes to
                the materialized view table and once the change has been applied the bit value for the materialized view is
                removed from the PgMview log row.

                Once all rows have been processed the materialized view log is cleaned up, in that all rows with a bitmap of zero
                are deleted as they are then no longer required

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult         CHAR(1)     := NULL;

BEGIN

    CASE pDmlType
    WHEN pConst.DELETE_DML_TYPE
    THEN
        cResult := mv$deleteMaterializedViewRows( pConst, pOwner, pPgMvName, pRowidColumn, pRowIDArray );

    WHEN pConst.INSERT_DML_TYPE
    THEN
        IF TRUE = pOuterTable
        THEN
            cResult :=  mv$insertOuterJoinRows
                        (
                            pConst,
                            pOwner,
                            pViewName,
                            pTableAlias,
                            pParentAlias,
                            pParentRowid,
                            pRowIDArray
                        );
        ELSE
            cResult := mv$insertMaterializedViewRows( pConst, pOwner, pViewName, pTableAlias, pRowIDArray );
        END IF;

    WHEN pConst.UPDATE_DML_TYPE
    THEN
        cResult := mv$deleteMaterializedViewRows( pConst, pOwner, pPgMvName, pRowidColumn, pRowIDArray );
        cResult := mv$updateMaterializedViewRows( pConst, pOwner, pViewName, pTableAlias,  pRowIDArray );
    ELSE
        RAISE EXCEPTION 'DML Type % is unknown', pDmlType;
    END CASE;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$executeMVFastRefresh';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$refreshMaterializedViewFast
            (
                pConst          IN      mike_pgmview.mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pPgMvName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pTableName      IN      TEXT,
                pRowidColumn    IN      TEXT,
                pPgMviewBit     IN      SMALLINT,
                pOuterTable     IN      BOOLEAN,
                pParentAlias    IN      TEXT,
                pParentRowid    IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFast
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
Description:    Selects all of the data from the materialized view log, in the order it was created, and applies the changes to
                the materialized view table and once the change has been applied the bit value for the materialized view is
                removed from the PgMview log row.

                Once all rows have been processed the materialized view log is cleaned up, in that all rows with a bitmap of zero
                are deleted as they are then no longer required

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    tDmlType        TEXT        := NULL;
    tLastType       TEXT        := NULL;
    tSqlStatement   TEXT        := NULL;
    cResult         CHAR(1)     := NULL;
    iArraySeq       INTEGER     := 0;
    biSequence      INTEGER     := 0;
    biMaxSequence   INTEGER     := 0;
    uRowID          UUID;
    uRowIDArray     UUID[];

    aViewLog        mike_pgmview.mike$_pgmview_logs;

BEGIN

    aViewLog := mv$getPgMviewLogTableData( pConst, pTableName );

    tSqlStatement    := pConst.MV_LOG$_SELECT_M_ROW$  || aViewLog.owner || pConst.DOT_CHARACTER || aViewLog.pglog$_name ||
                        pConst.MV_LOG$_WHERE_BITMAP$  ||
                        pConst.MV_LOG$_SELECT_M_ROWS_ORDER_BY;

    FOR     uRowID, biSequence, tDmlType
    IN
    EXECUTE tSqlStatement
    USING   pPgMviewBit, pPgMviewBit
    LOOP
        biMaxSequence := biSequence;

        IF tLastType =  tDmlType
        OR tLastType IS NULL
        THEN
            tLastType               := tDmlType;
            iArraySeq               := iArraySeq + 1;
            uRowIDArray[iArraySeq]  := uRowID;
        ELSE
            cResult :=  mv$executeMVFastRefresh
                        (
                            pConst,
                            tLastType,
                            pOwner,
                            pViewName,
                            pPgMvName,
                            pRowidColumn,
                            pTableAlias,
                            pOuterTable,
                            pParentAlias,
                            pParentRowid,
                            uRowIDArray
                        );

            tLastType               := tDmlType;
            iArraySeq               := 1;
            uRowIDArray[iArraySeq]  := uRowID;
        END IF;
    END LOOP;

    IF biMaxSequence > 0
    THEN
        cResult :=  mv$executeMVFastRefresh
                    (
                        pConst,
                        tLastType,
                        pOwner,
                        pViewName,
                        pPgMvName,
                        pRowidColumn,
                        pTableAlias,
                        pOuterTable,
                        pParentAlias,
                        pParentRowid,
                        uRowIDArray
                    );

        cResult :=  mv$clearPgMvLogTableBits
                    (
                        pConst,
                        aViewLog.owner,
                        aViewLog.pglog$_name,
                        pPgMviewBit,
                        biMaxSequence
                    );

        cResult := mv$clearSpentPgMviewLogs( pConst, aViewLog.owner, aViewLog.pglog$_name );
    END IF;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$refreshMaterializedViewFast';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$refreshMaterializedViewFull
            (
                pConst      IN      mike_pgmview.mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFull
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
Description:    Performs a full refresh of the materialized view, which consists of truncating the table and then re-populating it.

                This activity also requires that every row in the materialized view log is updated to remove the interest from this
                materialized view, then as with the fast refresh once all the rows have been processed the materialized view log is
                cleaned up, in that all rows with a bitmap of zero are deleted as they are then no longer required.

Note:           This function requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres functions is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult     CHAR(1);
    aPgMview    mike_pgmview.mike$_pgmviews;

BEGIN

    aPgMview    := mv$getPgMviewTableData(        pConst, pOwner, pViewName );
    cResult     := mv$truncateMaterializedView(   pConst, pOwner, aPgMview.pgmv$_name );
    cResult     := mv$insertMaterializedViewRows( pConst, pOwner, pViewName );
    cResult     := mv$clearAllPgMvLogTableBits(     pConst, pOwner, pViewName );

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$refreshMaterializedViewFull';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$refreshMaterializedViewFast
            (
                pConst      IN      mike_pgmview.mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFast
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
Description:    Determins what type of refresh is required and then calls the appropriate refresh function

Notes:          This function must come after the creation of the 2 functions
                it calls
                o   mv$refreshMaterializedViewFast( pOwner, pViewName );
                o   mv$refreshMaterializedViewFull( pOwner, pViewName );

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult         CHAR(1);
    aPgMview        mike_pgmview.mike$_pgmviews;
    bOuterJoined    BOOLEAN;

BEGIN
    aPgMview   := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.table_array, 1 ) .. ARRAY_UPPER( aPgMview.table_array, 1 )
    LOOP
        bOuterJoined := mv$checkIfOuterJoinedTable( pConst, aPgMview.table_array[i], aPgMview.outer_table_array );
        cResult :=  mv$refreshMaterializedViewFast
                    (
                        pConst,
                        pOwner,
                        pViewName,
                        aPgMview.pgmv$_name,
                        aPgMview.alias_array[i],
                        aPgMview.table_array[i],
                        aPgMview.rowid_array[i],
                        aPgMview.bit_array[i],
                        bOuterJoined,
                        aPgMview.parent_alias_array[i],
                        aPgMview.parent_rowid_array[i]
                    );
    END LOOP;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$insertOuterJoinRows
            (
                pConst          IN      mike_pgmview.mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pParentAlias    IN      TEXT,
                pParentRowid    IN      TEXT,
                pRowIDs         IN      UUID[]
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertOuterJoinRows
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    When inserting data into a complex materialized view, it is possible that a previous insert has already inserted
                the row that we are about to insert if that row is the subject of an outer join or is a parent of multiple new rows

                When applying updates to the materialized view it is possible that the row being updated has subsiquently been
                deleted, so before we can apply an update we have to ensure that the base row still exists.

                So to remove the possibility of duplicate rows we have to look to see if this situation has occured

Arguments:      IN      pMikePgMviews      The record of data for the materialized view
                IN      pSourceTableAlias   The alias for the source table in the view create command
                IN      pRowID              The rowid we are looking for
Returns:                NULL
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tFromClause     TEXT;
    tSqlStatement   TEXT;
    aPgMview        mike_pgmview.mike$_pgmviews;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    tFromClause  := pConst.FROM_COMMAND           || aPgMview.table_names   ||
                    pConst.WHERE_COMMAND          || aPgMview.where_clause  ||
                    pConst.AND_COMMAND            || pTableAlias            || pConst.MV_M_ROW$_SOURCE_COLUMN   ||
                    pConst.IN_ROWID_LIST;

    tSqlStatement :=    pConst.DELETE_FROM        ||
                        aPgMview.owner            || pConst.DOT_CHARACTER   || aPgMview.pgmv$_name              ||
                        pConst.WHERE_COMMAND      || pParentRowid           ||
                        pConst.IN_SELECT_COMMAND  || pParentAlias           || pConst.MV_M_ROW$_SOURCE_COLUMN   ||
                        tFromClause               || pConst.CLOSE_BRACKET;


    EXECUTE tSqlStatement
    USING   pRowIDs;

    tSqlStatement :=    pConst.INSERT_INTO      ||
                        aPgMview.owner          || pConst.DOT_CHARACTER     || aPgMview.pgmv$_name  ||
                        pConst.OPEN_BRACKET     || aPgMview.pgmv_columns    || pConst.CLOSE_BRACKET     ||
                        pConst.SELECT_COMMAND   || aPgMview.select_columns  ||
                        tFromClause;

    EXECUTE tSqlStatement
    USING   pRowIDs;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$insertOuterJoinRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$setPgMviewLogBit
            (
                pConst          IN      mike_pgmview.mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pPbMviewBitmap  IN      INTEGER
            )
    RETURNS INTEGER
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setPgMviewLogBit
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
Description:    Determins which which bit has been assigned to the base table and then adds that to the PgMview bitmap in the
                materialized view log data dictionary table to record all of the materialized views that are using the rows created
                in this table.

Notes:          This is how we determine which materialized views require an update when the fast refresh function is called

Arguments:      IN      pTableName          The name of the materialized view source table
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    iBit        SMALLINT    := NULL;
    iBitValue   INTEGER     := NULL;

BEGIN
    iBit                := mv$findFirstFreeBit( pConst, pPbMviewBitmap );
    iBitValue           := mv$getBitValue( pConst, iBit );

    UPDATE  mike_pgmview.mike$_pgmview_logs
    SET     pg_mview_bitmap = pg_mview_bitmap + iBitValue
    WHERE   owner           = pOwner
    AND     pglog$_name     = pPgLog$Name;

    RETURN( iBit );
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$updateMaterializedViewRows
            (
                pConst          IN      mike_pgmview.mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pRowIDs         IN      UUID[]
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$updateMaterializedViewRows
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
Description:    Gets called to insert a new row into the Materialized View when an insert is detected

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pTableAlias         The alias for the base table in the original select statement
                IN      pRowID              The unique identifier to locate the new row
Returns:                VOID

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult         CHAR(1)     := NULL;
    tSqlStatement   TEXT;
    aPgMview        mike_pgmview.mike$_pgmviews;
    bBaseRowExists  BOOLEAN := FALSE;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.pgmv$_name  ||
                     pConst.OPEN_BRACKET   || aPgMview.pgmv_columns             || pConst.CLOSE_BRACKET ||
                     pConst.SELECT_COMMAND || aPgMview.select_columns           ||
                     pConst.FROM_COMMAND   || aPgMview.table_names              ||
                     pConst.WHERE_COMMAND;

    IF aPgMview.where_clause != pConst.EMPTY_STRING
    THEN
        tSqlStatement := tSqlStatement || aPgMview.where_clause || pConst.AND_COMMAND;
    END IF;

    tSqlStatement :=  tSqlStatement || pTableAlias  || pConst.MV_M_ROW$_SOURCE_COLUMN || pConst.IN_ROWID_LIST;

    EXECUTE tSqlStatement
    USING   pRowIDs;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$updateMaterializedViewRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
