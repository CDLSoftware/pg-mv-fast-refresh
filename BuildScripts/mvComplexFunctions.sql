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
05/11/2019  | M Revitt      | mv$clearPgMvLogTableBits is now a complex function so move it into the conplex script
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

-- psql -h localhost -p 5432 -d postgres -U pgrs_mview -q -f mvComplexFunctions.sql

-- -------------------- Write DROP-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP FUNCTION IF EXISTS mv$clearAllPgMvLogTableBits;
DROP FUNCTION IF EXISTS mv$clearPgMvLogTableBits;
DROP FUNCTION IF EXISTS mv$clearPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$createPgMv$Table;
DROP FUNCTION IF EXISTS mv$insertMaterializedViewRows;
DROP FUNCTION IF EXISTS mv$insertPgMview;
DROP FUNCTION IF EXISTS mv$insertOuterJoinRows;
DROP FUNCTION IF EXISTS mv$insertPgMviewOuterJoinDetails;
DROP FUNCTION IF EXISTS mv$checkParentToChildOuterJoinAlias;
DROP FUNCTION IF EXISTS mv$executeMVFastRefresh;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewFast;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewFull;
DROP FUNCTION IF EXISTS mv$setPgMviewLogBit;
DROP FUNCTION IF EXISTS mv$updateMaterializedViewRows;
DROP FUNCTION IF EXISTS mv$updateOuterJoinColumnsNull;
DROP FUNCTION IF EXISTS mv$regExpCount;
DROP FUNCTION IF EXISTS mv$regExpInstr;
DROP FUNCTION IF EXISTS mv$regExpReplace;
DROP FUNCTION IF EXISTS mv$regExpSubstr;
DROP FUNCTION IF EXISTS mv$outerJoinToInnerJoinReplacement;

SET CLIENT_MIN_MESSAGES = NOTICE;

--------------------------------------------- Write CREATE-FUNCTION-stage scripts --------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$clearAllPgMvLogTableBits
            (
                pConst      IN      mv$allConstants,
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
17/09/2019  | D Day         | Bug fix - Added logic to ignore table name if it already exists when clearing the bits in the mview logs
			|				| mview logs
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

    cResult         		CHAR(1);
    aViewLog        		pg$mview_logs;
    aPgMview        		pg$mviews;
	
	tTableName				TEXT;
	tDistinctTableArray		TEXT[];
	
	iTableAlreadyExistsCnt 	INTEGER DEFAULT 0;

BEGIN
    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.table_array, 1 ) .. ARRAY_UPPER( aPgMview.table_array, 1 )
    LOOP
        aViewLog := mv$getPgMviewLogTableData( pConst, aPgMview.table_array[i] );
		
		tTableName := aPgMview.table_array[i];		
		tDistinctTableArray[i] := tTableName;
		
		SELECT count(1) INTO STRICT iTableAlreadyExistsCnt 
		FROM (SELECT unnest(tDistinctTableArray) as table_name) inline
		WHERE inline.table_name = tTableName;
		
		IF iTableAlreadyExistsCnt = 1
		THEN

			cResult :=  mv$clearPgMvLogTableBits
						(
							pConst,
							aViewLog.owner,
							aViewLog.pglog$_name,
							aPgMview.bit_array[i],
							pConst.MAX_BITMAP_SIZE
						);

			cResult := mv$clearSpentPgMviewLogs( pConst, aViewLog.owner, aViewLog.pglog$_name );
			
		END IF;

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
FUNCTION    mv$clearPgMvLogTableBits
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pBit            IN      SMALLINT,
                pMaxSequence    IN      BIGINT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearPgMvLogTableBits
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
29/05/2020  | D Day			| Defect fix - Removed dblink action and call to function mv$clearPgMvLogTableBitsAction as this
			|				| was causing missing when an error occurred during the transaction process steps.
18/03/202   | D Day         | Defect fix - To reduce the impact of deadlocks caused by multiple mview refreshes trying to update them
			|				| the same mview log BITMAP$ column row. The UPDATE statement has been moved to a separate transaction 
			|				| using dblink extension. PG_BACKGROUND extension would have been the preferred option - this is not 
			|				| yet available in AWS RDS Postgres.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Bitmaps are how we manage multiple registrations against the same base table, every time the recorded row has been
                applied to the materialized view we remove the bit that signifies the interest from the materialized view log

Notes:          Array Processing command

                UPDATE  cdl_data.log$_t1
                SET     bitmap$[0] = bitmap$[0] - 1
                WHERE   sequence$
                IN(     SELECT  sequence$
                        FROM    cdl_data.log$_t1
                        WHERE   bitmap$[0] & 1   = 1
                        AND     sequence$       <= 9223372036854775807
                   );

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pPgLog$Name         The name of the materialized view log table
                IN      pBit                The bit to be cleared from the row
                IN      pMaxSequence        The maximum value bitmap being used
Returns:                VOID
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT;
    tBitValue       mv$bitValue;
    
BEGIN

    tBitValue       := mv$getBitValue( pConst, pBit );
    tSqlStatement   := pConst.UPDATE_COMMAND        || pOwner || pConst.DOT_CHARACTER   || pPgLog$Name  ||
                       pConst.SET_COMMAND           || pConst.BITMAP_COLUMN             || '[' || tBitValue.BIT_ROW || ']' ||
                       pConst.EQUALS_COMMAND        || pConst.BITMAP_COLUMN             || '[' || tBitValue.BIT_ROW || ']' ||
                                                       pConst.SUBTRACT_COMMAND          ||        tBitValue.BIT_MAP ||
                       pConst.WHERE_COMMAND         || pConst.MV_SEQUENCE$_COLUMN       ||
                       pConst.IN_SELECT_COMMAND     || pConst.MV_SEQUENCE$_COLUMN       ||
                       pConst.FROM_COMMAND          || pOwner || pConst.DOT_CHARACTER   || pPgLog$Name  ||
                       pConst.WHERE_COMMAND         || pConst.BITMAP_COLUMN             || '[' || tBitValue.BIT_ROW || ']' ||
                       pConst.BITAND_COMMAND        || tBitValue.BIT_MAP                ||
                                                       pConst.EQUALS_COMMAND            || tBitValue.BIT_MAP            ||
                       pConst.AND_COMMAND           || pConst.MV_SEQUENCE$_COLUMN       || pConst.LESS_THAN_EQUAL       ||
                       pMaxSequence                 || pConst.CLOSE_BRACKET;
		
	EXECUTE tSqlStatement;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$clearPgMvLogTableBits';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$clearPgMviewLogBit
            (
                pConst      IN      mv$allConstants,
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
30/11/2019  | M Revitt      | Use mv$bitValue to accomodate > 62 MV's per base Table
17/09/2019  | D Day         | Bug fix - Added logic to ignore table name if it already exists to stop pg$mview_logs table
			|				| pg_mview_bitmap column not being updated multiple times.
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

    cResult     			CHAR(1);
    tBitValue   			mv$bitValue;
    aViewLog    			pg$mview_logs;
    aPgMview    			pg$mviews;
	
	tTableName				TEXT;
	tDistinctTableArray		TEXT[];
	
	iTableAlreadyExistsCnt 	INTEGER DEFAULT 0;

BEGIN
    aPgMview    := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.log_array, 1 ) .. ARRAY_UPPER( aPgMview.log_array, 1 )
    LOOP
        aViewLog := mv$getPgMviewLogTableData( pConst, aPgMview.table_array[i] );
		
		tTableName := aPgMview.log_array[i];		
		tDistinctTableArray[i] := tTableName;
		
		SELECT  count(1)
        INTO    STRICT iTableAlreadyExistsCnt
		FROM (  SELECT unnest(tDistinctTableArray) as table_name ) inline
		WHERE   inline.table_name = tTableName;
		
		IF iTableAlreadyExistsCnt = 1
		THEN
		
			tBitValue := mv$getBitValue( pConst, aPgMview.bit_array[i] );

			UPDATE  pg$mview_logs
			SET     pg_mview_bitmap[tBitValue.BIT_ROW]  = pg_mview_bitmap[tBitValue.BIT_ROW] - tBitValue.BIT_MAP
			WHERE   owner                               = aViewLog.owner
			AND     table_name                          = aViewLog.table_name;
		
		END IF;
    END LOOP;
    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$clearPgMviewLogBit';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$createPgMv$Table
            (
                pConst              IN      mv$allConstants,
                pOwner              IN      TEXT,
                pViewName           IN      TEXT,
                pViewColumns        IN      TEXT,
                pSelectColumns      IN      TEXT,
                pTableNames         IN      TEXT,
                pStorageClause      IN      TEXT
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createPgMv$Table
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
Description:    Creates the base table upon which the Materialized View will be based from the provided SQL statment

Note:           This function requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres functions is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view base table
                IN      pViewColumns        Allow the view to be created with different names to the base table
                                            This list is positional so must match the position and number of columns in the
                                            select statment
                IN      pSelectColumns      The column list from the SQL query that will be used to create the view
                IN      pTableNames         The string between the FROM and WHERE clauses in the SQL query
                IN      pStorageClause      Optional, storage clause for the materialized view
Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    cResult         CHAR(1);
    tDefinedColumns TEXT    := NULL;
    tSqlStatement   TEXT    := NULL;
    tStorageClause  TEXT    := NULL;
    tViewColumns    TEXT    := NULL;

BEGIN

    IF pViewColumns IS NOT NULL
    THEN
        tDefinedColumns :=  pConst.OPEN_BRACKET ||
                                REPLACE( REPLACE( pViewColumns,
                                pConst.OPEN_BRACKET  , NULL ),
                                pConst.CLOSE_BRACKET , NULL ) ||
                            pConst.CLOSE_BRACKET;
    ELSE
        tDefinedColumns :=  pConst.SPACE_CHARACTER;
    END IF;

    IF pStorageClause IS NOT NULL
    THEN
        tStorageClause := pStorageClause;
    ELSE
        tStorageClause := pConst.SPACE_CHARACTER;
    END IF;

    tSqlStatement   :=  pConst.CREATE_TABLE     || pOwner          || pConst.DOT_CHARACTER || pViewName || tDefinedColumns  ||
                        pConst.AS_COMMAND       ||
                        pConst.SELECT_COMMAND   || pSelectColumns  ||
                        pConst.FROM_COMMAND     || pTableNames     ||
                        pConst.WHERE_NO_DATA    || tStorageClause;

    EXECUTE tSqlStatement;

    cResult         :=  mv$grantSelectPrivileges( pConst, pOwner, pViewName );
    tViewColumns    :=  mv$getPgMviewViewColumns( pConst, pOwner, pViewName );

    RETURN tViewColumns;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$createPgMv$Table';
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
                pConst          IN      mv$allConstants,
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
    aPgMview        pg$mviews;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
                     pConst.OPEN_BRACKET   || aPgMview.pgmv_columns             || pConst.CLOSE_BRACKET ||
                     pConst.SELECT_COMMAND || aPgMview.select_columns           ||
                     pConst.FROM_COMMAND   || aPgMview.table_names;

    IF aPgMview.where_clause != pConst.EMPTY_STRING
    THEN
        tSqlStatement := tSqlStatement || pConst.WHERE_COMMAND || aPgMview.where_clause ;
    END IF;

    IF pRowIDs IS NOT NULL -- Because this fires for a Full Refresh as well as a Fast Refresh
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
FUNCTION    mv$insertPgMview
            (
                pConst              IN      mv$allConstants,
                pOwner              IN      TEXT,
                pViewName           IN      TEXT,
                pViewColumns        IN      TEXT,
                pSelectColumns      IN      TEXT,
                pTableNames         IN      TEXT,
                pWhereClause        IN      TEXT,
                pTableArray         IN      TEXT[],
                pAliasArray         IN      TEXT[],
                pRowidArray         IN      TEXT[],
                pOuterTableArray    IN      TEXT[],
                pInnerAliasArray    IN      TEXT[],
                pInnerRowidArray    IN      TEXT[],
                pFastRefresh        IN      BOOLEAN
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertPgMview
Author:       Mike Revitt
Date:         12/011/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
17/09/2019  | D Day         | Bug fix - Added logic to ignore log table name if it already exists as this was causing the bit value being set incorrectly
			|				| in the data dictionary table bit_array column in pg$mviews table.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Every time a new materialized view is created, a record of that view is also created in the data dictionary table
                pg$mviews.

                This table holds all of the pertinent information about the materialized view which is later used in the management
                of that view.

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pViewColumns        The comma delimited list of columns in the base pgmv$ table
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

    aPgMviewLogData pg$mview_logs;
	
	aPgMviewLogOriginalData pg$mview_logs;

    iBit            		SMALLINT    := NULL;
    tLogArray       		TEXT[];
    iBitArray       		INTEGER[];
	
	tTableName				TEXT;
	
	tDistinctTableArray 	TEXT[];
	
	iOrigBitValue			SMALLINT;
	tOrigLogTableNameValue	TEXT;
	
	iTableAlreadyExistsCnt	INTEGER DEFAULT 0;
	iLoopCounter			INTEGER DEFAULT 0; 
	
	rOrigMviewLogInfo 		RECORD;

BEGIN

    IF TRUE = pFastRefresh
    THEN

        FOR i IN array_lower( pTableArray, 1 ) .. array_upper( pTableArray, 1 )
        LOOP
			
            aPgMviewLogData     :=  mv$getPgMviewLogTableData( pConst, pTableArray[i] );
						
			tTableName := pTableArray[i];		
			tDistinctTableArray[i] := tTableName;
			
			SELECT count(1) INTO STRICT iTableAlreadyExistsCnt 
			FROM (SELECT unnest(tDistinctTableArray) AS table_name) inline
			WHERE inline.table_name = tTableName;
			
			IF iTableAlreadyExistsCnt = 1 
			THEN

            	iBit                :=  mv$setPgMviewLogBit
            	                        (
           	                             pConst,
            	                            aPgMviewLogData.owner,
           	                             	aPgMviewLogData.pglog$_name,
            	                            aPgMviewLogData.pg_mview_bitmap
                                   	 	);
										
            	tLogArray[i]        :=  aPgMviewLogData.pglog$_name;
            	iBitArray[i]        :=  iBit;
			
			ELSE
							
				SELECT DISTINCT inline.bitvalue,
				inline.LogTableNameValue
				INTO iOrigBitValue, tOrigLogTableNameValue
				FROM (SELECT unnest(iBitArray) bitValue,
					  		 unnest(tLogArray) AS LogTableNameValue) inline
				WHERE inline.LogTableNameValue = aPgMviewLogData.pglog$_name;
				
            	tLogArray[i]        := tOrigLogTableNameValue;
				iBitArray[i]        := iOrigBitValue;
			
			END IF;
			
        END LOOP;
    END IF;

    INSERT
    INTO    pg$mviews
    (
            owner,
            view_name,
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
            inner_alias_array,
            inner_rowid_array
    )
    VALUES
    (
            pOwner,
            pViewName,
            pViewColumns,
            pSelectColumns,
            pTableNames,
            pWhereClause,
            pTableArray,
            pAliasArray,
            pRowidArray,
            tLogArray,
            iBitArray,
            pOuterTableArray,
            pInnerAliasArray,
            pInnerRowidArray
    );
    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$insertPgMview';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$executeMVFastRefresh
            (
                pConst          IN      mv$allConstants,
                pDmlType        IN      TEXT,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pRowidColumn    IN      TEXT,
                pTableAlias     IN      TEXT,
                pOuterTable     IN      BOOLEAN,
                pInnerAlias     IN      TEXT,
                pInnerRowid     IN      TEXT,
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
01/07/2019	| David Day		| Added function mv$updateOuterJoinColumnsNull to handle outer join deletes.            |               |
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

    cResult CHAR(1)     := NULL;

BEGIN

    CASE pDmlType
    WHEN pConst.DELETE_DML_TYPE
    THEN
	    IF TRUE = pOuterTable
        THEN	
		
			cResult :=  mv$updateOuterJoinColumnsNull
							(
								pConst,
								pOwner,
								pViewName,
								pTableAlias,
								pRowidColumn,
								pRowIDArray
							);
		
		ELSE
			cResult := mv$deleteMaterializedViewRows( pConst, pOwner, pViewName, pRowidColumn, pRowIDArray );
		END IF;
			
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
                            pInnerAlias,
                            pInnerRowid,
                            pRowIDArray
                        );
        ELSE
            cResult := mv$deleteMaterializedViewRows( pConst, pOwner, pViewName, pRowidColumn, pRowIDArray );
            cResult := mv$insertMaterializedViewRows( pConst, pOwner, pViewName, pTableAlias,  pRowIDArray );
        END IF;

    WHEN pConst.UPDATE_DML_TYPE
    THEN
        cResult := mv$deleteMaterializedViewRows( pConst, pOwner, pViewName, pRowidColumn, pRowIDArray );
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
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pTableName      IN      TEXT,
                pRowidColumn    IN      TEXT,
                pPgMviewBit     IN      SMALLINT,
                pOuterTable     IN      BOOLEAN,
                pInnerAlias     IN      TEXT,
                pInnerRowid     IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFast
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
Description:    Selects all of the data from the materialized view log, in the order it was created, and applies the changes to
                the materialized view table and once the change has been applied the bit value for the materialized view is
                removed from the PgMview log row.
				
				This is used as part of the initial materialized view creation were all details are loaded into table
				pg$mviews_oj_details which is later used by the refresh procesa.

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
    iArraySeq       INTEGER     := pConst.ARRAY_LOWER_VALUE;
    biSequence      BIGINT      := 0;
    biMaxSequence   BIGINT      := 0;
    uRowID          UUID;
    uRowIDArray     UUID[];

    aViewLog        pg$mview_logs;
    tBitValue       mv$bitValue;

BEGIN

    aViewLog    := mv$getPgMviewLogTableData( pConst, pTableName );
    tBitValue   := mv$getBitValue( pConst, pPgMviewBit );

    tSqlStatement    := pConst.MV_LOG$_SELECT_M_ROW$    || aViewLog.owner || pConst.DOT_CHARACTER   || aViewLog.pglog$_name     ||
                        pConst.WHERE_COMMAND            || pConst.BITMAP_COLUMN              || '[' || tBitValue.BIT_ROW || ']' ||
                        pConst.BITAND_COMMAND           || tBitValue.BIT_MAP                 ||
                        pConst.EQUALS_COMMAND           || tBitValue.BIT_MAP                 ||
                        pConst.MV_LOG$_SELECT_M_ROWS_ORDER_BY;
 
 -- SELECT m_row$,sequence$,dmltype$ FROM cdl_data.log$_t1 WHERE bitmap$ &  POWER( 2, $1)::BIGINT =  POWER( 2, $2)::BIGINT ORDER BY sequence$
 
    FOR     uRowID, biSequence, tDmlType
    IN
    EXECUTE tSqlStatement
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
                            pRowidColumn,
                            pTableAlias,
                            pOuterTable,
                            pInnerAlias,
                            pInnerRowid,
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
                        pRowidColumn,
                        pTableAlias,
                        pOuterTable,
                        pInnerAlias,
                        pInnerRowid,
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
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFull
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
    aPgMview    pg$mviews;

BEGIN

    aPgMview    := mv$getPgMviewTableData(        pConst, pOwner, pViewName );
    cResult     := mv$truncateMaterializedView(   pConst, pOwner, aPgMview.view_name );
    cResult     := mv$insertMaterializedViewRows( pConst, pOwner, pViewName );
    cResult     := mv$clearAllPgMvLogTableBits(   pConst, pOwner, pViewName );

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
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFast
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/03/2020  | D Day         | Defect fix to resolve outer join check function mv$checkIfOuterJoinedTable to handle if the table_array
			|				| value had both an inner join and outer join condition inside the main sql query. Amended to only pass in
			|				| in the outer_table_array loop value not the full array.
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
    aPgMview        pg$mviews;
    bOuterJoined    BOOLEAN;

BEGIN
    aPgMview   := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.table_array, 1 ) .. ARRAY_UPPER( aPgMview.table_array, 1 )
    LOOP
        bOuterJoined := mv$checkIfOuterJoinedTable( pConst, aPgMview.table_array[i], aPgMview.outer_table_array[i] );
        cResult :=  mv$refreshMaterializedViewFast
                    (
                        pConst,
                        pOwner,
                        pViewName,
                        aPgMview.alias_array[i],
                        aPgMview.table_array[i],
                        aPgMview.rowid_array[i],
                        aPgMview.bit_array[i],
                        bOuterJoined,
                        aPgMview.inner_alias_array[i],
                        aPgMview.inner_rowid_array[i]
                    );
    END LOOP;

    RETURN;
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$refreshMaterializedViewFast';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$insertOuterJoinRows
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pInnerAlias     IN      TEXT,
                pInnerRowid     IN      TEXT,
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
28/04/2020	| D Day			| Added join_replacement_from_sql value from pg$mview_oj_details data dictionary table to use in DELETE 
			|				| and INSERT statements to help performance.
14/02/2020	| D Day			| Added dot character inbetween pInnerAlias and pConst.MV_M_ROW$_SOURCE_COLUMN as the inner alias array
			|				| values no longer include the dot.
19/06/2019  | M Revitt      | Fixed issue with Delete statment that added superious WHERE Clause when there was not WHERE statment
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    When inserting data into a complex materialized view, it is possible that a previous insert has already inserted
                the row that we are about to insert if that row is the subject of an outer join or is a parent of multiple new rows

                When applying updates to the materialized view it is possible that the row being updated has subsiquently been
                deleted, so before we can apply an update we have to ensure that the base row still exists.

                So to remove the possibility of duplicate rows we have to look to see if this situation has occured

Arguments:      IN      pMikepg$mviews      The record of data for the materialized view
                IN      pSourceTableAlias   The alias for the source table in the view create command
                IN      pRowID              The rowid we are looking for
Returns:                NULL
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tFromClause     	TEXT;
    tSqlStatement   	TEXT;
    aPgMview        	pg$mviews;
	aPgMviewOjDetails	pg$mviews_oj_details;

BEGIN

    aPgMview    		 := mv$getPgMviewTableData( pConst, pOwner, pViewName );
    aPgMviewOjDetails    := mv$getPgMviewOjDetailsTableData( pConst, pOwner, pViewName, pTableAlias);
	
	tFromClause := pConst.FROM_COMMAND  || aPgMviewOjDetails.join_replacement_from_sql     || pConst.WHERE_COMMAND;

    IF LENGTH( aPgMview.where_clause ) > 0
    THEN
        tFromClause := tFromClause      || aPgMview.where_clause    || pConst.AND_COMMAND;
    END IF;

    tFromClause := tFromClause  || pTableAlias   || pConst.MV_M_ROW$_SOURCE_COLUMN   || pConst.IN_ROWID_LIST;

    tSqlStatement   :=  pConst.DELETE_FROM       		||
                        aPgMview.owner           		|| pConst.DOT_CHARACTER    || aPgMview.view_name			||
                        pConst.WHERE_COMMAND     		|| pInnerRowid             ||
                        pConst.IN_SELECT_COMMAND 		|| pInnerAlias             || pConst.DOT_CHARACTER    		|| 
						pConst.MV_M_ROW$_SOURCE_COLUMN	|| tFromClause     		   || pConst.CLOSE_BRACKET;


    EXECUTE tSqlStatement
    USING   pRowIDs;
	
    tSqlStatement :=    pConst.INSERT_INTO       ||
                        aPgMview.owner           || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
                        pConst.OPEN_BRACKET      || aPgMview.pgmv_columns   || pConst.CLOSE_BRACKET ||
                        pConst.SELECT_COMMAND    || aPgMview.select_columns ||
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
FUNCTION    mv$insertPgMviewOuterJoinDetails
			(	pConst                IN      mv$allConstants,
                pOwner                IN      TEXT,
                pViewName             IN      TEXT,
                pSelectColumns        IN      TEXT,
				pTableNames			  IN	  TEXT,
                pAliasArray           IN      TEXT[],
                pRowidArray           IN      TEXT[],
                pOuterTableArray      IN      TEXT[],
	            pouterLeftAliasArray  IN      TEXT[],
	            pOuterRightAliasArray IN      TEXT[],
	            pLeftOuterJoinArray   IN      TEXT[],
	            pRightOuterJoinArray  IN      TEXT[]
			 )
    RETURNS VOID
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertPgMviewOuterJoinDetails
Author:       David Day
Date:         25/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
01/07/2019  | D Day      	| Initial version
28/04/2020	| D Day			| Added mv$OuterJoinToInnerJoinReplacement function call to replace alias matching outer join conditions
			|				| with inner join conditions and new IN parameter pTableNames.
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Dynamically builds UPDATE statement(s) for any outer join table to nullify all the alias outer join column(s)
				including rowid held in the materialized view table when an DELETE is done against the 
				source table. This logic support outer join table parent to child join relationships so that all child table columns
				and linking rowids are included in the UPDATE statement.
				
				This function inserts data into the data dictionary table pgmview_oj_details 

Arguments:      IN      pConst	

Arguments:      IN      pOwner                  The owner of the object
                IN      pViewName               The name of the materialized view
				IN		pSelectColumns		    The column list from the SQL query that will be used to build the UPDATE statement
				IN      pTableNames				The table name join conditions from the SQL query will be used to replace alias table outer joins with inner joins
                IN      pAliasArray             An array that holds the list of table aliases
                IN      pRowidArray    		    An array that holds the list of rowid columns
                IN      pOuterTableArray        An array that holds the list of outer joined tables in a multi table materialized view
                IN      pouterLeftAliasArray    An array that holds the list of outer joined tables left aliases
                IN      pOuterRightAliasArray   An array that holds the list of outer joined tables right aliases
                IN      pLeftOuterJoinArray     An array that holds the the position list of whether it was a left outer join
                IN      pRightOuterJoinArray    An array that holds the the position list of whether it was a right outer join

Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
	
	iColumnNameAliasCnt				INTEGER DEFAULT 0;	
	
	tRegexp_rowid					TEXT;
	tSelectColumns					TEXT;
	tColumnNameAlias				TEXT;
	tRegExpColumnNameAlias			TEXT;
	tColumnNameArray 				TEXT[];	
	tColumnNameSql 					TEXT;
	tMvColumnName					TEXT;
	tTableName						TEXT;
	tMvRowidColumnName				TEXT;
	iMvColumnNameExists				INTEGER DEFAULT 0;	
	iMvColumnNameLoopCnt			INTEGER DEFAULT 0;
	
	tUpdateSetSql					TEXT;
	tSqlStatement					TEXT;
	tWhereClause					TEXT;

    rPgMviewColumnNames     		RECORD;
	rMvOuterJoinDetails				RECORD;
	rAliasJoinLinks					RECORD;
	rBuildAliasArray				RECORD;
	rMainAliasArray					RECORD;
	rLeftOuterJoinAliasArray		RECORD;
	rRightJoinAliasArray			RECORD;
	rRightOuterJoinAliasArray		RECORD;
	rLeftJoinAliasArray				RECORD;
	
	iWhileCounter					INTEGER DEFAULT 0;
	iAliasJoinLinksCounter			INTEGER DEFAULT 0;	
	iMainLoopCounter				INTEGER DEFAULT 0;
	iWhileLoopCounter			    INTEGER DEFAULT 0;
	iLoopCounter					INTEGER DEFAULT 0;
	iRightLoopCounter				INTEGER DEFAULT 0;
	iLeftAliasLoopCounter			INTEGER DEFAULT 0;
	iRightAliasLoopCounter			INTEGER DEFAULT 0;
	iLeftLoopCounter				INTEGER DEFAULT 0;
	iColumnNameAliasLoopCnt			INTEGER DEFAULT 0;
	
	tOuterJoinAlias					TEXT;	
	tAlias							TEXT;	
	
	tParentToChildAliasArray		TEXT[];	
	tAliasArray						TEXT[];
	tMainAliasArray					TEXT[];
	tRightJoinAliasArray			TEXT[];
	tBuildAliasArray				TEXT[];
	tLeftJoinAliasArray				TEXT[];
	
	tRightJoinAliasExists			TEXT DEFAULT 'N';	
	tLeftJoinAliasExists			TEXT DEFAULT 'N';
	
	tIsTrueOrFalse					TEXT;
	
	tClauseJoinReplacement			TEXT;		
	
BEGIN

	FOR rMvOuterJoinDetails IN (SELECT inline.oj_table AS table_name
								,      inline.oj_table_alias AS table_name_alias
								,	   inline.oj_rowid AS rowid_column_name
								,      inline.oj_outer_left_alias AS outer_left_alias
								,      inline.oj_outer_right_alias AS outer_right_alias
								,      inline.oj_left_outer_join AS left_outer_join
								,      inline.oj_right_outer_join AS right_outer_join
								FROM (
									SELECT 	UNNEST(pOuterTableArray) AS oj_table
									, 		UNNEST(pAliasArray) AS oj_table_alias
									, 		UNNEST(pRowidArray) AS oj_rowid
								    ,       UNNEST(pOuterLeftAliasArray) AS oj_outer_left_alias
									,		UNNEST(pOuterRightAliasArray) AS oj_outer_right_alias
									,		UNNEST(pLeftOuterJoinArray) AS oj_left_outer_join
									,		UNNEST(pRightOuterJoinArray) AS oj_right_outer_join) inline
								WHERE inline.oj_table IS NOT NULL) 
	LOOP
	
		iMainLoopCounter := iMainLoopCounter +1;		
		tOuterJoinAlias := TRIM(REPLACE(rMvOuterJoinDetails.table_name_alias,'.',''));
		iWhileLoopCounter := 0;
		iWhileCounter := 0;	
		tParentToChildAliasArray[iMainLoopCounter] := tOuterJoinAlias;
		tAliasArray[iMainLoopCounter] := tOuterJoinAlias;
										
		WHILE iWhileCounter = 0 LOOP
		
			IF rMvOuterJoinDetails.left_outer_join = pConst.LEFT_OUTER_JOIN THEN			
			
				iWhileLoopCounter := iWhileLoopCounter +1;
				tMainAliasArray := '{}';
				
				IF tAliasArray <> '{}' THEN
			
					tMainAliasArray[iWhileLoopCounter] := tAliasArray;
	
					FOR rMainAliasArray IN (SELECT UNNEST(tMainAliasArray) AS left_alias) LOOP
					
						tOuterJoinAlias := TRIM(REPLACE(rMainAliasArray.left_alias,'{',''));
						tOuterJoinAlias := TRIM(REPLACE(tOuterJoinAlias,'}',''));
						iLeftAliasLoopCounter := 0;
					
						FOR rLeftOuterJoinAliasArray IN (SELECT UNNEST(pOuterLeftAliasArray) as left_alias) LOOP
				
							IF rLeftOuterJoinAliasArray.left_alias = tOuterJoinAlias THEN
								iLeftAliasLoopCounter := iLeftAliasLoopCounter +1;
							END IF;
			
						END LOOP;
						
						IF iLeftAliasLoopCounter > 0 THEN 
								
							SELECT 	pChildAliasArray 
							FROM 	mv$checkParentToChildOuterJoinAlias(
																pConst
														,		tOuterJoinAlias
														,		rMvOuterJoinDetails.left_outer_join
														,		pOuterLeftAliasArray
														,		pOuterRightAliasArray
														,		pLeftOuterJoinArray) 
							INTO	tRightJoinAliasArray;

							IF tRightJoinAliasArray = '{}' THEN
								tRightJoinAliasExists := 'N';
								--RAISE INFO 'No Left Aliases Match Right Aliases';
							ELSE
								iRightLoopCounter := 0;

								FOR rRightJoinAliasArray IN (SELECT UNNEST(tRightJoinAliasArray) as right_join_alias) LOOP
									
									iRightLoopCounter := iRightLoopCounter +1;
									iMainLoopCounter := iMainLoopCounter +1;
									tParentToChildAliasArray[iMainLoopCounter] := rRightJoinAliasArray.right_join_alias;
									tRightJoinAliasExists := 'Y';
									tBuildAliasArray[iRightLoopCounter] := rRightJoinAliasArray.right_join_alias;

								END LOOP;
							END IF;

							IF (tRightJoinAliasArray <> '{}' OR tRightJoinAliasExists = 'Y') THEN

								tAliasArray := '{}';

								FOR rBuildAliasArray IN (SELECT UNNEST(tBuildAliasArray) AS right_join_alias) LOOP

									iLoopCounter = iLoopCounter +1;
									iLeftAliasLoopCounter := 0;

									FOR rLeftOuterJoinAliasArray IN (SELECT UNNEST(pOuterLeftAliasArray) AS left_alias) LOOP

										IF rMainAliasArray.left_alias = rBuildAliasArray.right_join_alias THEN
											iLeftAliasLoopCounter := iLeftAliasLoopCounter +1;
										END IF;

									END LOOP;

									IF iLeftAliasLoopCounter > 0 THEN
										tAliasArray[iLoopCounter] := rBuildAliasArray.right_join_alias;
									END IF;

								END LOOP;

							ELSE

								tRightJoinAliasExists = 'N';
								tRightJoinAliasArray = '{}';
								tAliasArray = '{}';

							END IF;

						ELSE
						
							tRightJoinAliasExists = 'N';
							tRightJoinAliasArray = '{}';
							tAliasArray = '{}';					
						
						END IF;
						
					END LOOP;
				
				ELSE
					iWhileCounter := 1;	
				END IF;
				
			ELSIF rMvOuterJoinDetails.right_outer_join = pConst.RIGHT_OUTER_JOIN THEN
			
				iWhileLoopCounter := iWhileLoopCounter +1;
				tMainAliasArray := '{}';
				
				IF tAliasArray <> '{}' THEN
			
					tMainAliasArray[iWhileLoopCounter] := tAliasArray;
	
					FOR rMainAliasArray IN (SELECT UNNEST(tMainAliasArray) AS right_alias) LOOP
					
						tOuterJoinAlias := TRIM(REPLACE(rMainAliasArray.right_alias,'{',''));
						tOuterJoinAlias := TRIM(REPLACE(tOuterJoinAlias,'}',''));
						iRightAliasLoopCounter := 0;
					
						FOR rRightOuterJoinAliasArray IN (SELECT UNNEST(pOuterRightAliasArray) as right_alias) LOOP
				
							IF rRightOuterJoinAliasArray.right_alias = tOuterJoinAlias THEN
								iRightAliasLoopCounter := iRightAliasLoopCounter +1;
							END IF;
			
						END LOOP;
						
						IF iRightAliasLoopCounter > 0 THEN 
								
							SELECT 	pChildAliasArray 
							FROM 	mv$checkParentToChildOuterJoinAlias(
																pConst
														,		tOuterJoinAlias
														,		rMvOuterJoinDetails.right_outer_join
														,		pOuterLeftAliasArray
														,		pOuterRightAliasArray
														,		pRightOuterJoinArray) 
							INTO	tLeftJoinAliasArray;

							IF tLeftJoinAliasArray = '{}' THEN
								tLeftJoinAliasExists := 'N';
								--RAISE INFO 'No Right Aliases Match Left Aliases';
							ELSE
								iLeftLoopCounter := 0;

								FOR rLeftJoinAliasArray IN (SELECT UNNEST(tLeftJoinAliasArray) as left_join_alias) LOOP
									
									iLeftLoopCounter := iLeftLoopCounter +1;
									iMainLoopCounter := iMainLoopCounter +1;
									tParentToChildAliasArray[iMainLoopCounter] := rLeftJoinAliasArray.left_join_alias;
									tLeftJoinAliasExists := 'Y';
									tBuildAliasArray[iLeftLoopCounter] := rLeftJoinAliasArray.left_join_alias;

								END LOOP;
							END IF;

							IF (tLeftJoinAliasArray <> '{}' OR tLeftJoinAliasExists = 'Y') THEN

								tAliasArray := '{}';

								FOR rBuildAliasArray IN (SELECT UNNEST(tBuildAliasArray) AS left_join_alias) LOOP

									iLoopCounter = iLoopCounter +1;
									iRightAliasLoopCounter := 0;

									FOR rRightOuterJoinAliasArray IN (SELECT UNNEST(pOuterRightAliasArray) AS right_alias) LOOP

										IF rMainAliasArray.right_alias = rBuildAliasArray.left_join_alias THEN
											iRightAliasLoopCounter := iRightAliasLoopCounter +1;
										END IF;

									END LOOP;

									IF iRightAliasLoopCounter > 0 THEN
										tAliasArray[iLoopCounter] := rBuildAliasArray.left_join_alias;
									END IF;

								END LOOP;

							ELSE

								tLeftJoinAliasExists = 'N';
								tLeftJoinAliasArray = '{}';
								tAliasArray = '{}';

							END IF;

						ELSE
						
							tLeftJoinAliasExists = 'N';
							tLeftJoinAliasArray = '{}';
							tAliasArray = '{}';					
						
						END IF;
						
					END LOOP;
				
				ELSE
					iWhileCounter := 1;	
				END IF;
			
			END IF;
			
		END LOOP;
		
		-- Key values for the main UPDATE statement breakdown
		tMvRowidColumnName 		:= rMvOuterJoinDetails.rowid_column_name;
		tWhereClause 			:= pConst.WHERE_COMMAND || tMvRowidColumnName  || pConst.IN_ROWID_LIST;
		tColumnNameAlias 		:= rMvOuterJoinDetails.table_name_alias;
		tTableName 				:= rMvOuterJoinDetails.table_name;
		tColumnNameArray	 	:= '{}';
		tUpdateSetSql 		 	:= ' ';
		iMvColumnNameLoopCnt 	:= 0;
		iAliasJoinLinksCounter 	:= 0;
		iColumnNameAliasLoopCnt := 0;
		
		-- Building the UPDATE statement including any child relationship columns and m_row$ based on these aliases
		FOR rAliasJoinLinks IN (SELECT UNNEST(tParentToChildAliasArray) AS alias) LOOP
		
			iAliasJoinLinksCounter 	:= iAliasJoinLinksCounter +1;
			tAlias 					:= rAliasJoinLinks.alias||'.';
			tSelectColumns 			:= SUBSTRING(pSelectColumns,1,mv$regExpInstr(pSelectColumns,'[,]+[[:alnum:]]+[.]+'||'m_row\$'||''));
			tRegExpColumnNameAlias 	:= REPLACE(tAlias,'.','\.');
			iColumnNameAliasCnt 	:= mv$regExpCount(tSelectColumns, '[A-Za-z0-9]+('||tRegExpColumnNameAlias||')', 1);
			iColumnNameAliasCnt 	:= mv$regExpCount(tSelectColumns, '('||tRegExpColumnNameAlias||')', 1) - iColumnNameAliasCnt;
		
			IF iColumnNameAliasCnt > 0 THEN
		
				FOR i IN 1..iColumnNameAliasCnt 
				LOOP
				
					tColumnNameSql := SUBSTRING(tSelectColumns,mv$regExpInstr(tSelectColumns,
							 tRegExpColumnNameAlias,
							 1,
							 i)-1);
					tColumnNameSql := mv$regExpReplace(tColumnNameSql,'(^[[:space:]]+)',null);
					tColumnNameSql := mv$regExpSubstr(tColumnNameSql,'(.*'||tRegExpColumnNameAlias||'+[[:alnum:]]+(.*?[^,|$]))',1,1,'i');
					tMvColumnName  := TRIM(REPLACE(mv$regExpSubstr(tColumnNameSql, '\S+$'),',',''));
					tMvColumnName  := LOWER(TRIM(REPLACE(tMvColumnName,tAlias,'')));
					
					FOR rPgMviewColumnNames IN (SELECT column_name
												FROM   information_schema.columns
												WHERE  table_schema    = LOWER( pOwner )
												AND    table_name      = LOWER( pViewName ) )
					LOOP
					
						IF rPgMviewColumnNames.column_name = tMvColumnName THEN
										
							iMvColumnNameLoopCnt := iMvColumnNameLoopCnt + 1;	

							-- Check for duplicates
							SELECT tMvColumnName = ANY (tColumnNameArray) INTO tIsTrueOrFalse;	
							
							IF tIsTrueOrFalse = 'false' THEN

								iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;	
								
								tColumnNameArray[iColumnNameAliasLoopCnt] := tMvColumnName;
								
								IF iMvColumnNameLoopCnt = 1 THEN 	
									tUpdateSetSql := pConst.SET_COMMAND || tMvColumnName || pConst.EQUALS_NULL || pConst.COMMA_CHARACTER;
								ELSE	
									tUpdateSetSql := tUpdateSetSql || tMvColumnName || pConst.EQUALS_NULL || pConst.COMMA_CHARACTER ;
								END IF;
								
							END IF;
						
							EXIT WHEN iMvColumnNameLoopCnt > 0;
							
						END IF;

					END LOOP;
					
				END LOOP;
				
				iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;
				tColumnNameArray[iColumnNameAliasLoopCnt] := rAliasJoinLinks.alias|| pConst.UNDERSCORE_CHARACTER || pConst.MV_M_ROW$_COLUMN;
				tUpdateSetSql := tUpdateSetSql || rAliasJoinLinks.alias|| pConst.UNDERSCORE_CHARACTER || pConst.MV_M_ROW$_COLUMN || pConst.EQUALS_NULL || pConst.COMMA_CHARACTER;
				
			ELSE
				IF iAliasJoinLinksCounter = 1 THEN
					iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;
					tColumnNameArray[iColumnNameAliasLoopCnt] := rAliasJoinLinks.alias|| pConst.UNDERSCORE_CHARACTER || pConst.MV_M_ROW$_COLUMN;
					tUpdateSetSql := pConst.SET_COMMAND || rAliasJoinLinks.alias|| pConst.UNDERSCORE_CHARACTER || pConst.MV_M_ROW$_COLUMN || pConst.EQUALS_NULL || pConst.COMMA_CHARACTER;			
				ELSE
					iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;
					tColumnNameArray[iColumnNameAliasLoopCnt] := rAliasJoinLinks.alias|| pConst.UNDERSCORE_CHARACTER || pConst.MV_M_ROW$_COLUMN;
					tUpdateSetSql := tUpdateSetSql || rAliasJoinLinks.alias || pConst.UNDERSCORE_CHARACTER || pConst.MV_M_ROW$_COLUMN || pConst.EQUALS_NULL || pConst.COMMA_CHARACTER;		
				END IF;
					
			END IF;
		
		END LOOP;
		
		tUpdateSetSql := SUBSTRING(tUpdateSetSql,1,length(tUpdateSetSql)-1);
		
		tSqlStatement := pConst.UPDATE_COMMAND ||
						 pOwner		|| pConst.DOT_CHARACTER		|| pViewName	|| pConst.NEW_LINE		||
						 tUpdateSetSql || pConst.NEW_LINE ||
						 tWhereClause;
						 
		tClauseJoinReplacement := mv$OuterJoinToInnerJoinReplacement(pConst, pTableNames, tColumnNameAlias);
		
		INSERT INTO pg$mviews_oj_details
		(	owner
		,	view_name
		,	table_alias
		,   rowid_column_name
		,   source_table_name
		,   column_name_array
		,   update_sql
		,   join_replacement_from_sql)
		VALUES
		(	pOwner
		,	pViewName
		,   tColumnNameAlias
		,   tMvRowidColumnName
		,   tTableName
		,   tColumnNameArray
		,	tSqlStatement
		,   tClauseJoinReplacement);
		
		iMainLoopCounter := 0;
		tParentToChildAliasArray := '{}';
		tAliasArray  := '{}';
		tMainAliasArray := '{}';
		iWhileCounter := 0;
		iWhileLoopCounter := 0;
		iLoopCounter := 0;
		
		
	END LOOP;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$insertPgMviewOuterJoinDetails';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$checkParentToChildOuterJoinAlias(
	pConst 					IN	"mv$allconstants",
	pAlias 					IN	text,
	pOuterJoinType 			IN	text,
	pOuterLeftAliasArray 	IN	text[],
	pOuterRightAliasArray 	IN	text[],
	pOuterJoinTypeArray 	IN 	text[],
	pChildAliasArray 		OUT text[])
    RETURNS text[]
AS $BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$checkParentToChildOuterJoinAlias
Author:       David Day
Date:         18/07/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
18/07/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description: 	Function to check either left or right outer join parent to child column joining aliases to be used to build
				the dynamic UPDATE statement for outer join table DELETE changes.

Arguments:      IN      pConst	

Arguments:      IN      pAlias           
                IN      pOuterJoinType        
				IN		pOuterLeftAliasArray	
                IN      pOuterRightAliasArray  
                IN      pOuterJoinTypeArray
                OUT     pChildAliasArray		

Returns:                OUT array value for parameter pChildAliasArray
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
	
	
	rMvOuterJoinDetails     RECORD;
	iLoopCounter            INTEGER DEFAULT 0;
	
BEGIN

	pChildAliasArray := '{}';

	FOR rMvOuterJoinDetails IN (SELECT inline.oj_left_alias
								,	   inline.oj_right_alias
								,      inline.oj_type
								FROM (
									SELECT 	UNNEST(pOuterLeftAliasArray) AS oj_left_alias
									,		UNNEST(pOuterRightAliasArray) AS oj_right_alias
									, 		UNNEST(pOuterJoinTypeArray) AS oj_type) inline
								WHERE inline.oj_type = pOuterJoinType) 
	LOOP
		
		iLoopCounter := iLoopCounter + 1;
		
		IF iLoopCounter = 1 THEN
			pChildAliasArray := '{}';
		END IF;
	
		IF pAlias = rMvOuterJoinDetails.oj_left_alias AND pOuterJoinType = pConst.LEFT_OUTER_JOIN THEN
		
			pChildAliasArray[iLoopCounter] := rMvOuterJoinDetails.oj_right_alias;
			
		ELSIF pAlias = rMvOuterJoinDetails.oj_right_alias AND pOuterJoinType = pConst.RIGHT_OUTER_JOIN THEN
		
			pChildAliasArray[iLoopCounter] := rMvOuterJoinDetails.oj_left_alias;
		
		END IF;
		
	END LOOP;
	
	RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$checkParentToChildOuterJoinAlias';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE
FUNCTION    mv$updateOuterJoinColumnsNull
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                ptablealias     IN      TEXT,
                pRowidColumn    IN      TEXT,
                pRowIDs         IN      UUID[]
            )
    RETURNS VOID
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$updateOuterJoinColumnsNull
Author:       David Day
Date:         25/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
25/06/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Executes UPDATE statement to nullify outer join columns held in the materialized view table when a DELETE has been
				done against the source table.
				
				A decision was made that an UPDATE would be the more per-formant way of deleting the data rather than to get
				the inner join rowids to allow the rows to be deleted and inserted back if the inner join conditions still match.
				
				Due to the overhead of getting the inner join rowids from the materialized view to allow this to happen in this scenario.

Arguments:      IN      pConst	

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pTableAlias         The alias for the outer join table
                IN      pRowidColumn    	The name of the outer join rowid column
                IN      pRowID              The unique identifier to locate the row			

Returns:                VOID
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   				TEXT;

BEGIN	

	SELECT update_sql INTO tSqlStatement
	FROM pg$mviews_oj_details
	WHERE owner = pOwner
	AND view_name = pViewName
	AND table_alias = ptablealias
	AND rowid_column_name = pRowidColumn;
	
	EXECUTE tSqlStatement
	USING   pRowIDs;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$updateOuterJoinColumnsNull';
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
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pBitmap         IN      BIGINT[]
            )
    RETURNS SMALLINT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setPgMviewLogBit
Author:       Mike Revitt
Date:         12/01/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
08/10/2019  | D Day         | Changed returns type from INTEGER to SMALLINT to match the bit data type.
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

    tBitValue   mv$bitValue;

BEGIN

    tBitValue   := mv$findFirstFreeBit( pConst, pBitmap );

    UPDATE  pg$mview_logs
    SET     pg_mview_bitmap[tBitValue.BIT_ROW] = COALESCE( pg_mview_bitmap[tBitValue.BIT_ROW], pConst.BITMAP_NOT_SET ) + tBitValue.BIT_MAP
    WHERE   owner           = pOwner
    AND     pglog$_name     = pPgLog$Name;

    RETURN( tBitValue.BIT_VALUE );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$setPgMviewLogBit';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$updateMaterializedViewRows
            (
                pConst          IN      mv$allConstants,
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
    aPgMview        pg$mviews;
    bBaseRowExists  BOOLEAN := FALSE;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
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
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$regExpCount(
						p_src_string text,
						p_regexp_pat CHARACTER VARYING,
						p_position NUMERIC DEFAULT 1,
						p_match_param CHARACTER VARYING DEFAULT 'c'::character varying)
		RETURNS INTEGER
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$regExpCount
Author:       David Day
Date:         03/07/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/07/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Function to use regular expression pattern to count the total amount of occurrences of the input parameter p_src_string

Arguments:      IN      p_src_string             
                IN      p_regexp_pat           	  
                IN      p_position         		  
                IN      p_match_param			              
Returns:                INTEGER

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/

DECLARE
    v_res_count INTEGER;
    v_position INTEGER := floor(p_position);
    v_match_param VARCHAR := trim(p_match_param);
    v_src_string TEXT := substr(p_src_string, v_position);
BEGIN
    IF (coalesce(p_src_string, '') = '' OR coalesce(p_regexp_pat, '') = '' OR p_position IS NULL)
    THEN
        RETURN NULL;
    ELSIF (v_position <= 0) THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "3" (start position) should be greater than or equal to 1';
    ELSIF (coalesce(v_match_param, '') = '') THEN
        v_match_param := 'c';
    ELSIF (v_match_param !~ 'i|c|n|m|x') THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "4" (match_parameter) must be one of the following: "i", "c", "n", "m", "x"';
    END IF;

    v_match_param := concat('g', v_match_param);
    v_match_param := regexp_replace(v_match_param, 'm|x', '', 'g');
    v_match_param := CASE
                       WHEN v_match_param !~ 'n' THEN concat(v_match_param, 'p')
                       ELSE regexp_replace(v_match_param, 'n', '', 'g')
                    END;

    SELECT COUNT(regexpval)::INTEGER
      INTO v_res_count
      FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rownum,
                   regexpval
              FROM (SELECT unnest(regexp_matches(v_src_string,
                                                 p_regexp_pat,
                                                 v_match_param)) AS regexpval
                   ) AS regexpvals
             WHERE char_length(regexpval) > 0
           ) AS rankexpvals;

    RETURN v_res_count;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$regExpInstr(
	p_src_string TEXT,
	p_regexp_pat CHARACTER VARYING,
	p_position NUMERIC DEFAULT 1,
	p_occurrence NUMERIC DEFAULT 1,
	p_retopt NUMERIC DEFAULT 0,
	p_match_param CHARACTER VARYING DEFAULT 'c'::character varying)
    RETURNS INTEGER
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$regExpInstr
Author:       David Day
Date:         03/07/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/07/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:  Function to use regular expression pattern to evaluate strings using characters as defined by the input character set.
			  It returns an integer indicating the beginning or ending position of the matched string depending on the value of the
			  p_retopt argument. If no match is found it returns 0.

Arguments:      IN      p_src_string             
                IN      p_regexp_pat           	  
                IN      p_position   
				IN		p_occurrence
				IN      p_retopt
                IN      p_match_param			              
Returns:                INTEGER

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    v_resposition INTEGER;
    v_regexpres_row RECORD;
    v_match_count INTEGER := 0;
    v_retopt INTEGER := floor(p_retopt);
    v_position INTEGER := floor(p_position);
    v_occurrence INTEGER := floor(p_occurrence);
    v_match_param VARCHAR := trim(p_match_param);
    v_src_string TEXT := substr(p_src_string, v_position);
    v_srcstr_len INTEGER := char_length(v_src_string);
BEGIN
    IF (coalesce(p_src_string, '') = '' OR coalesce(p_regexp_pat, '') = '' OR
        p_position IS NULL OR p_occurrence IS NULL OR p_retopt IS NULL)
    THEN
        RETURN NULL;
    ELSIF (v_position <= 0) THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "3" (start position) should be greater than or equal to 1';
    ELSIF (v_occurrence <= 0) THEN
        RAISE EXCEPTION 'The value of the argument parameter in position "4" (occurrence of match) should be greater than or equal to 1';
    ELSIF (v_retopt NOT IN (0, 1)) THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "5" (return-option) should be either 0 or 1';
    ELSIF (coalesce(v_match_param, '') = '') THEN
        v_match_param := 'c';
    ELSIF (v_match_param !~ 'i|c|n|m|x') THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "6" (match_parameter) must be one of the following: "i", "c", "n", "m", "x"';
    END IF;

    v_match_param := concat('g', v_match_param);
    v_match_param := regexp_replace(v_match_param, 'm|x', '', 'g');
    v_match_param := CASE
                       WHEN v_match_param !~ 'n' THEN concat(v_match_param, 'p')
                       ELSE regexp_replace(v_match_param, 'n', '', 'g')
                    END;

    FOR v_regexpres_row IN
    (SELECT rownum,
            regexpval,
            char_length(regexpval) AS value_len
       FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rownum,
                    regexpval
               FROM (SELECT unnest(regexp_matches(v_src_string,
                                                  p_regexp_pat,
                                                  v_match_param)) AS regexpval
                    ) AS regexpvals
              WHERE char_length(regexpval) > 0
            ) AS rankexpvals
      ORDER BY rownum ASC)
    LOOP
        v_src_string := substr(v_src_string, strpos(v_src_string, v_regexpres_row.regexpval) + v_regexpres_row.value_len);
        v_resposition := v_srcstr_len - char_length(v_src_string) - v_regexpres_row.value_len + 1;

        IF (v_position > 1) THEN
            v_resposition := v_resposition + v_position - 1;
        END IF;

        IF (v_retopt = 1) THEN
            v_resposition := v_resposition + v_regexpres_row.value_len;
        END IF;

        v_match_count := v_regexpres_row.rownum;
        EXIT WHEN v_match_count = v_occurrence;
    END LOOP;

    RETURN CASE
              WHEN v_match_count != v_occurrence THEN 0
              ELSE v_resposition
           END;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$regExpReplace(
	p_srcstring TEXT,
	p_regexppat CHARACTER VARYING,
	p_replacestring text DEFAULT ''::text,
	p_position INTEGER DEFAULT 1,
	p_occurrence INTEGER DEFAULT 0,
	p_matchparam CHARACTER VARYING DEFAULT 'c'::character varying)
    RETURNS TEXT
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$regExpReplace
Author:       David Day
Date:         03/07/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/07/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Function to use regular expression pattern to replace value(s) from the input parameter p_src_string

Arguments:      IN      p_srcstring             
                IN      p_regexppat
				IN		p_replacestring text
                IN      p_position
				IN 		p_occurrence
                IN      p_matchparam			              
Returns:                TEXT

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    v_resstring TEXT;
    v_regexpval TEXT;
    v_resposition INTEGER;
    v_regexpres_row RECORD;
    v_match_count INTEGER := 0;
    v_matchparam VARCHAR := trim(p_matchparam);
    v_srcstring TEXT := substr(p_srcstring, p_position);
    v_srcstrlen INTEGER := char_length(v_srcstring);
	
BEGIN
    -- Possible combinations of the input parameters (processing some of them)
    IF (char_length(v_srcstring) = 0 AND char_length(p_regexppat) = 0 AND p_position = 1 AND p_occurrence IN (0, 1)) THEN
        RETURN p_replacestring;
    ELSIF (char_length(v_srcstring) != 0 AND char_length(p_regexppat) = 0) THEN
        RETURN p_srcstring;
    END IF;

    -- Block of input parameters validation checks
    IF (coalesce(p_srcstring, '') = '' OR coalesce(p_regexppat, '') = '' OR p_position IS NULL OR p_occurrence IS NULL) THEN
        RETURN NULL;
    ELSIF (p_position <= 0) THEN
        RAISE EXCEPTION 'The value for parameter in position "4" (start position) should be greater than or equal to 1';
    ELSIF (p_occurrence < 0) THEN
        RAISE EXCEPTION 'The value for parameter in position "5" (occurrence of match) should be greater than or equal to 0';
    ELSIF (coalesce(v_matchparam, '') = '') THEN
        v_matchparam := 'c';
    ELSIF (v_matchparam !~ 'i|c|n|m|x') THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "6" (match_parameter) must be one of the following: "i", "c", "n", "m", "x"';
    END IF;
																											  
-- Translate regexp flags (match_parameter) between matching engines
    v_matchparam := concat('g', v_matchparam);
    v_matchparam := regexp_replace(v_matchparam, 'm|x', '', 'g');
    v_matchparam := CASE
                       WHEN v_matchparam !~ 'n' THEN concat(v_matchparam, 'p')
                       ELSE regexp_replace(v_matchparam, 'n', '', 'g')
                    END;

    -- Replace all occurrences of match if particular one isn't specified
    IF (p_occurrence = 0) THEN
        v_resstring := regexp_replace(v_srcstring,
                                      p_regexppat,
                                      coalesce(p_replacestring, ''),
                                      v_matchparam);

        v_resstring := concat(substr(p_srcstring, 1, p_position - 1), v_resstring);
    -- Replace the particular occurrence of regexp match (specified as "p_occurrence" param)
    ELSE
        FOR v_regexpres_row IN
        (SELECT rownum,
                regexpval,
                char_length(regexpval) AS value_len
           FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rownum,
                        regexpval
                   FROM (SELECT unnest(regexp_matches(v_srcstring,
                                                      p_regexppat,
                                                      v_matchparam)) AS regexpval
                        ) AS regexpvals
                  WHERE char_length(regexpval) > 0
                ) AS rankexpvals
          ORDER BY rownum ASC)
        LOOP
            v_regexpval := v_regexpres_row.regexpval;
            v_srcstring := substr(v_srcstring, strpos(v_srcstring, v_regexpval) + v_regexpres_row.value_len);
            v_resposition := v_srcstrlen - char_length(v_srcstring) - v_regexpres_row.value_len + 1;

            IF (p_position > 1) THEN
                v_resposition := v_resposition + p_position - 1;
            END IF;

            v_match_count := v_regexpres_row.rownum;
            EXIT WHEN v_match_count = p_occurrence;
        END LOOP;

        IF (v_match_count = p_occurrence) THEN
            v_resstring := concat(substr(p_srcstring, 0, v_resposition),
                           p_replacestring,
                           substr(p_srcstring, v_resposition + char_length(v_regexpval)));
        END IF;
    END IF;

    RETURN coalesce(v_resstring, p_srcstring);
END;
																		   
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$regExpSubstr(
	p_src_string TEXT,
	p_regexp_pat CHARACTER VARYING,
	p_position NUMERIC DEFAULT 1,
	p_occurrence NUMERIC DEFAULT 1,
	p_match_param CHARACTER VARYING DEFAULT 'c'::character varying)
    RETURNS TEXT
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$regExpReplace
Author:       David Day
Date:         03/07/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/07/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Function to search a string value and return the substring of itself based on the input
				regular expression pattern.

Arguments:      IN      p_srcstring             
                IN      p_regexp_pat
                IN      p_position
				IN 		p_occurrence
                IN      p_match_param			              
Returns:                TEXT

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    v_res_string TEXT;
    v_regexp_val TEXT;
    v_regexpres_row RECORD;
    v_match_count INTEGER := 0;
    v_position INTEGER := floor(p_position);
    v_occurrence INTEGER := floor(p_occurrence);
    v_match_param VARCHAR := trim(p_match_param);
    v_src_string TEXT := substr(p_src_string, v_position);
BEGIN
    IF (coalesce(p_src_string, '') = '' OR coalesce(p_regexp_pat, '') = '' OR
        p_position IS NULL OR p_occurrence IS NULL)
    THEN
        RETURN NULL;
    ELSIF (v_position <= 0) THEN
        RAISE EXCEPTION 'The value for parameter in position "3" (start position) should be greater than or equal to 1';
    ELSIF (v_occurrence < 0) THEN
        RAISE EXCEPTION 'The value for parameter in position "4" (occurrence of match) should be greater than or equal to 1';
    ELSIF (coalesce(v_match_param, '') = '') THEN
        v_match_param := 'c';
    ELSIF (v_match_param !~ 'i|c|n|m|x') THEN
        RAISE EXCEPTION 'The value of the argument for parameter in position "5" (match_parameter) must be one of the following: "i", "c", "n", "m", "x"';
    END IF;

    v_match_param := concat('g', v_match_param);
    v_match_param := regexp_replace(v_match_param, 'm|x', '', 'g');
    v_match_param := CASE
                       WHEN v_match_param !~ 'n' THEN concat(v_match_param, 'p')
                       ELSE regexp_replace(v_match_param, 'n', '', 'g')
                    END;

    FOR v_regexpres_row IN
    (SELECT rownum,
            regexpval,
            char_length(regexpval) AS value_len
       FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rownum,
                    regexpval
               FROM (SELECT unnest(regexp_matches(v_src_string,
                                                  p_regexp_pat,
                                                  v_match_param)) AS regexpval
                    ) AS regexpvals
              WHERE char_length(regexpval) > 0
            ) AS rankexpvals
      ORDER BY rownum ASC)
    LOOP
        v_match_count := v_regexpres_row.rownum;
        v_regexp_val := v_regexpres_row.regexpval;
        v_src_string := substr(v_src_string, strpos(v_src_string, v_regexp_val) + v_regexpres_row.value_len);

        IF (v_match_count = v_occurrence) THEN
            v_res_string := v_regexp_val;
            EXIT;
        END IF;
    END LOOP;

    RETURN v_res_string;
END;

$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$outerJoinToInnerJoinReplacement(
	pConst          IN      mv$allConstants,
	pTableNames 	IN		TEXT,
	pTableAlias 	IN		TEXT)
    RETURNS text
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$outerJoinToInnerJoinReplacement
Author:       David Day
Date:         28/04/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
28/04/2020  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Function to replace the alias driven outer join conditions to inner join in the from tables join sql
				regular expression pattern.

Arguments:      IN      pTableNames             
                IN      pTableAlias              
Returns:                TEXT

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE	
	
	iLeftJoinCnt 			INTEGER := 0;	
	iRightJoinCnt 			INTEGER := 0;	
	
	iLoopLeftJoinCnt		INTEGER := 0;
	iLoopRightJoinCnt		INTEGER := 0;
	
	iLeftJoinLoopAliasCnt 	INTEGER := 0;
	iRightJoinLoopAliasCnt 	INTEGER := 0;
	
	tTablesSQL 				TEXT := pTableNames;
	  
	tSQL					TEXT;
	tLeftJoinLine			TEXT;
	tRightJoinLine			TEXT;
	tOrigLeftJoinLine 		TEXT;
	tOrigRightJoinLine 		TEXT;
	
	tLeftMatched			CHAR := 'N';
	
	tTableAlias 			TEXT := REPLACE(pTableAlias,'.','\.');
	
	ls_table_name			TEXT;
	ls_column_name			TEXT;
	
	iStartPosition			INTEGER := 0;
	iEndPosition			INTEGER := 0;
	
	iLoopColNullableNoCnt	INTEGER := 0;
	
	iTabColExist			INTEGER := 0;
	iColNullableNo			INTEGER := 0;
	
	iLoopColNullableNo		INTEGER := 0;
	
	tLeftAliasColumnName	TEXT;
	tLeftAliasTableName		TEXT;
	tRightAliasColumnName	TEXT;
	tRightAliasTableName	TEXT;
	
	tTablesMarkerSQL		TEXT;

BEGIN

tTablesSQL := regexp_replace(tTablesSQL,'left join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left outer join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right outer join','RIGHT JOIN','gi');
	  
SELECT count(1) INTO iLeftJoinCnt FROM regexp_matches(tTablesSQL,'LEFT JOIN','g');
SELECT count(1) INTO iRightJoinCnt FROM regexp_matches(tTablesSQL,'RIGHT JOIN','g');

tTablesSQL :=
		TRIM (
		   mv$regexpreplace(
			  tTablesSQL,
			  '([' || CHR (11) || CHR (13) || CHR (9) || ']+)',
			  ' '));

tTablesMarkerSQL :=	mv$regexpreplace(tTablesSQL, 'LEFT JOIN',pConst.COMMA_LEFT_TOKEN);
tTablesMarkerSQL :=	mv$regexpreplace(tTablesMarkerSQL, 'RIGHT JOIN',pConst.COMMA_RIGHT_TOKEN);
tTablesMarkerSQL :=	mv$regexpreplace(tTablesMarkerSQL, 'INNER JOIN',pConst.COMMA_INNER_TOKEN);
tTablesMarkerSQL :=	mv$regexpreplace(tTablesMarkerSQL, 'JOIN',pConst.COMMA_INNER_TOKEN);

tTablesMarkerSQL := tTablesMarkerSQL||pConst.COMMA_INNER_TOKEN;
			  
tSQL := tTablesSQL;

IF iLeftJoinCnt > 0 THEN
	  
	FOR i IN 1..iLeftJoinCnt
	LOOP
	
	tLeftJoinLine :=  'LEFT JOIN'||substr(substr(tTablesMarkerSQL,mv$regexpinstr(tTablesMarkerSQL, 
	'('|| pConst.COMMA_LEFT_TOKEN ||'+)',
		1,
		i,
		1,
		'i')),1,
			   mv$regexpinstr(substr(tTablesMarkerSQL,mv$regexpinstr(tTablesMarkerSQL,
	'('|| pConst.COMMA_LEFT_TOKEN ||'+)',
					   1,
		i,
		1,
		'i')),'('||pConst.COMMA_LEFT_TOKEN||'|'||pConst.COMMA_INNER_TOKEN||'|'||pConst.COMMA_RIGHT_TOKEN||')',
		1,
		1,
		1,
		'i')-3);
		
		tOrigLeftJoinLine := tLeftJoinLine;
		
		SELECT count(1) INTO iLeftJoinLoopAliasCnt FROM regexp_matches(tLeftJoinLine,'[[:space:]]+'||tTableAlias,'g');

		IF iLeftJoinLoopAliasCnt > 0 THEN
		
			iLoopLeftJoinCnt := iLoopLeftJoinCnt +1;
			
			iStartPosition :=  mv$regexpinstr(tLeftJoinLine,'[[:space:]]+'||tTableAlias,
				1,
				1,
				1,
				'i');
				
			iEndPosition := mv$regexpinstr(tLeftJoinLine,'[[:space:]]+'||tTableAlias||'+[a-zA-Z0-9_]+',
				1,
				1,
				1,
				'i');		
				
			tLeftAliasColumnName := substr(tLeftJoinLine,iStartPosition, iEndPosition - iStartPosition);
			
			ls_column_name := TRIM(
				 mv$regexpreplace(
					tLeftAliasColumnName,
					'([' || CHR (10) || CHR (11) || CHR (13) || CHR(9) || ']+)',
					''));
					
			IF iLoopLeftJoinCnt = 1 THEN
			
				iStartPosition := mv$regexpinstr(tLeftJoinLine,
					'LEFT+[[:space:]]+JOIN+[[:space:]]+',
					1,
					1,
					1,
					'i');
					
				iEndPosition := mv$regexpinstr(tLeftJoinLine,
					'LEFT+[[:space:]]+JOIN+[[:space:]]+[a-zA-Z0-9_]+',
					1,
					1,
					1,
					'i');
					
				tLeftAliasTableName := substr(tLeftJoinLine,iStartPosition, iEndPosition - iStartPosition);
				
				ls_table_name := TRIM(
					 mv$regexpreplace(
						tLeftAliasTableName,
						'([' || CHR (10) || CHR (11) || CHR (13) || CHR(9) || ']+)',
						''));
						
			END IF;
			
			SELECT count(1) INTO iTabColExist
			FROM information_schema.columns 
			WHERE table_name=LOWER(ls_table_name)
			AND column_name=LOWER(ls_column_name);
			
			IF iTabColExist = 1 THEN
			
				SELECT count(1) INTO iColNullableNo
				FROM information_schema.columns 
				WHERE table_name=LOWER(ls_table_name) 
				AND column_name=LOWER(ls_column_name)
				AND is_nullable = 'NO';
				
				IF iColNullableNo = 1 THEN
				
					tLeftMatched := 'Y';
				
					iLoopColNullableNoCnt := iLoopColNullableNoCnt +1;
				
					IF iLoopColNullableNoCnt = 1 THEN
					
						tLeftJoinLine := replace(tLeftJoinLine,'LEFT JOIN','INNER JOIN');
						tSQL := replace(tTablesSQL,tOrigLeftJoinLine,tLeftJoinLine);

					ELSE 
					
						tLeftJoinLine := replace(tLeftJoinLine,'LEFT JOIN','INNER JOIN');
						tSQL := replace(tSQL,tOrigLeftJoinLine,tLeftJoinLine);

					END IF;
					
				END IF;

			ELSE
			
				RAISE EXCEPTION 'The value of the argument to confirm alias table name and column name exist in the data dictionary cannot be found from left join line '' % ''. Function does not handle string format.',tLeftJoinLine;
		
			END IF;

		END IF;

	END LOOP;

ELSIF iRightJoinCnt > 0 THEN

	iLoopColNullableNoCnt := 0;

	FOR i IN 1..iRightJoinCnt
	LOOP
	
		tRightJoinLine := 'RIGHT JOIN'||substr(substr(tTablesMarkerSQL,mv$regexpinstr(tTablesMarkerSQL, 
	'('|| pConst.COMMA_RIGHT_TOKEN ||'+)',
		1,
		i,
		1,
		'i')),1,
			   mv$regexpinstr(substr(tTablesMarkerSQL,mv$regexpinstr(tTablesMarkerSQL,
	'('|| pConst.COMMA_RIGHT_TOKEN ||'+)',
					   1,
		i,
		1,
		'i')),'('||pConst.COMMA_LEFT_TOKEN||'|'||pConst.COMMA_INNER_TOKEN||'|'||pConst.COMMA_RIGHT_TOKEN||')',
		1,
		1,
		1,
		'i')-3);
			
		tOrigRightJoinLine := tRightJoinLine;
			
		SELECT count(1) INTO iRightJoinLoopAliasCnt 
		FROM regexp_matches(tRightJoinLine,'[[:space:]]+'||tTableAlias,'g');

		IF iRightJoinLoopAliasCnt > 0 THEN
		
			iLoopRightJoinCnt := iLoopRightJoinCnt +1;
			
			iStartPosition :=  mv$regexpinstr(tRightJoinLine,'[[:space:]]+'||tTableAlias,
				1,
				1,
				1,
				'i');
				
			iEndPosition := mv$regexpinstr(tRightJoinLine,'[[:space:]]+'||tTableAlias||'+[a-zA-Z0-9_]+',
				1,
				1,
				1,
				'i');		
				
			tRightAliasColumnName := substr(tRightJoinLine,iStartPosition, iEndPosition - iStartPosition);
			
			ls_column_name := LOWER(TRIM(
				 mv$regexpreplace(
					tRightAliasColumnName,
					'([' || CHR (10) || CHR (11) || CHR (13) || CHR(9) || ']+)',
					'')));
		
			IF iLoopRightJoinCnt = 1 THEN
			
				iStartPosition := mv$regexpinstr(tRightJoinLine,
					'RIGHT+[[:space:]]+JOIN+[[:space:]]+',
					1,
					1,
					1,
					'i');
					
				iEndPosition := mv$regexpinstr(tRightJoinLine,
					'RIGHT+[[:space:]]+JOIN+[[:space:]]+[a-zA-Z0-9_]+',
					1,
					1,
					1,
					'i');
					
				tRightAliasTableName := substr(tRightJoinLine,iStartPosition, iEndPosition - iStartPosition);
				
				ls_table_name := LOWER(TRIM(
					 mv$regexpreplace(
						tRightAliasTableName,
						'([' || CHR (10) || CHR (11) || CHR (13) || CHR(9) || ']+)',
						'')));
				
			END IF;
			
			SELECT count(1) INTO iTabColExist
			FROM information_schema.columns 
			WHERE table_name=LOWER(ls_table_name)
			AND column_name=LOWER(ls_column_name);
				
			IF iTabColExist = 1 THEN
			
				SELECT count(1) INTO iColNullableNo
				FROM information_schema.columns 
				WHERE table_name=LOWER(ls_table_name)
				AND column_name=LOWER(ls_column_name)
				AND is_nullable = 'NO';
								
				IF iColNullableNo = 1 THEN
				
					iLoopColNullableNoCnt := iLoopColNullableNoCnt +1;
				
					IF iLoopColNullableNoCnt = 1 AND tLeftMatched = 'N' THEN
					
						tRightJoinLine := replace(tRightJoinLine,'RIGHT JOIN','INNER JOIN');
						tSQL := replace(tTablesSQL,tOrigRightJoinLine,tRightJoinLine);

					ELSE 
					
						tRightJoinLine := replace(tRightJoinLine,'RIGHT JOIN','INNER JOIN');
						tSQL := replace(tSQL,tOrigRightJoinLine,tRightJoinLine);

					END IF;
					
				END IF;

			ELSE
			
				RAISE EXCEPTION 'The value of the argument to confirm alias table name and column name exist in the data dictionary cannot be found from right join line '' % ''. Function does not handle string format.',tRightJoinLine;
		
			END IF;

		END IF;

	END LOOP;

END IF;

RETURN tSQL;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;