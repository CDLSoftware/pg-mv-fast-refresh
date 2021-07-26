/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvComplexFunctions.sql
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
05/11/2019  | M Revitt      | mv$clearPgMvLogTableBits is now a complex function so move it into the conplex script
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This is the build script for the complex database procedures that are required to support the Materialized View
                fast refresh process.

                This script contains procedures that rely on other database procedures having been previously created and must
                therefore be run last in the build process.

Notes:          Some of the procedures in this file rely on procedures that are created within this file and so whilst the procedures
                should be maintained in alphabetic order, this is not always possible.

                More importantly the order of procedures in this file should not be altered

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

-- -------------------- Write DROP-PROCEDURE-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP PROCEDURE IF EXISTS mv$clearAllPgMvLogTableBitsCompleteRefresh;
DROP PROCEDURE IF EXISTS mv$clearAllPgMvLogTableBits;
DROP PROCEDURE IF EXISTS mv$clearPgMvLogTableBits;
DROP PROCEDURE IF EXISTS mv$clearPgMviewLogBit;
DROP PROCEDURE IF EXISTS mv$createPgMv$Table;
DROP PROCEDURE IF EXISTS mv$insertMaterializedViewRows;
DROP PROCEDURE IF EXISTS mv$insertParallelMaterializedViewRows;
DROP PROCEDURE IF EXISTS mv$insertPgMview;
DROP PROCEDURE IF EXISTS mv$insertOuterJoinRows;
DROP PROCEDURE IF EXISTS mv$insertPgMviewOuterJoinDetails;
DROP FUNCTION IF EXISTS mv$checkParentToChildOuterJoinAlias;
DROP PROCEDURE IF EXISTS mv$executeMVFastRefresh;
DROP PROCEDURE IF EXISTS mv$refreshMaterializedViewFast;
DROP PROCEDURE IF EXISTS mv$refreshMaterializedViewFull;
DROP PROCEDURE IF EXISTS mv$setPgMviewLogBit;
DROP PROCEDURE IF EXISTS mv$updateMaterializedViewRows;
DROP PROCEDURE IF EXISTS mv$updateOuterJoinColumnsNull;
DROP FUNCTION IF EXISTS mv$regExpCount;
DROP FUNCTION IF EXISTS mv$regExpInstr;
DROP FUNCTION IF EXISTS mv$regExpReplace;
DROP FUNCTION IF EXISTS mv$regExpSubstr;
DROP FUNCTION IF EXISTS mv$outerJoinToInnerJoinReplacement;
DROP FUNCTION IF EXISTS mv$refreshMaterializedViewInitial;
DROP FUNCTION IF EXISTS mv$outerJoinDeleteStatement;

SET CLIENT_MIN_MESSAGES = NOTICE;


--------------------------------------------- Write CREATE-PROCEDURE-FUNCTION-stage scripts --------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$clearAllPgMvLogTableBitsCompleteRefresh
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearAllPgMvLogTableBitsCompleteRefresh
Author:       David Day
Date:         04/05/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/05/2021  | D Day         | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Performs a full refresh of the materialized view used for Complete Refresh Only, which consists of truncating the table 
				and then re-populating it.

                This activity also requires that every row in the materialized view log is updated to remove the interest from this
                materialized view, then as with the fast refresh once all the rows have been processed the materialized view log is
                cleaned up, in that all rows with a bitmap of zero are deleted as they are then no longer required.

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
         		IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

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

			CALL mv$clearPgMvLogTableBits
						(
							pConst,
							aViewLog.owner,
							aViewLog.pglog$_name,
							aPgMview.bit_array[i],
							pConst.MAX_BITMAP_SIZE
						);

			CALL mv$clearSpentPgMviewLogs( pConst, aViewLog.owner, aViewLog.pglog$_name );
			
			COMMIT;
			
		END IF;

    END LOOP;

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$clearAllPgMvLogTableBits
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
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
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
17/09/2019  | D Day         | Bug fix - Added logic to ignore table name if it already exists when clearing the bits in the mview logs
			|				| mview logs
04/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Performs a full refresh of the materialized view, which consists of truncating the table and then re-populating it.

                This activity also requires that every row in the materialized view log is updated to remove the interest from this
                materialized view, then as with the fast refresh once all the rows have been processed the materialized view log is
                cleaned up, in that all rows with a bitmap of zero are deleted as they are then no longer required.

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
         		IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

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

			CALL mv$clearPgMvLogTableBits
						(
							pConst,
							aViewLog.owner,
							aViewLog.pglog$_name,
							aPgMview.bit_array[i],
							pConst.MAX_BITMAP_SIZE
						);

			CALL mv$clearSpentPgMviewLogs( pConst, aViewLog.owner, aViewLog.pglog$_name );
			
		END IF;

    END LOOP;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$clearAllPgMvLogTableBits';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$clearPgMvLogTableBits
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pBit            IN      SMALLINT,
                pMaxSequence    IN      BIGINT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearPgMvLogTableBits
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
29/05/2020  | D Day			| Defect fix - Removed dblink action and call to function mv$clearPgMvLogTableBitsAction as this
			|				| was causing missing when an error occurred during the transaction process steps.
18/03/2020  | D Day         | Defect fix - To reduce the impact of deadlocks caused by multiple mview refreshes trying to update them
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

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$clearPgMvLogTableBits';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$clearPgMviewLogBit
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearPgMviewLogBit
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
30/11/2019  | M Revitt      | Use mv$bitValue to accomodate > 62 MV's per base Table
17/09/2019  | D Day         | Bug fix - Added logic to ignore table name if it already exists to stop pg$mview_logs table
			|				| pg_mview_bitmap column not being updated multiple times.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Determins which which bit has been assigned to the base table and then adds that to the PgMview bitmap in the
                materialized view log data dictionary table to record all of the materialized views that are using the rows created
                in this table.

Notes:          This is how we determine which materialized views require an update when the fast refresh procedure is called

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner             The owner of the materialized view source table
				IN      pViewName          The name of the materialized view source table

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

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

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$clearPgMviewLogBit';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$createPgMv$Table
            (
                pConst              IN      mv$allConstants,
                pOwner              IN      TEXT,
                pViewName           IN      TEXT,
                pViewColumns        IN      TEXT,
                pSelectColumns      IN      TEXT,
                pTableNames         IN      TEXT,
                pStorageClause      IN      TEXT,
				pParallel			IN		TEXT,
				pTableColumns		INOUT	TEXT		
            )
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
23/07/2021	| D Day			| Added logic to support running build in parallel.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
			|				| Added new INOUT parameter to replace the RETURN TEXT as this function was previously doing UPDATE and RETURN
			|				| which is not supported by procedure.
16/01/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Creates the base table upon which the Materialized View will be based from the provided SQL statment

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view base table
                IN      pViewColumns        Allow the view to be created with different names to the base table
                                            This list is positional so must match the position and number of columns in the
                                            select statment
                IN      pSelectColumns      The column list from the SQL query that will be used to create the view
                IN      pTableNames         The string between the FROM and WHERE clauses in the SQL query
                IN      pStorageClause      Optional, storage clause for the materialized view
				IN		pParallel			Optional, build in parallel
				INOUT	pTableColumns		The columns from the materialized view table returned as an INOUT paremeter
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tDefinedColumns TEXT    := NULL;
    tSqlStatement   TEXT    := NULL;
    tStorageClause  TEXT    := NULL;
	ret				TEXT;

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
						
	IF pParallel = 'Y' THEN
	
		PERFORM * FROM dblink('pgmv$_instance',tSqlStatement) AS p (ret TEXT);
	
	ELSE

		EXECUTE tSqlStatement;
		
	END IF;

    CALL mv$grantSelectPrivileges( pConst, pOwner, pViewName, pParallel );
	
	pTableColumns    :=  mv$getPgMviewViewColumns( pConst, pOwner, pViewName );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$createPgMv$Table';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$insertMaterializedViewRows
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT    DEFAULT NULL,
                pRowIDs         IN      UUID[]  DEFAULT NULL,
				pDmlType		IN		TEXT	DEFAULT NULL,
				pTabPkExist		IN      INTEGER DEFAULT 0
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertMaterializedViewRows
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
25/03/2021	| D Day			| Added workaround to fix primary key issue against mv_policy materialized view to ignore duplicates.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Gets called to insert a new row into the Materialized View when an insert is detected

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pTableAlias         The alias for the base table in the original select statement
                IN      pRowID              The unique identifier to locate the new row
				IN      pDmlType			The dml type
				IN      pTabPkExist			The table has a primary key that exists

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT;
    aPgMview        pg$mviews;
	tSqlSelectColumns TEXT;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );
	
	IF ( pViewName = 'mv_policy' AND pDmlType = 'INSERT' AND pTabPkExist = 1 ) THEN
		tSqlSelectColumns := pConst.OPEN_BRACKET   || pConst.SELECT_COMMAND || aPgMview.select_columns;
	ELSE
		tSqlSelectColumns := pConst.SELECT_COMMAND || aPgMview.select_columns;
	END IF;

    tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
                     pConst.OPEN_BRACKET   || aPgMview.pgmv_columns             || pConst.CLOSE_BRACKET ||
                     --pConst.SELECT_COMMAND || aPgMview.select_columns           ||
					 tSqlSelectColumns || pConst.FROM_COMMAND   || aPgMview.table_names;

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
		
		IF ( pViewName = 'mv_policy' AND pDmlType = 'INSERT' AND pTabPkExist = 1 ) THEN
			
			tSqlStatement := tSqlStatement || pConst.ON_CONFLICT_DO_NOTHING;
			
		END IF;
		
    END IF;

    EXECUTE tSqlStatement
    USING   pRowIDs;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$insertMaterializedViewRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$insertParallelMaterializedViewRows
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertParallelMaterializedViewRows
Author:       David Day
Date:         13/07/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
13/07/2021  | D Day     	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Gets called to insert a new row into the Materialized View when an insert is detected

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   		TEXT;
    aPgMview        		pg$mviews;
	tSqlSelectColumns 		TEXT;
	
	tCronJobSchedule		TEXT;
	
	tsCurrentDate			TIMESTAMP;
	tUsername				TEXT := 'biadmin';
	tDatabase				TEXT := 'strata';
	tJobName				TEXT;
	tTableName				TEXT;
	
	tsMaxTimestamp			TIMESTAMP;
	tsMinTimestamp			TIMESTAMP;
	
	insert_rec				RECORD;
	
	iCounter				INTEGER := 0;
	
	ret 					TEXT;
	iDblinkCount			INT;
	
	iStatusCount			INTEGER := 1;
	iJobCount				INTEGER := 0;
	iJobErrorCount			INTEGER := 0;	
	tsJobCreation			TIMESTAMP;
	
	tErrorCheckSql			TEXT;
	tCronSqlStatement		TEXT;
	tStatusCheckSql			TEXT;
	tMinMaxTimestampSql		TEXT;	
	tTimestampRangeSql		TEXT;
	tResult					TEXT;
	tDeleteSql				TEXT;

BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );
	
	-- set cron time
	tCronJobSchedule := mv$setCronSchedule();
	
	RAISE INFO '%', tCronJobSchedule;
	
	SELECT DISTINCT inline.table_name FROM
	(SELECT UNNEST(aPgMview.table_array) AS table_name
	 ,	    UNNEST(aPgMview.alias_array) AS table_alias ) inline INTO tTableName
	WHERE aPgMview.parallel_alias = REPLACE(inline.table_alias,'.','');
	
	tMinMaxTimestampSql := 'SELECT MIN('||aPgMview.parallel_column||'), MAX('||aPgMview.parallel_column||') FROM '||tTableName;
	
	-- set Min and Max Date with Timestamp
	EXECUTE tMinMaxTimestampSql INTO tsMinTimestamp, tsMaxTimestamp;
	
	tsJobCreation := clock_timestamp();
	
	FOR insert_rec IN 1..aPgMview.parallel_jobs LOOP
		
		iCounter := iCounter+1;
	
		tJobName := pViewName||'_job_'||iCounter;
		
		-- set date range sql for insert where clause based on max and min date split as per parallel jobs calculation
		tTimestampRangeSql := mv$setFromAndToTimestampRange(tsMinTimestamp::DATE,tsMaxTimestamp::DATE,aPgMview.parallel_jobs, iCounter, aPgMview.parallel_column, aPgMview.parallel_alias );	
		
		tSqlSelectColumns := pConst.SELECT_COMMAND || aPgMview.select_columns;

		tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
						 pConst.OPEN_BRACKET   || aPgMview.pgmv_columns             || pConst.CLOSE_BRACKET ||
						 tSqlSelectColumns || pConst.FROM_COMMAND   || aPgMview.table_names;

		IF aPgMview.where_clause != pConst.EMPTY_STRING
		THEN
			tSqlStatement := tSqlStatement || pConst.WHERE_COMMAND || aPgMview.where_clause || pConst.AND_COMMAND || tTimestampRangeSql;
		ELSE
			tSqlStatement := tSqlStatement || pConst.WHERE_COMMAND || aPgMview.where_clause || pConst.SPACE_CHARACTER || tTimestampRangeSql;
		END IF;
		
		tSqlStatement := REPLACE(tSqlStatement,'''','''''');
		
		tCronSqlStatement := 'INSERT INTO cron.job(schedule, command, database, username, jobname)
						  VALUES ('''||tCronJobSchedule||''','''||tSqlStatement||''','''||tDatabase||''','''||tUsername||''','''||tJobName||''')';
						  
		--RAISE INFO '%', tCronSqlStatement;
		
		PERFORM * FROM dblink('pgmv$cron_instance',tCronSqlStatement) AS p (ret TEXT);
		
	END LOOP;
	
	-- Checks to confirm jobs have been created and successfully ran
	WHILE iJobCount < aPgMview.parallel_jobs LOOP

		tStatusCheckSql := 'SELECT count(1) FROM cron.job_run_details jrd
							JOIN cron.job j ON j.jobid = jrd.jobid
							WHERE j.jobname LIKE '''||pViewName||'_job_%''
							AND jrd.start_time >= '''||tsJobCreation||'''';
										
		SELECT * FROM
		dblink('pgmv$cron_instance', tStatusCheckSql) AS p (iDblinkCount INT) INTO iJobCount;
		
		IF iJobCount < aPgMview.parallel_jobs THEN		
			SELECT pg_sleep(60) INTO tResult;			
		END IF;
		
	END LOOP;

	WHILE iStatusCount > 0 LOOP
	 
		tStatusCheckSql := 'SELECT count(1) FROM cron.job_run_details jrd
							JOIN cron.job j ON j.jobid = jrd.jobid
							WHERE j.jobname LIKE '''||pViewName||'_job_%''
							AND jrd.status = ''running''';
							
		SELECT * FROM
		dblink('pgmv$cron_instance', tStatusCheckSql) AS p (iDblinkCount INT) INTO iStatusCount;
		
		tErrorCheckSql := 'SELECT count(1) FROM cron.job_run_details jrd
							JOIN cron.job j ON j.jobid = jrd.jobid
							WHERE j.jobname LIKE '''||pViewName||'_job_%''
							AND jrd.start_time >= '''||tsJobCreation||'''
							AND jrd.status = ''failed''';
							
		SELECT * FROM
		dblink('pgmv$cron_instance', tStatusCheckSql) AS p (iDblinkCount INT) INTO iJobErrorCount;
		
		IF iJobErrorCount > 0 THEN
		
			IF EXISTS (
			  SELECT
			  FROM   pg_tables
			  WHERE  tablename = pViewName) THEN
		
				tDeleteSql := 'DROP TABLE '||pViewName;
							   
				PERFORM * FROM dblink('pgmv$_instance',tDeleteSql) AS p (ret TEXT);
				
			END IF;
			
			RAISE INFO      'Exception in procedure mv$insertParallelMaterializedViewRows';
			RAISE EXCEPTION 'Error: Cron job(s) for % found in status of failed - please check table cron.job_run_details for full details', pViewName;
		END IF;
		
		IF iStatusCount > 0 THEN
		
			SELECT pg_sleep(120) INTO tResult;
			
		END IF;
		
		IF iStatusCount = 0 THEN
		
			tDeleteSql := 'DELETE FROM cron.job j 
						   WHERE j.jobname LIKE '''||pViewName||'_job_%''';
							   
			PERFORM * FROM dblink('pgmv$cron_instance',tDeleteSql) AS p (ret TEXT);
							   
		END IF;
		
	END LOOP;	

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$insertParallelMaterializedViewRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$insertPgMview
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
				pInnerJoinTableNameArray		IN		TEXT[],
				pInnerJoinTableAliasArray		IN		TEXT[],
				pInnerJoinTableRowidArray		IN		TEXT[],				
				pInnerJoinOtherTableNameArray	IN		TEXT[],		
				pInnerJoinOtherTableAliasArray	IN		TEXT[],
				pInnerJoinOtherTableRowidArray	IN		TEXT[],
				pQueryJoinsMultiTabCntArray 	IN 		SMALLINT[],
				pQueryJoinsMultiTabPosArray 	IN 		SMALLINT[],
				pParallel			IN      TEXT,
				pParallelJobs		IN		INTEGER,
				pParallelColumn		IN		TEXT,
				pParallelAlias		IN		TEXT,
                pFastRefresh        IN      BOOLEAN
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertPgMview
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
21/10/2020	| D Day			| Bug fix - Added new columns to support applying DML changes when same table exists in materialized view
			|				| stored query more than once.
29/06/2020	| D Day			| Defect fix - Added new columns to INSERT statement INTO pg$mviews table to support DML type change INSERTS for
							| the DELETE part as this was not removing rows for certain scenarios.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
17/09/2019  | D Day         | Bug fix - Added logic to ignore log table name if it already exists as this was causing the bit value being set incorrectly
			|				| in the data dictionary table bit_array column in pg$mviews table.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Every time a new materialized view is created, a record of that view is also created in the data dictionary table
                pg$mviews.

                This table holds all of the pertinent information about the materialized view which is later used in the management
                of that view.

Arguments:      
				IN      pConst              The memory structure containing all constants 
				IN      pOwner              The owner of the object
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
				IN		pInnerJoinTableNameArray		An array that holds the list of INNER JOIN tables
				IN		pInnerJoinTableAliasArray		An array that holds the list of INNER JOIN aliases
				IN		pInnerJoinTableRowidArray		An array that holds the list of INNER JOIN rowid column names
				IN		pInnerJoinOtherTableNameArray	An array that holds the list of INNER JOIN other joining tables
				IN		pInnerJoinOtherTableAliasArray	An array that holds the list of INNER JOIN other joining aliases
				IN		pInnerJoinOtherTableRowidArray	An array that holds the list of INNER JOIN other joining rowid column names
				IN		pQueryJoinsMultiTabCntArray 	An array that holds the materialized view stored query joins multi table count per table
				IN		pQueryJoinsMultiTabPosArray 	An array that holds the materialized view stored query joins multi table position

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

				CALL mv$setPgMviewLogBit
            	                        (
           	                             pConst,
            	                            aPgMviewLogData.owner,
           	                             	aPgMviewLogData.pglog$_name,
            	                            aPgMviewLogData.pg_mview_bitmap,
											iBit
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
            inner_rowid_array,
			inner_join_table_array,
			inner_join_alias_array,
			inner_join_rowid_array,
			inner_join_other_table_array,
			inner_join_other_alias_array,
			inner_join_other_rowid_array,
			query_joins_multi_table_cnt_array,
			query_joins_multi_table_pos_array,
			parallel,
			parallel_jobs,
			parallel_column,
			parallel_alias
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
            pInnerRowidArray,
			pInnerJoinTableNameArray,
			pInnerJoinTableAliasArray,
			pInnerJoinTableRowidArray,			
			pInnerJoinOtherTableNameArray,		
			pInnerJoinOtherTableAliasArray,
			pInnerJoinOtherTableRowidArray,
			pQueryJoinsMultiTabCntArray,
			pQueryJoinsMultiTabPosArray,
			pParallel,
			pParallelJobs,
			pParallelColumn,
			pParallelAlias
    );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$insertPgMview';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$executeMVFastRefresh
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
                pRowIDArray     IN      UUID[],
				pTabPkExist		IN      INTEGER
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$executeMVFastRefresh
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
30/03/2021  | D Day			| Added new parameter variables for insert process to handle primary key duplicates on mv_policy when
			|				| calling procedures mv$updateMaterializedViewRows and mv$insertMaterializedViewRows.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
01/07/2019	| David Day		| Added function mv$updateOuterJoinColumnsNull to handle outer join deletes.            |               |
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Selects all of the data from the materialized view log, in the order it was created, and applies the changes to
                the materialized view table and once the change has been applied the bit value for the materialized view is
                removed from the PgMview log row.

                Once all rows have been processed the materialized view log is cleaned up, in that all rows with a bitmap of zero
                are deleted as they are then no longer required

Arguments:      IN      pConst              The memory structure containing all constants
				IN		pDmlType
                IN		pOwner				The owner of the object
                IN		pViewName       	The name of the materialized view
                IN		pRowidColumn 
                IN		pTableAlias
                IN		pOuterTable
                IN		pInnerAlias
                IN		pInnerRowid
                IN		pRowIDArray
				IN      pTabPkExist

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

BEGIN

    CASE pDmlType
    WHEN pConst.DELETE_DML_TYPE
    THEN
	    IF TRUE = pOuterTable
        THEN	
		
			CALL  mv$updateOuterJoinColumnsNull
							(
								pConst,
								pOwner,
								pViewName,
								pTableAlias,
								pRowidColumn,
								pRowIDArray
							);
		
		ELSE
			CALL mv$deleteMaterializedViewRows( pConst, pOwner, pViewName, pConst.DELETE_DML_TYPE, pRowidColumn, pRowIDArray );
		END IF;
			
    WHEN pConst.INSERT_DML_TYPE
    THEN
        IF TRUE = pOuterTable
        THEN
            CALL  mv$insertOuterJoinRows
                        (
                            pConst,
                            pOwner,
                            pViewName,
                            pTableAlias,
                            pInnerAlias,
                            pInnerRowid,
                            pRowIDArray,
							pTabPkExist
                        );
        ELSE
            CALL mv$deleteMaterializedViewRows( pConst, pOwner, pViewName, pConst.INSERT_DML_TYPE, pRowidColumn, pRowIDArray );
            CALL mv$insertMaterializedViewRows( pConst, pOwner, pViewName, pTableAlias,  pRowIDArray, pConst.INSERT_DML_TYPE, pTabPkExist );
        END IF;

    WHEN pConst.UPDATE_DML_TYPE
    THEN
        CALL mv$deleteMaterializedViewRows( pConst, pOwner, pViewName, pConst.UPDATE_DML_TYPE, pRowidColumn, pRowIDArray );
        CALL mv$updateMaterializedViewRows( pConst, pOwner, pViewName, pTableAlias,  pRowIDArray, pTabPkExist);
    ELSE
        RAISE EXCEPTION 'DML Type % is unknown', pDmlType;
    END CASE;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$executeMVFastRefresh';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$refreshMaterializedViewFast
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
                pInnerRowid     IN      TEXT,
				pQueryJoinsMultiTabCnt	IN SMALLINT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFast
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
20/05/2020  | D Day			| Bug fix - to ignore DML changes to prtyinst, currprty, personxx for materialized views that only
			|				| use these table joins to get forename and surname. If the main joining table changes for the linking id
			|				| this will update the row i.e. createdby or updatedby columns for the forename and surname.
19/11/2020	| D Day			| CDL specific change - temp workaround performance improvement to ignore DML changes to prtyinst, currprty, personxx
			|				| on MV_DIARY and MV_APPLICATION_EVENTS. The way new client party instances get created even though the forename and surname
			|				| for Strata users never or rarely changes it still causes the source table row to change and this causes the mview log table
			|				| to record a DML change. This can happen often and cause large scale DELETE and INSERT combinations to apply to them
			|				| corresponding mview table. Pernament fix being reviewed.
22/10/2020	| D Day			| Defect fix - Added logic to loop around all join table aliases to allow the DML changes selected to be applied 
			|				| from the same materialized view log before clearing the bitmap value for all selected rows. Previously the bitmap
			|				| value selected was being cleared on the first occurrence even though the rowid could have been linked to them
			|				| second occurrence of that table alias within the materialized view stored query.
21/07/2020	|				| Defect fix to clear rowid array uRowIDArray when DML Type last type value has changed as this was
			|				| causing rowids from previous DML Types to not be cleared correctly causing incorrect routines to be actioned.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Selects all of the data from the materialized view log, in the order it was created, and applies the changes to
                the materialized view table and once the change has been applied the bit value for the materialized view is
                removed from the PgMview log row.
				
				This is used as part of the initial materialized view creation were all details are loaded into table
				pg$mviews_oj_details which is later used by the refresh procesa.

Arguments:      IN      pConst              The memory structure containing all constants          
                IN		pOwner          
                IN		pViewName     
                IN		pTableAlias     
                IN		pTableName
                IN		pRowidColumn
                IN		pPgMviewBit
                IN		pOuterTable
                IN		pInnerAlias
                IN		pInnerRowid
				IN      pQueryJoinsMultiTabCnt
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    tDmlType        TEXT        := NULL;
    tLastType       TEXT        := NULL;
    tSqlStatement   TEXT        := NULL;
    iArraySeq       INTEGER     := pConst.ARRAY_LOWER_VALUE;
    biSequence      BIGINT      := 0;
    biMaxSequence   BIGINT      := 0;
    uRowID          UUID;
    uRowIDArray     UUID[];

    aViewLog        pg$mview_logs;
    tBitValue       mv$bitValue;
	
	aMultiTablePgMview		pg$mviews;
	bOuterJoined			BOOLEAN;
	
	iTabPkExist				INTEGER := 0;

BEGIN

    aViewLog    := mv$getPgMviewLogTableData( pConst, pTableName );
    tBitValue   := mv$getBitValue( pConst, pPgMviewBit );
	
	IF pQueryJoinsMultiTabCnt > 1 THEN
		aMultiTablePgMview   := mv$getPgMviewTableData( pConst, pOwner, pViewName );
	END IF;
	
	IF pViewName = 'mv_policy' THEN	
		SELECT count(1) INTO iTabPkExist
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
							 AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = pViewName::regclass
		AND    i.indisprimary;	
	END IF;

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
		
			IF pQueryJoinsMultiTabCnt > 1 THEN
			
				-- Ignore DML changes for forename and surname columns unless the joining createdby or updatedby ID changes on the joining table. This is required ro reduce performance impact
				-- when these tables get updated with no changes to forename and surname.
				IF (pViewName = 'mv_insurer_details' OR pTableName NOT IN ('prtyinst','personxx','currprty')) THEN

					FOR i IN ARRAY_LOWER( aMultiTablePgMview.table_array, 1 ) .. ARRAY_UPPER( aMultiTablePgMview.table_array, 1 ) LOOP

						IF aMultiTablePgMview.table_array[i] = pTableName THEN
						
							bOuterJoined := mv$checkIfOuterJoinedTable( pConst, aMultiTablePgMview.table_array[i], aMultiTablePgMview.outer_table_array[i] );
										
										CALL mv$executeMVFastRefresh
										(
											pConst,
											tLastType,
											pOwner,
											pViewName,
											aMultiTablePgMview.rowid_array[i],
											aMultiTablePgMview.alias_array[i],
											bOuterJoined,
											aMultiTablePgMview.inner_alias_array[i],
											aMultiTablePgMview.inner_rowid_array[i],
											uRowIDArray,
											iTabPkExist
										);
				
						END IF;
						
					END LOOP;
					
				END IF;
								
			ELSE
					
				CALL mv$executeMVFastRefresh
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
								uRowIDArray,
								iTabPkExist
							);
							
			END IF;

            tLastType               := tDmlType;
            iArraySeq               := 1;
			uRowIDArray 			:= '{}';
			uRowIDArray[iArraySeq]  := uRowID;

        END IF;
		
    END LOOP;

    IF biMaxSequence > 0
    THEN

		IF pQueryJoinsMultiTabCnt > 1 THEN
		
			-- Ignore DML changes for forename and surname columns unless the joining createdby or updatedby ID changes on the joining table. This is required ro reduce performance impact
			-- when these tables get updated with no changes to forename and surname.
			IF (pViewName = 'mv_insurer_details' OR pTableName NOT IN ('prtyinst','personxx','currprty')) THEN
		
				aMultiTablePgMview   := mv$getPgMviewTableData( pConst, pOwner, pViewName );

				FOR i IN ARRAY_LOWER( aMultiTablePgMview.table_array, 1 ) .. ARRAY_UPPER( aMultiTablePgMview.table_array, 1 ) LOOP
					
					IF aMultiTablePgMview.table_array[i] = pTableName THEN
										
						bOuterJoined := mv$checkIfOuterJoinedTable( pConst, aMultiTablePgMview.table_array[i], aMultiTablePgMview.outer_table_array[i] );
									
									CALL  mv$executeMVFastRefresh
									(
										pConst,
										tLastType,
										pOwner,
										pViewName,
										aMultiTablePgMview.rowid_array[i],
										aMultiTablePgMview.alias_array[i],
										bOuterJoined,
										aMultiTablePgMview.inner_alias_array[i],
										aMultiTablePgMview.inner_rowid_array[i],
										uRowIDArray,
										iTabPkExist
									);
								
					END IF;
					
				END LOOP;
				
			END IF;
			
		ELSE
	
			CALL mv$executeMVFastRefresh
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
							uRowIDArray,
							iTabPkExist
						);
						
		END IF;

        CALL mv$clearPgMvLogTableBits
                    (
                        pConst,
                        aViewLog.owner,
                        aViewLog.pglog$_name,
                        pPgMviewBit,
                        biMaxSequence
                    );

        CALL mv$clearSpentPgMviewLogs( pConst, aViewLog.owner, aViewLog.pglog$_name );
		
		uRowIDArray := '{}';
		
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$refreshMaterializedViewFast';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$refreshMaterializedViewFull
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
				pParallel	IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFull
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
15/07/2021	| D Day			| Added function mv$insertParallelMaterializedViewRows and parameter pParallel to allow complete
			|				| refresh of materialized view table in Parallel.
04/05/2021  | D Day			| Replaced function mv$clearAllPgMvLogTableBits with mv$clearAllPgMvLogTableBitsCompleteRefresh to handle
			|				| commits during the complete refresh. Removed exception handler to support commits.
18/08/2020	| J Bills		| Added process to drop and re-index MVs to prevent the refresh from hanging. Also changed the order of 
			|				| clearing the logs to prevent missing data when base table replication process is running.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Performs a full refresh of the materialized view, which consists of truncating the table and then re-populating it.

                This activity also requires that every row in the materialized view log is updated to remove the interest from this
                materialized view, then as with the fast refresh once all the rows have been processed the materialized view log is
                cleaned up, in that all rows with a bitmap of zero are deleted as they are then no longer required.

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
				IN		pParallel			Optional, build in parallel	

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMview    pg$mviews;

BEGIN

    aPgMview    := mv$getPgMviewTableData(        pConst, pOwner, pViewName );
    CALL mv$createindexestemptable(pOwner, pViewName);
	CALL mv$dropmvindexes(pOwner, pViewName);
	CALL mv$truncateMaterializedView(   pConst, pOwner, aPgMview.view_name, pParallel );
    CALL mv$clearAllPgMvLogTableBitsCompleteRefresh(   pConst, pOwner, pViewName );
	IF pParallel = 'Y' THEN
		CALL mv$insertParallelMaterializedViewRows( pConst, pOwner, pViewName );
	ELSE
		CALL mv$insertMaterializedViewRows( pConst, pOwner, pViewName );
	END IF; 
    CALL mv$readdmvindexes (pViewName);
	CALL mv$dropindexestemptable (pViewName);

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$refreshMaterializedViewFast
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewFast
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
22/10/2020  | D Day			| Defect fix - Added logic to handle processing table_array DML changes that link to the same materialized view
			|				| log. The refresh will ONLY be executed when the query joins multi table position is equal to 1 - otherwise it
			|				| will ignore as this is now handled in the calling procedure.
08/06/2020	| D Day			| Added sub begin and end block to capture EXCEPTION handler as this is not support in procedures using a COMMIT.
			|				| By adding to an independant block enables exception handling to be coded. Anything outside of this block will
			|				| be handled as the default Postgres error handler. 
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
03/03/2020  | D Day         | Defect fix to resolve outer join check function mv$checkIfOuterJoinedTable to handle if the table_array
			|				| value had both an inner join and outer join condition inside the main sql query. Amended to only pass in
			|				| in the outer_table_array loop value not the full array.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Determins what type of refresh is required and then calls the appropriate refresh procedure

Notes:          This procedure must come after the creation of the 2 procedures
                it calls
                o   mv$refreshMaterializedViewFast( pOwner, pViewName );
                o   mv$refreshMaterializedViewFull( pOwner, pViewName );

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMview        pg$mviews;
    bOuterJoined    BOOLEAN;

BEGIN
    aPgMview   := mv$getPgMviewTableData( pConst, pOwner, pViewName );

    FOR i IN ARRAY_LOWER( aPgMview.table_array, 1 ) .. ARRAY_UPPER( aPgMview.table_array, 1 )
    LOOP
	
		BEGIN
		
			IF aPgMview.query_joins_multi_table_pos_array[i] = 1 THEN
		
				bOuterJoined := mv$checkIfOuterJoinedTable( pConst, aPgMview.table_array[i], aPgMview.outer_table_array[i] );
				CALL mv$refreshMaterializedViewFast
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
							aPgMview.inner_rowid_array[i],
							aPgMview.query_joins_multi_table_cnt_array[i]
						);
						
			END IF;
					
		EXCEPTION
		WHEN OTHERS
		THEN
			RAISE INFO      'Exception in procedure mv$refreshMaterializedViewFast';
			RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
			RAISE EXCEPTION '%',                SQLSTATE;
			
		END;
					
		COMMIT;
		
    END LOOP;

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$insertOuterJoinRows
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pInnerAlias     IN      TEXT,
                pInnerRowid     IN      TEXT,
                pRowIDs         IN      UUID[],
				pTabPkExist		IN      INTEGER DEFAULT 0
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertOuterJoinRows
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
25/03/2021	| D Day			| Added workaround to fix primary key issue against mv_policy materialized view to ignore
10/03/2021	| D Day         | Added new delete and insert statements for outer join alias performance improvements
18/08/2020	| D Day			| Removed outer to inner join logic as this is under further review.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
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

Arguments:      IN      pConst              The memory structure containing all constants
                IN		pOwner
                IN		pViewName
                IN		pTableAlias
                IN		pInnerAlias
                IN		pInnerRowid
                IN		pRowIDs
				IN		pTabPkExist

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tDeleteFromClause   TEXT;
    tInsertFromClause   TEXT;
    tSqlStatement   	TEXT;
    aPgMview        	pg$mviews;
	aPgMviewOjDetails	pg$mviews_oj_details;
	tFromClause			TEXT;
	tSqlSelectColumns   TEXT;

BEGIN

    aPgMview    		 := mv$getPgMviewTableData( pConst, pOwner, pViewName );
    aPgMviewOjDetails    := mv$getPgMviewOjDetailsTableData( pConst, pOwner, pViewName, pTableAlias);
	
	--tFromClause		:= pConst.FROM_COMMAND  || aPgMview.table_names    || pConst.WHERE_COMMAND;
	--tDeleteFromClause := pConst.FROM_COMMAND  || aPgMview.table_names    || pConst.WHERE_COMMAND;
	tInsertFromClause := pConst.FROM_COMMAND  || aPgMviewOjDetails.join_replacement_from_sql    || pConst.WHERE_COMMAND;

    IF LENGTH( aPgMview.where_clause ) > 0
    THEN
		--tFromClause := tFromClause      || aPgMview.where_clause    || pConst.AND_COMMAND;
        --tDeleteFromClause := tDeleteFromClause      || aPgMview.where_clause    || pConst.AND_COMMAND;
		tInsertFromClause := tInsertFromClause      || aPgMview.where_clause    || pConst.AND_COMMAND;
    END IF;
	
    --tFromClause := tFromClause  || pTableAlias   || pConst.MV_M_ROW$_SOURCE_COLUMN   || pConst.IN_ROWID_LIST;
    --tDeleteFromClause := tDeleteFromClause  || pTableAlias   || pConst.MV_M_ROW$_SOURCE_COLUMN   || pConst.IN_ROWID_LIST;
    tInsertFromClause := tInsertFromClause  || pTableAlias   || pConst.MV_M_ROW$_SOURCE_COLUMN   || pConst.IN_ROWID_LIST;

	tSqlStatement	  :=  aPgMviewOjDetails.delete_sql;

    --tSqlStatement   :=  pConst.DELETE_FROM       		||
    --                    aPgMview.owner           		|| pConst.DOT_CHARACTER    || aPgMview.view_name			||
    --                    pConst.WHERE_COMMAND     		|| pInnerRowid             ||
    --                    pConst.IN_SELECT_COMMAND 		|| pInnerAlias             || pConst.DOT_CHARACTER    		|| 
	--					pConst.MV_M_ROW$_SOURCE_COLUMN	|| tFromClause       || pConst.CLOSE_BRACKET;

    EXECUTE tSqlStatement
    USING   pRowIDs;
	
	IF ( pViewName = 'mv_policy' AND pTabPkExist = 1 ) THEN
		tSqlSelectColumns := pConst.OPEN_BRACKET   || pConst.SELECT_COMMAND || aPgMview.select_columns;
	ELSE
		tSqlSelectColumns := pConst.SELECT_COMMAND || aPgMview.select_columns;
	END IF;
	
    tSqlStatement :=    pConst.INSERT_INTO       ||
                        aPgMview.owner           || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
                        pConst.OPEN_BRACKET      || aPgMview.pgmv_columns   || pConst.CLOSE_BRACKET ||
                        tSqlSelectColumns 		 || tInsertFromClause;
						
	IF ( pViewName = 'mv_policy' AND pTabPkExist = 1 )  THEN		
		tSqlStatement := tSqlStatement || pConst.ON_CONFLICT_DO_NOTHING;
	END IF;

    EXECUTE tSqlStatement
    USING   pRowIDs;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$insertOuterJoinRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;

------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$insertPgMviewOuterJoinDetails
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
	            pRightOuterJoinArray  IN      TEXT[],
				pWhereClause		  IN	  TEXT,
				pTableArray			  IN 	  TEXT[]
			 )
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
10/03/2021	| D Day			| Added new function mv$outerJoinDeleteStatement to dynamically create delete sql to store in data dictionary 
			|				| to be used during the refresh process.
23/10/2020  | D Day			| Defect fix - added REGEXP_REPLACE to remove any spaces or tabs at the end of the line for variable 
			|				| tColumnNameSql. This was causing columns to be not added to the UPDATE statement. Added logic to 
			|				| ignore rAliasJoinLinks.alias array null values and check for characters before alias matches in select column
			|				| string.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
28/04/2020	| D Day			| Added mv$OuterJoinToInnerJoinReplacement function call to replace alias matching outer join conditions
			|				| with inner join conditions and new IN parameter pTableNames.
01/07/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Dynamically builds UPDATE statement(s) for any outer join table to nullify all the alias outer join column(s)
				including rowid held in the materialized view table when an DELETE is done against the 
				source table. This logic support outer join table parent to child join relationships so that all child table columns
				and linking rowids are included in the UPDATE statement.
				
				This procedure inserts data into the data dictionary table pgmview_oj_details 

Arguments:      IN      pConst              	The memory structure containing all constants
                IN      pOwner                  The owner of the object
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
				IN		pWhereClause
				IN		pTableArray

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
	tOuterJoinDeleteStatement		TEXT;	
	
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
		
			IF rAliasJoinLinks.alias IS NOT NULL THEN
		
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
								 '([^A-Za-z0-9]+('||tRegExpColumnNameAlias||'))',
								 1,
								 i)-1);
						tColumnNameSql := mv$regExpReplace(tColumnNameSql,'(^[[:space:]]+)',null);
						tColumnNameSql := mv$regExpSubstr((tColumnNameSql),'(.*'||tRegExpColumnNameAlias||'+[[:alnum:]]+(.*?[^,|$]))',1,1,'i');
						tColumnNameSql := mv$regExpReplace(tColumnNameSql,'\s+$','');
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
			
			END IF;
		
		END LOOP;
		
		tUpdateSetSql := SUBSTRING(tUpdateSetSql,1,length(tUpdateSetSql)-1);
		
		tSqlStatement := pConst.UPDATE_COMMAND ||
						 pOwner		|| pConst.DOT_CHARACTER		|| pViewName	|| pConst.NEW_LINE		||
						 tUpdateSetSql || pConst.NEW_LINE ||
						 tWhereClause;
						 
						 
						 
		tClauseJoinReplacement := mv$OuterJoinToInnerJoinReplacement(pConst, pTableNames, tColumnNameAlias);	
		tOuterJoinDeleteStatement := mv$outerJoinDeleteStatement(pConst, pTableNames, tColumnNameAlias, pViewName, pWhereClause, tTableName, pTableArray, pAliasArray);
			
		INSERT INTO pg$mviews_oj_details
		(	owner
		,	view_name
		,	table_alias
		,   rowid_column_name
		,   source_table_name
		,   column_name_array
		,   update_sql
		,   join_replacement_from_sql
		,   delete_sql)
		VALUES
		(	pOwner
		,	pViewName
		,   tColumnNameAlias
		,   tMvRowidColumnName
		,   tTableName
		,   tColumnNameArray
		,	tSqlStatement
		,   tClauseJoinReplacement
		,	tOuterJoinDeleteStatement);
		
		iMainLoopCounter := 0;
		tParentToChildAliasArray := '{}';
		tAliasArray  := '{}';
		tMainAliasArray := '{}';
		iWhileCounter := 0;
		iWhileLoopCounter := 0;
		iLoopCounter := 0;
		
		
	END LOOP;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$insertPgMviewOuterJoinDetails';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
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
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE
PROCEDURE    mv$updateOuterJoinColumnsNull
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                ptablealias     IN      TEXT,
                pRowidColumn    IN      TEXT,
                pRowIDs         IN      UUID[]
            )
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
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
28/04/2020	| D Day			| Added mv$OuterJoinToInnerJoinReplacement function call to replace alias matching outer join conditions
			|				| with inner join conditions and new IN parameter pTableNames.
25/06/2019  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Executes UPDATE statement to nullify outer join columns held in the materialized view table when a DELETE has been
				done against the source table.
				
				A decision was made that an UPDATE would be the more per-formant way of deleting the data rather than to get
				the inner join rowids to allow the rows to be deleted and inserted back if the inner join conditions still match.
				
				Due to the overhead of getting the inner join rowids from the materialized view to allow this to happen in this scenario.

Arguments:      IN      pConst	
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pTableAlias         The alias for the outer join table
                IN      pRowidColumn    	The name of the outer join rowid column
                IN      pRowID              The unique identifier to locate the row			

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

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$updateOuterJoinColumnsNull';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$setPgMviewLogBit
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pBitmap         IN      BIGINT[],
				pBitValue		INOUT	SMALLINT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setPgMviewLogBit
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
08/10/2019  | D Day         | Changed returns type from INTEGER to SMALLINT to match the bit data type.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Determins which which bit has been assigned to the base table and then adds that to the PgMview bitmap in the
                materialized view log data dictionary table to record all of the materialized views that are using the rows created
                in this table.

Notes:          This is how we determine which materialized views require an update when the fast refresh procedure is called

Arguments:      IN      pTableName          The name of the materialized view source table

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
	
	pBitValue := tBitValue.BIT_VALUE;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$setPgMviewLogBit';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$updateMaterializedViewRows
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
                pTableAlias     IN      TEXT,
                pRowIDs         IN      UUID[],
				pTabPkExist		IN      INTEGER DEFAULT 0
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$updateMaterializedViewRows
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
30/03/2021	| D Day			| Added workaround to fix primary key issue against mv_policy materialized view to ignore duplicates.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Gets called to insert a new row into the Materialized View when an insert is detected

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
                IN      pTableAlias         The alias for the base table in the original select statement
                IN      pRowID              The unique identifier to locate the new row
				IN      pTabPkExist

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   	TEXT;
    aPgMview        	pg$mviews;
    bBaseRowExists  	BOOLEAN := FALSE;
	tSqlSelectColumns 	TEXT;


BEGIN

    aPgMview := mv$getPgMviewTableData( pConst, pOwner, pViewName );
	
	IF ( pViewName = 'mv_policy' AND pTabPkExist = 1 ) THEN
		tSqlSelectColumns := pConst.OPEN_BRACKET   || pConst.SELECT_COMMAND || aPgMview.select_columns;
	ELSE
		tSqlSelectColumns := pConst.SELECT_COMMAND || aPgMview.select_columns;
	END IF;

    tSqlStatement := pConst.INSERT_INTO    || pOwner || pConst.DOT_CHARACTER    || aPgMview.view_name   ||
                     pConst.OPEN_BRACKET   || aPgMview.pgmv_columns             || pConst.CLOSE_BRACKET ||
                     --pConst.SELECT_COMMAND || aPgMview.select_columns           ||
					 tSqlSelectColumns || pConst.FROM_COMMAND   || aPgMview.table_names ||
                     pConst.WHERE_COMMAND;

    IF aPgMview.where_clause != pConst.EMPTY_STRING
    THEN
        tSqlStatement := tSqlStatement || aPgMview.where_clause || pConst.AND_COMMAND;
    END IF;

    tSqlStatement :=  tSqlStatement || pTableAlias  || pConst.MV_M_ROW$_SOURCE_COLUMN || pConst.IN_ROWID_LIST;
	
	IF ( pViewName = 'mv_policy' AND pTabPkExist = 1 ) THEN
		
		tSqlStatement := tSqlStatement || pConst.ON_CONFLICT_DO_NOTHING;
		
	END IF;	

    EXECUTE tSqlStatement
    USING   pRowIDs;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$updateMaterializedViewRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
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
LANGUAGE    plpgsql;
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
LANGUAGE    plpgsql;
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
LANGUAGE    plpgsql;
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
LANGUAGE    plpgsql;
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
01/03/2021	| D Day			| Added some additional replace logic to handle double spaces found in ansi join types LEFT JOIN,
			|				| RIGHT JOIN, etc.
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
tTablesSQL := regexp_replace(tTablesSQL,'left  join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left  outer join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left  outer  join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left outer  join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right  join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right  outer join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right outer  join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right  outer  join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'RIGHT  JOIN','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'LEFT  JOIN','LEFT JOIN','gi');
	  
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
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$refreshMaterializedViewInitial
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
				pParallel	IN		TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedViewInitial
Author:       Jack Bills
Date:         19/08/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
15/07/2021	| D Day			| Added new parallel option to allow materialized views to be built in parallel INSERT sessions.
19/08/2020  | J Bills       | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Performs a full refresh of the materialized view, which consists of truncating the table and then re-populating it.

                This activity also requires that every row in the materialized view log is updated to remove the interest from this
                materialized view, then as with the fast refresh once all the rows have been processed the materialized view log is
                cleaned up, in that all rows with a bitmap of zero are deleted as they are then no longer required.

Note:           This procedure requires the SEARCH_PATH to be set to the current value so that the select statement can find the
                source tables.
                The default for PostGres procedures is to not use the search path when executing with the privileges of the creator.
				This is a revised version of the orignial will refresh to be used when the materialized view is being created.

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
				IN      pParallel			The materialized view set Y/N depending if it has been selected to run in parallel 
											for the INSERT process

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMview    pg$mviews;

BEGIN

    aPgMview    := mv$getPgMviewTableData(        pConst, pOwner, pViewName );
    CALL mv$truncateMaterializedView(   pConst, pOwner, aPgMview.view_name, pParallel );
	
	IF aPgMview.parallel = 'Y' THEN
		CALL mv$insertParallelMaterializedViewRows( pConst, pOwner, pViewName );
	ELSE
		CALL mv$insertMaterializedViewRows( pConst, pOwner, pViewName );
	END IF;
				
    CALL mv$clearAllPgMvLogTableBits(   pConst, pOwner, pViewName );

    EXCEPTION
    WHEN OTHERS
    THEN
		IF aPgMview.parallel = 'Y' THEN
		
			IF EXISTS (
			  SELECT
			  FROM   pg_tables
			  WHERE  tablename = pViewName) THEN
		
				tDeleteSql := 'DROP TABLE '||pViewName;
							   
				PERFORM * FROM dblink('pgmv$_instance',tDeleteSql) AS p (ret TEXT);
				
			END IF;

		END IF;
		
        RAISE INFO      'Exception in procedure mv$refreshMaterializedViewInitial';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;

-----------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE 
FUNCTION mv$outerJoinDeleteStatement(
	pConst          IN      mv$allConstants,
	pTableNames 	IN		TEXT,
	pTableAlias 	IN		TEXT,
	pViewName		IN      TEXT,
	pWhereClause	IN		TEXT,
	pTableName		IN		TEXT,
    pTableArray		IN		TEXT[],
	pAliasArray		IN		TEXT[])
    RETURNS text
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$outerJoinDeleteStatement
Author:       David Day
Date:         26/02/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
07/07/2021	| D Day			| Bug fix to resolve syntax issues with the dynamic DELETE statement creation.
23/02/2021  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Function to create outer join delete statement to remove the use of running the full materialized view query.

Arguments:      IN      pConst             
                IN      pTableNames
				IN		pTableAlias
				IN		pViewName
				IN		pRowidArray
				IN		pWhereClause
				IN		pTableName
				IN		pTableArray
				IN		pAliasArray
Returns:                TEXT

************************************************************************************************************************************
Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE	

	tJoinTableInfoFound		CHAR := 'N';
	tOtherTableFound		CHAR := 'N';
	
	tLeftJoinConditions		TEXT;
	tRightJoinConditions	TEXT;
	
	tTableName				TEXT := pTableName;
	
	iLeftJoinCnt 			INTEGER := 0;	
	iRightJoinCnt 			INTEGER := 0;	
	
	iLeftJoinAliasCnt 		INTEGER := 0;
	iRightJoinAliasCnt 		INTEGER := 0;
	
	tTablesSQL 				TEXT := pTableNames;
	  
	tSQL					TEXT;
	tLeftJoinLine			TEXT;
	tRightJoinLine			TEXT;
	
	tTableAliasReg 			TEXT := REPLACE(pTableAlias,'.','\.');	
	tTableAlias 			TEXT := REPLACE(pTableAlias,'.','');
	
	iStartPosition			INTEGER := 0;
	iEndPosition			INTEGER := 0;
	
	tLeftColumnAlias		TEXT;
	tRightColumnAlias 		TEXT;
	tOtherTableName 		TEXT;
	tOtherAlias				TEXT;
	tJoinConditions 		TEXT;
	tWhereClause 			TEXT;
	tWhereClauseCondition   TEXT;
	addWhereClauseJoinConditions TEXT;
	whereClauseConditionExists 	 CHAR := 'N';
	
	iAliasCnt 				INTEGER := 0;
	iAndCnt 				INTEGER := 0;
	iAliasLoopCnt 			INTEGER := 0;
	iDotCnt 				INTEGER := 0;
	iOtherAliasCnt 			INTEGER := 0;
	
	tTablesMarkerSQL		TEXT;	
	tTable_Alias			TEXT;
	
	rec						RECORD;
	
	tMatchedTable_Alias		TEXT;

BEGIN

tTablesSQL := regexp_replace(tTablesSQL,'left join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left outer join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right outer join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left  join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left  outer join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left  outer  join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left outer  join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right  join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right  outer join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right outer  join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right  outer  join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'RIGHT  JOIN','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'LEFT  JOIN','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,' on ',' ON ','gi');
	  
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
tTablesMarkerSQL :=	mv$regexpreplace(tTablesMarkerSQL,'[[:space:]]+'||'ON ',pConst.ON_TOKEN);
tTablesMarkerSQL := tTablesMarkerSQL||pConst.COMMA_INNER_TOKEN;

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
			
		IF tJoinTableInfoFound = 'N' THEN
		
			IF tTableName = tTableAlias THEN
		
				SELECT count(1) INTO iLeftJoinAliasCnt FROM regexp_matches(tLeftJoinLine,tTableName||pConst.ON_TOKEN,'g');
			
			ELSE 
			
				SELECT count(1) INTO iLeftJoinAliasCnt FROM regexp_matches(tLeftJoinLine,tTableName||'+[[:space:]]+'||tTableAlias||pConst.ON_TOKEN,'g');
			
			END IF;
			
			IF iLeftJoinAliasCnt > 0 THEN
				
				iStartPosition :=  mv$regexpinstr(tLeftJoinLine,pConst.ON_TOKEN,
					1,
					1,
					1,
					'i');
					
				tLeftJoinConditions := substr(tLeftJoinLine,iStartPosition);
				
				tLeftColumnAlias :=  TRIM(SUBSTR(tLeftJoinConditions,
										   1,
											 mv$regexpinstr(tLeftJoinConditions,
														   '(\.){1}',
														   1,
														   1)
										   - 1));
								  
				tRightColumnAlias := TRIM(SUBSTR(tLeftJoinConditions,mv$regexpinstr(tLeftJoinConditions,
												  '[[:space:]]+(=|>|<|<>|!=)',
												  1,
												  1,
												  1,
												  'i')));
								  
				tRightColumnAlias := TRIM(SUBSTR(tRightColumnAlias,
										   1,
											 mv$regexpinstr(tRightColumnAlias||'\.',
														   '(\.){1}',
														   1,
														   1)
										   - 1));

				tJoinTableInfoFound := 'Y';
				
			END IF;
			
			IF tJoinTableInfoFound = 'Y' THEN
			
				EXIT;
				
			END IF;
			
		END IF;	

	END LOOP;

ELSIF iRightJoinCnt > 0 AND tJoinTableInfoFound = 'N' THEN

	FOR i IN 1..iRightJoinCnt
	LOOP
	
	tRightJoinLine :=  'RIGHT JOIN'||substr(substr(tTablesMarkerSQL,mv$regexpinstr(tTablesMarkerSQL, 
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
			
		IF tJoinTableInfoFound = 'N' THEN
		
			IF tTableName = tTableAlias THEN
		
				SELECT count(1) INTO iRightJoinAliasCnt FROM regexp_matches(tLeftJoinLine,tTableName||pConst.ON_TOKEN,'g');
			
			ELSE 
			
				SELECT count(1) INTO iRightJoinAliasCnt FROM regexp_matches(tLeftJoinLine,tTableName||'+[[:space:]]+'||tTableAlias,'g');
			
			END IF;

			IF iRightJoinAliasCnt > 0 THEN
				
				iStartPosition :=  mv$regexpinstr(tRightJoinLine,pConst.ON_TOKEN,
					1,
					1,
					1,
					'i');
					
				tRightJoinConditions := substr(tRightJoinLine,iStartPosition);
				
				tLeftColumnAlias :=  TRIM(SUBSTR(tRightJoinConditions,
										   1,
											 mv$regexpinstr(tRightJoinConditions,
														   '(\.){1}',
														   1,
														   1)
										   - 1));
								  
				tRightColumnAlias := TRIM(SUBSTR(tRightJoinConditions,mv$regexpinstr(tRightJoinConditions,
												  '[[:space:]]+(=|>|<|<>|!=)',
												  1,
												  1,
												  1,
												  'i')));
								  
				tRightColumnAlias := TRIM(SUBSTR(tRightColumnAlias,
										   1,
											 mv$regexpinstr(tRightColumnAlias||'\.',
														   '(\.){1}',
														   1,
														   1)
										   - 1));

				tJoinTableInfoFound := 'Y';
				
			END IF;
			
			IF tJoinTableInfoFound = 'Y' THEN
			
				EXIT;
				
			END IF;
			
		END IF;
		
	END LOOP;

END IF;

FOR rec IN (SELECT UNNEST(pAliasArray) table_alias, UNNEST(pTableArray) table_name) LOOP

tTable_Alias := REPLACE(rec.table_alias,'.','');

IF tLeftColumnAlias = tTableAlias THEN

	IF tRightColumnAlias = tTable_Alias THEN
	
		tOtherTableName := rec.table_name;
		tOtherAlias := tRightColumnAlias;
		tJoinConditions := COALESCE(tLeftJoinConditions,tRightJoinConditions);		
		tJoinConditions := CONCAT(' ', tJoinConditions);		
		tMatchedTable_Alias := tLeftColumnAlias;
			
	END IF;
	
ELSE

	IF tLeftColumnAlias = tTable_Alias THEN
	
		tOtherTableName := rec.table_name;
		tOtherAlias := tLeftColumnAlias;
		tJoinConditions := COALESCE(tLeftJoinConditions,tRightJoinConditions);
		tJoinConditions := CONCAT(' ', tJoinConditions);		
		tMatchedTable_Alias := tRightColumnAlias;
			
	END IF;

END IF;

END LOOP;

tJoinConditions := REPLACE(tJoinConditions,CONCAT(' ',tMatchedTable_Alias||'.'),' src$.');
tJoinConditions := REPLACE(tJoinConditions,CONCAT(' ',tOtherAlias||'.'),' src$99.');
tJoinConditions := LTRIM(tJoinConditions);

IF pWhereClause <> '' THEN

	SELECT count(1) INTO iAliasCnt FROM regexp_matches(pWhereClause,tTableAliasReg,'g');

	IF iAliasCnt > 0 THEN

		tWhereClause :=
				REPLACE(TRIM(
				   mv$regexpreplace(
					  pWhereClause,
					  '([' || CHR (11) || CHR (13) || CHR (9) || ']+)',
					  ' ')),CHR(13),' ');

		tWhereClause := REPLACE(tWhereClause, CHR(9), CHR(32) );
		tWhereClause := REPLACE(tWhereClause, ' and ', ' AND ' );
		tWhereClause := REPLACE(tWhereClause, 'where ', 'WHERE ' );
		tWhereClause := REPLACE(tWhereClause, ' AND ', ' ### ' );
		tWhereClause := REPLACE(tWhereClause, 'WHERE ', ' ### ' );

		SELECT count(1) INTO iAndCnt FROM regexp_matches(tWhereClause,'###','g');

		tWhereClause := tWhereClause||'###';
		
		IF iAndCnt > 0 THEN

			FOR i IN 1..iAndCnt LOOP

				iAliasLoopCnt := iAliasLoopCnt + 1;

				IF iAliasLoopCnt = 1 THEN
				
					iStartPosition :=  mv$regexpinstr(tWhereClause,'###',
					1,
					i,
					1,
					'i');
					
					iEndPosition :=  mv$regexpinstr(tWhereClause,'###',
					1,
					i+1,
					1,
					'i');
					
					tWhereClauseCondition := TRIM(SUBSTR(tWhereClause,iStartPosition,iEndPosition-iStartPosition-3));										
										
					SELECT count(1) INTO iDotCnt FROM regexp_matches(tWhereClauseCondition,'\.','g');
					
					IF iDotCnt = 1 THEN
					
						SELECT count(1) INTO iAliasCnt FROM regexp_matches(tWhereClauseCondition,tTableAliasReg,'g');
						
						IF iAliasCnt = 1 THEN
						
							addWhereClauseJoinConditions := ' AND '|| tWhereClauseCondition;	
							whereClauseConditionExists = 'Y';
							
						END IF;
						
					ELSE
					
						iStartPosition :=  mv$regexpinstr(tWhereClause,'###',
						1,
						i,
						1,
						'i');

						iEndPosition :=  mv$regexpinstr(tWhereClause,'###',
						1,
						i+1,
						1,
						'i');
					
						tWhereClauseCondition := TRIM(SUBSTR(tWhereClause,iStartPosition,iEndPosition-iStartPosition-3));

						SELECT count(1) INTO iAliasCnt FROM regexp_matches(tWhereClauseCondition,tTableAliasReg,'g');
						SELECT count(1) INTO iOtherAliasCnt FROM regexp_matches(tWhereClauseCondition,tOtherAlias||'\.','g');

						IF iAliasCnt = 1 AND iOtherAliasCnt = 1 THEN

							IF whereClauseConditionExists = 'Y' THEN
								addWhereClauseJoinConditions := CONCAT(addWhereClauseJoinConditions,' AND ', tWhereClauseCondition);	
							ELSE
								addWhereClauseJoinConditions := ' AND '|| tWhereClauseCondition;	
								whereClauseConditionExists = 'Y';
							END IF;

						END IF;
						
					END IF;
						
				ELSE
				
					iStartPosition :=  mv$regexpinstr(tWhereClause,'###',
					1,
					i,
					1,
					'i');
					
					iEndPosition :=  mv$regexpinstr(tWhereClause,'###',
					1,
					i+1,
					1,
					'i');
					
					tWhereClauseCondition := TRIM(SUBSTR(tWhereClause,iStartPosition,iEndPosition-iStartPosition-3));
					
					SELECT count(1) INTO iDotCnt FROM regexp_matches(tWhereClauseCondition,'\.','g');
					
					IF iDotCnt = 1 THEN
					
						SELECT count(1) INTO iAliasCnt FROM regexp_matches(tWhereClauseCondition,tTableAliasReg,'g');
						
						IF iAliasCnt = 1 AND whereClauseConditionExists = 'Y' THEN
						
							addWhereClauseJoinConditions := CONCAT(addWhereClauseJoinConditions,' AND ', tWhereClauseCondition);
							
						ELSIF iAliasCnt = 1 AND whereClauseConditionExists = 'N' THEN

							addWhereClauseJoinConditions := ' AND '|| tWhereClauseCondition;	
							whereClauseConditionExists = 'Y';
	
						END IF;
						
					END IF;

					SELECT count(1) INTO iAliasCnt FROM regexp_matches(tWhereClauseCondition,tTableAliasReg,'g');
					SELECT count(1) INTO iOtherAliasCnt FROM regexp_matches(tWhereClauseCondition,tOtherAlias||'\.','g');
					
					IF iAliasCnt = 1 AND iOtherAliasCnt = 1 THEN
					
						IF whereClauseConditionExists = 'Y' THEN
						
							addWhereClauseJoinConditions := CONCAT(addWhereClauseJoinConditions,' AND ', tWhereClauseCondition);
							
						ELSE
						
							addWhereClauseJoinConditions := ' AND '|| tWhereClauseCondition;	
							whereClauseConditionExists = 'Y';

						END IF;
						
					END IF;
					
				END IF;

			END LOOP;

		END IF;
	
	END IF;
	
END IF;

addWhereClauseJoinConditions := REPLACE(addWhereClauseJoinConditions,CONCAT(' ',tMatchedTable_Alias||'.'),' src$.');
addWhereClauseJoinConditions := REPLACE(addWhereClauseJoinConditions,CONCAT(' ',tOtherAlias||'.'),' src$99.');

tJoinConditions := CONCAT(tJoinConditions,addWhereClauseJoinConditions);

tSQL := 'DELETE FROM '||pViewName||'
		 WHERE '||tOtherAlias||'_m_row$ IN (
				SELECT src$99.m_row$
				FROM
					(
						SELECT sna$.m_row$ rid$,
							   sna$.*
						FROM
							'||tTableName||' sna$
						WHERE
							m_row$ IN (SELECT UNNEST($1))
					) src$
				JOIN '||tOtherTableName||' src$99 ON '||tJoinConditions||'
							)';

--RAISE INFO '%', tSQL;

RETURN tSQL;

END;
$BODY$
LANGUAGE    plpgsql;
