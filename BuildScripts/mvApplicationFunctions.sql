/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvApplicationFunctions.sql
Author:       Mike Revitt
Date:         28/04/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
03/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
15/01/2020  | M Revitt      | Fixed the bug in mv$removeMaterializedViewLog
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This is the build script for the Application database procedures that are required to support the Materialized View
                fast refresh process.

                This script contains procedures that rely on other database procedures having been previously created and must
                therefore be run last in the build process.

Notes:          Some of the procedures in this file rely on procedures that are created within this file and so whilst the procedures
                should be maintained in alphabetic order, this is not always possible.

                All procedures must be created with SECURITY DEFINER to ensure they run with the privileges of the owner.

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Debug:          Add a variant of the following command anywhere you need some debug inforaiton
                RAISE NOTICE '<Funciton/Procedure Name> % %',  CHR(10), <Variable to be examined>;

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

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

-- psql -h localhost -p 5432 -d postgres -U pgrs_mview -q -f mvApplicationFunctions.sql

-- -------------------- Write DROP-PROCEDURE-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP PROCEDURE IF EXISTS mv$createMaterializedView;
DROP PROCEDURE IF EXISTS mv$createMaterializedViewlog;
DROP PROCEDURE IF EXISTS mv$refreshMaterializedView;
DROP FUNCTION IF EXISTS mv$help;
DROP PROCEDURE IF EXISTS mv$removeMaterializedView;
DROP PROCEDURE IF EXISTS mv$removeMaterializedViewLog;

SET CLIENT_MIN_MESSAGES = NOTICE;

-- -------------------- Write CREATE-PROCEDURE-FUNCTION-stage scripts --------------------
CREATE OR REPLACE
PROCEDURE    mv$createMaterializedView
            (
                pViewName           IN      TEXT,
                pSelectStatement    IN      TEXT,
                pOwner              IN      TEXT        DEFAULT USER,
                pNamedColumns       IN      TEXT        DEFAULT NULL,
                pStorageClause      IN      TEXT        DEFAULT NULL,
                pFastRefresh        IN      BOOLEAN     DEFAULT FALSE,
				pParallel			IN		TEXT		DEFAULT 'N',
				pParallelJobs		IN		INTEGER		DEFAULT 0,
				pParallelColumn		IN		TEXT		DEFAULT NULL,
				pParallelAlias		IN 		TEXT		DEFAULT NULL,
				pParallelUser		IN 		TEXT		DEFAULT NULL,
				pParallelDbname		IN 		TEXT		DEFAULT NULL				
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createMaterializedView
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
30/08/2022  | D Day			| Added mv log and trigger check
12/07/2021	| D Day			| Added new input parameters to support parallel inserts during build phase.
10/03/2021  | D Day 		| Added additional input parameters to call mv$insertPgMviewOuterJoinDetails to support dynamical delete_sql
			|				| statement for performance improvements to the outer join alias delete and re-insert routine for DML type INSERT.
22/10/2020	| D Day			| Defect fix - added two variables tQueryJoinsMultiTabCntArray and tQueryJoinsMultiTabPosArray to out in
			|				| arrays to handle DML changes being applied from materialized view log table that gets used more than once by
			|				| the same stored query. This procedure calls mv$insertPgMview to insert these new arrays into pg$mviews
			|				| data dictionary table to support the refresh process.
29/06/2020	| D Day			| Defect fix - added new procedural variables to support DELETE statements for DML Type INSERT to resolve
			|				| duplicate rows issue.
03/06/2020	| D Day			| Changed function to procedure to allow support/control of COMMITS within the refresh process.
28/04/2020	| D Day			| Added tTableNames input value parameter to mv$insertPgMviewOuterJoinDetails function call
23/07/2019  | D Day			| Added function mv$insertPgMviewOuterJoinDetails to handle Outer Join table DELETE
			|				| changes.
12/11/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Creates a materialized view, as a base table, and then populates the data dictionary table before calling the full
                refresh routine to populate it.

            This procedure performs the following steps
            1)  A base table is created based on the select statement provided
            2)  The MV_M_ROW$_COLUMN column is added to the base table
            3)  A record of the materialized view is entered into the data dictionary table

Notes:          If a materialized view with fast refresh is requested then a materialized view log table must have been pre-created

Arguments:      IN      pViewName           The name of the materialized view to be created
                IN      pSelectStatement    The SQL query that will be used to create the view
                IN      pOwner              Optional, where the view is to be created, defaults to current user
                IN      pNamedColumns       Optional, allows the view to be created with different column names to the base table
                                            This list is positional so must match the position and number of columns in the
                                            select statment
                IN      pStorageClause      Optional, storage clause for the materialized view
				IN		pParallel			Optional, set to Y if you want to run build insert in parallel
				IN		pParallelJobs		Optional, if pParallel set to Y then set how many parallel jobs are required
				IN		pParallelColumn		Optional, if pParallel set to Y then add date column you want to split insert into parallel 
											cron sessions by date range.
				IN		pParallelAlias		Optional, if pParallel set to Y then add date column alias you want to split insert into parallel 
											cron sessions by date range.
				IN		pParallelUser		Optional, if pParallel set to Y then add user for pg_cron job sessions.
				IN		pParallelDbname		Optional, if pParallel set to Y then add dbname for pg_cron job sessions.
                IN      pFastRefresh        Defaults to FALSE, but if set to yes then materialized view fast refresh is supported
Returns:                VOID
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    rConst              	mv$allConstants;

    tSelectColumns      	TEXT        := NULL;
    tTableNames         	TEXT        := NULL;
    tViewColumns        	TEXT        := NULL;
    tWhereClause        	TEXT        := NULL;
    tRowidArray         	TEXT[];
    tTableArray         	TEXT[];
    tAliasArray         	TEXT[];
    tOuterTableArray    	TEXT[];
    tInnerAliasArray    	TEXT[];
    tInnerRowidArray    	TEXT[];
	tOuterLeftAliasArray 	TEXT[];
	tOuterRightAliasArray 	TEXT[];
	tLeftOuterJoinArray 	TEXT[];
	tRightOuterJoinArray 	TEXT[];
	tInnerJoinTableNameArray	TEXT[];
	tInnerJoinTableAliasArray	TEXT[];
	tInnerJoinTableRowidArray	TEXT[];
	tInnerJoinOtherTableNameArray	TEXT[];
	tInnerJoinOtherTableAliasArray	TEXT[];
	tInnerJoinOtherTableRowidArray	TEXT[];
	tQueryJoinsMultiTabCntArray		SMALLINT[];
	tQueryJoinsMultiTabPosArray     SMALLINT[];	

BEGIN

    rConst      := mv$buildAllConstants();
	
	IF pParallel = 'Y' THEN
	
		IF pParallelColumn IS NULL OR pParallelAlias IS NULL OR pParallelJobs = 0 OR pParallelUser IS NULL OR pParallelDbname IS NULL THEN
		
			RAISE INFO      'Exception in procedure mv$createMaterializedView';
			RAISE EXCEPTION 'Error: Procedure input parameter pParallel set to Y but either pParallelColumn OR pParallelAlias OR pParallelJobs OR pParallelUser OR pParallelDbname have not been set from their default value';
			
		END IF;
		
	END IF;

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
            pInnerAliasArray,
            pInnerRowidArray,
			pOuterLeftAliasArray,
			pOuterRightAliasArray,
			pLeftOuterJoinArray,
			pRightOuterJoinArray,
			pInnerJoinTableNameArray,
			pInnerJoinTableAliasArray,
			pInnerJoinTableRowidArray,
			pInnerJoinOtherTableNameArray,		
			pInnerJoinOtherTableAliasArray,
			pInnerJoinOtherTableRowidArray,
			pQueryJoinsMultiTabCntArray,
			pQueryJoinsMultiTabPosArray

    FROM
            mv$extractCompoundViewTables( rConst, tTableNames )
    INTO
            tTableArray,
            tAliasArray,
            tRowidArray,
            tOuterTableArray,
            tInnerAliasArray,
            tInnerRowidArray,
			tOuterLeftAliasArray,
			tOuterRightAliasArray,
			tLeftOuterJoinArray,
			tRightOuterJoinArray,
			tInnerJoinTableNameArray,
			tInnerJoinTableAliasArray,
			tInnerJoinTableRowidArray,
			tInnerJoinOtherTableNameArray,		
			tInnerJoinOtherTableAliasArray,
			tInnerJoinOtherTableRowidArray,
			tQueryJoinsMultiTabCntArray,
			tQueryJoinsMultiTabPosArray;

    CALL mv$createPgMv$Table
                        (
                            rConst,
                            pOwner,
                            pViewName,
                            pNamedColumns,
                            tSelectColumns,
                            tTableNames,
                            pStorageClause,
							pParallel,
							tViewColumns
                        );
						
	CALL mv$addRow$ToMv$Table
            (
                rConst,
                pOwner,
                pViewName,
                tAliasArray,
                tRowidArray,
				pParallel,
                tViewColumns,
                tSelectColumns
            );
	
    CALL mv$insertPgMview
                (
                    rConst,
                    pOwner,
                    pViewName,
                    tViewColumns,
                    tSelectColumns,
                    tTableNames,
                    tWhereClause,
                    tTableArray,
                    tAliasArray,
                    tRowidArray,
                    tOuterTableArray,
                    tInnerAliasArray,
                    tInnerRowidArray,
					tInnerJoinTableNameArray,
					tInnerJoinTableAliasArray,
					tInnerJoinTableRowidArray,
					tInnerJoinOtherTableNameArray,		
					tInnerJoinOtherTableAliasArray,
					tInnerJoinOtherTableRowidArray,
					tQueryJoinsMultiTabCntArray,
					tQueryJoinsMultiTabPosArray,
					pParallel,
					pParallelJobs,
					pParallelColumn,
					pParallelAlias,
					pParallelUser,
					pParallelDbname,
                    pFastRefresh
                );
				
	CALL mv$insertPgMviewOuterJoinDetails
			(	rConst,
                pOwner,
                pViewName,
                tSelectColumns,
				tTableNames,
                tAliasArray,
                tRowidArray,
                tOuterTableArray,
				tOuterLeftAliasArray,
				tOuterRightAliasArray,
				tLeftOuterJoinArray,
				tRightOuterJoinArray,
				tWhereClause,
				tTableArray
			 );
			 
	CALL mv$CheckMvLogExists( rConst, pViewName, tTableArray );

    CALL mv$refreshMaterializedViewInitial( rConst , pOwner, pViewName, pParallel );

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$createMaterializedViewlog
            (
                pTableName          	IN      TEXT,
                pOwner              	IN      TEXT     DEFAULT USER,
                pStorageClause      	IN      TEXT     DEFAULT NULL,
				pAddRow$ToSourceTable	IN		TEXT	 DEFAULT 'Y'
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createMaterializedViewlog
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
27/07/2021	| D Day			| Added new input parameter pAddRow$ToSourceTable to support building a materialized view log without
			|				| adding the source table m_row$ column with values.
03/06/2020	| D Day			| Changed function to procedure to allow support/control of COMMITS within the refresh process.
12/11/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Creates a materialized view log against the base table, which is mandatory for fast refresh materialized views,
                sets up the row tracking on the base table, adds a database trigger to the base table and populates the data
                dictionary tables

                This procedure performs the following steps
                1)  The MV_M_ROW$_COLUMN column is added to the base table
                2)  A log table is created to hold a record of all changes to the base table
                3)  Creates a trigger on the base table to populate the log table
                4)  A record of the materialized view log is entered into the data dictionary table

Notes:          This is mandatory for a Fast Refresh Materialized View

Arguments:      IN      pTableName          	The name of the base table upon which the materialized view is created
                IN      pOwner              	Optional, the owner of the base table, defaults to current user
                IN      pStorageClause      	Optional, storage clause for the materialized view log
				IN		pAddRow$ToSourceTable	Optional, add m_row$ column to source table linked to materialized view log
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rConst          mv$allConstants;

    tSqlStatement   TEXT    := NULL;
    tLog$Name       TEXT    := NULL;
    tTriggerName    TEXT    := NULL;

BEGIN

    rConst          := mv$buildAllConstants();
    tLog$Name       := rConst.MV_LOG_TABLE_PREFIX   || SUBSTRING( pTableName, 1, rConst.MV_MAX_BASE_TABLE_LEN );
    tTriggerName    := rConst.MV_TRIGGER_PREFIX     || SUBSTRING( pTableName, 1, rConst.MV_MAX_BASE_TABLE_LEN );
	
	IF pAddRow$ToSourceTable = 'Y' THEN 
		CALL mv$addRow$ToSourceTable( rConst, pOwner, pTableName );
	ELSE
		CALL mv$checkRow$ExistsOnSourceTable( pOwner, pTableName );				  
	END IF;
    CALL mv$createMvLog$Table(       rConst, pOwner, tLog$Name,  pStorageClause );
    CALL mv$addIndexToMvLog$Table(   rConst, pOwner, tLog$Name                  );
    CALL mv$createMvLogTrigger(      rConst, pOwner, pTableName, tTriggerName   );
    CALL mv$insertPgMviewLogs
                (
                    rConst,
                    pOwner,
                    tLog$Name,
                    pTableName,
                    tTriggerName
                );

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$help()
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$help
Author:       Mike Revitt
Date:         18/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Displays the help message

Arguments:      IN      None
Returns:                TEXT    The help message

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rConst          mv$allConstants;

BEGIN

    rConst := mv$buildAllConstants();
    RETURN rConst.HELP_TEXT;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$help';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$refreshMaterializedView
            (
                pViewName           IN      TEXT,
                pOwner              IN      TEXT    DEFAULT USER,
                pFastRefresh        IN      BOOLEAN DEFAULT FALSE,
				pParallel			IN		TEXT		DEFAULT 'N',
				pParallelJobs		IN		INTEGER		DEFAULT 0,
				pParallelColumn		IN		TEXT		DEFAULT NULL,
				pParallelAlias		IN 		TEXT		DEFAULT NULL,
				pParallelUser		IN 		TEXT		DEFAULT NULL,
				pParallelDbname		IN 		TEXT		DEFAULT NULL
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$refreshMaterializedView
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
15/07/2021	| D Day			| Added logic to run complete refresh INSERT in parallel
03/06/2020	| D Day			| Changed function to procedure to allow support/control of COMMITS within the refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Loops through each of the base tables, upon which this materialised view is based, and updates the materialized
                view for each table in turn

Notes:          This procedure must come after the creation of the 2 procedures it calls
                o   mv$refreshMaterializedViewFast;
                o   mv$refreshMaterializedViewFull;

Arguments:      IN      pViewName           The name of the materialized view
                IN      pOwner              Optional, the owner of the materialized view, defaults to user
                IN      pFastRefresh        Defaults to FALSE, but if set to yes then materialized view fast refresh is performed
				IN		pParallel			Optional, set to Y if you want to run build insert in parallel
				IN		pParallelJobs		Optional, if pParallel set to Y then set how many parallel jobs are required
				IN		pParallelColumn		Optional, if pParallel set to Y then add date column you want to split insert into parallel 
											cron sessions by date range.
				IN		pParallelAlias		Optional, if pParallel set to Y then add date column alias you want to split insert into parallel 
											cron sessions by date range.
				IN		pParallelUser		Optional, if pParallel set to Y then add user for pg_cron job sessions.
				IN		pParallelDbname		Optional, if pParallel set to Y then add dbname for pg_cron job sessions.
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
	
	rConst      mv$allConstants;

BEGIN

    rConst  := mv$buildAllConstants();

    IF TRUE = pFastRefresh
    THEN
        CALL mv$refreshMaterializedViewFast( rConst, pOwner, pViewName );
    ELSE
	
		BEGIN
		
			IF pParallel = 'Y' THEN
		
				IF pParallelColumn IS NULL OR pParallelAlias IS NULL OR pParallelJobs = 0 OR pParallelUser IS NULL OR pParallelDbname IS NULL THEN
				
					RAISE INFO      'Exception in procedure mv$refreshMaterializedView';
					RAISE EXCEPTION 'Error: Procedure input parameter pParallel set to Y but either pParallelColumn OR pParallelAlias OR pParallelJobs OR pParallelUser OR pParallelDbname have not been set from their default value';
					
				END IF;
			
			END IF;
		
		END;
	
        CALL mv$refreshMaterializedViewFull( rConst, pOwner, pViewName, pParallel, pParallelJobs, pParallelColumn, pParallelAlias, pParallelUser, pParallelDbname );
    END IF;

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$removeMaterializedView
            (
                pViewName           IN      TEXT,
                pOwner              IN      TEXT        DEFAULT USER
            )
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
03/06/2020	| D Day			| Changed function to procedure to allow support/control of COMMITS within the fast refresh process.            |               |
01/07/2019	| David Day		| Added procedure mv$deletePgMviewOjDetails to delete data from data dictionary table pgmview_oj_details. 
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Removes a materialized view, clears down the entries in the Materialized View Log adn then removes the entry from
                the data dictionary table

                This procedure performs the following steps
                1)  Clears the MV Bit from all base tables logs used by thie materialized view
                2)  Drops the materialized view
                3)  Removes the MV_M_ROW$_COLUMN column from the base table
                4)  Removes the record of the materialized view from the data dictionary table

Arguments:      IN      pViewName           The name of the materialized view
                IN      pOwner              Optional, the owner of the materialized view, defaults to user
Returns:                VOID
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMview    pg$mviews;
    rConst      mv$allConstants;

BEGIN

    rConst      := mv$buildAllConstants();
    aPgMview    := mv$getPgMviewTableData(      rConst, pOwner, pViewName           );
    CALL mv$clearAllPgMvLogTableBits( rConst, pOwner, pViewName           );
    CALL mv$clearPgMviewLogBit(       rConst, pOwner, pViewName           );
    CALL mv$dropTable(                rConst, pOwner, aPgMview.view_name  );
    CALL mv$deletePgMview(               pOwner, pViewName           );
	CALL mv$deletePgMviewOjDetails(      pOwner, pViewName           );

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$removeMaterializedViewLog
            (
                pTableName          	IN      TEXT,
                pOwner              	IN      TEXT     DEFAULT USER,
				pDropRow$ToSourceTable	IN		TEXT	 DEFAULT 'Y'
            )
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
27/07/2021	| D Day			| Add new parameter pDropRow$ToSourceTable to provide user option of not dropping the m_row$ column.
03/06/2020	| D Day			| Changed function to procedure to allow support/control of COMMITS within the refresh process.
15/01/2020  | M Revitt      | Changed bitmap check to look at all values in the bitmap array
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Removes a materialized view log from the base table.

            This procedure has the following pre-requisites
            1)  All Materialized Views, with an interest in the log, must have been previously removed

            This procedure performs the following steps
            1)  Drops the trigger from the base table
            2)  Drops the Materialized View Log table
            3)  Removes the MV_M_ROW$_COLUMN column from the base table
            4)  Removes the record of the materialized view from the data dictionary table

Arguments:      IN      pTableName          The name of the base table containing the materialized view log
                IN      pOwner              Optional, the owner of the materialized view, defaults to user
Returns:                VOID
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rConst          mv$allConstants;
    aViewLog        pg$mview_logs;

    tSqlStatement       TEXT;
    tLog$Name           TEXT        := NULL;
    tMvTriggerName      TEXT        := NULL;

BEGIN

    rConst   := mv$buildAllConstants();
    aViewLog := mv$getPgMviewLogTableData( rConst, pTableName );

    IF rConst.BITMAP_NOT_SET = ALL( aViewLog.pg_mview_bitmap )
    THEN
        CALL mv$dropTrigger(                 rConst, pOwner, aViewLog.trigger_name, pTableName   );
        CALL mv$dropTable(                   rConst, pOwner, aViewLog.pglog$_name                );
		IF pDropRow$ToSourceTable = 'Y' THEN
			CALL mv$removeRow$FromSourceTable(   rConst, pOwner, pTableName                          );
		END IF;
        CALL mv$deletePgMviewLog(                    pOwner, pTableName                          );
    ELSE
        RAISE EXCEPTION 'The Materialized View Log on Table % is still in use', pTableName;
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$removeMaterializedViewLog';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;

END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------

GRANT   EXECUTE ON  PROCEDURE   mv$createMaterializedViewlog    TO  pgmv$_execute;
GRANT   EXECUTE ON  PROCEDURE   mv$createMaterializedView       TO  pgmv$_execute;
GRANT   EXECUTE ON  PROCEDURE   mv$refreshMaterializedView      TO  pgmv$_execute;
GRANT   EXECUTE ON  PROCEDURE   mv$removeMaterializedView       TO  pgmv$_execute;
GRANT   EXECUTE ON  PROCEDURE   mv$removeMaterializedViewLog    TO  pgmv$_execute;
GRANT   EXECUTE ON  FUNCTION    mv$help                         TO  pgmv$_execute;
