﻿/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvSimpleFunctions.sql
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
14/01/2020  | M Revitt      | Changes to fix the array boundaries when doing > 62 materialised views per table
            |               | Fixed bug in getBitValue
30/10/2019  | M Revitt      | Added an exception handler to the bottom of every function to aid bug and error tracking
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This is the build script for the simple database procedures that are required to support the Materialized View
                fast refresh process.

                This script contains procedures that can run standalone and must therefore be run before the complex procedures and
                application procedures can be created.

Notes:          The procedures in this script should be maintained in alphabetic order.

                All procedures must be created with SECURITY DEFINER to ensure they run with the privileges of the owner.

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents queries against the information_schema,
                this bug is fixed in versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Debug:          Add a variant of the following command anywhere you need some debug information
                RAISE NOTICE '<Funciton Name> % %',  CHR(10), <Variable to be examined>;

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

-- psql -h localhost -p 5432 -d postgres -U pgrs_mview -q -f mvSimpleFunctions.sql

----------------------- Write DROP-FUNCTION-stage scripts ----------------------
SET     CLIENT_MIN_MESSAGES = ERROR;

DROP PROCEDURE IF EXISTS mv$addIndexToMvLog$Table;
DROP PROCEDURE IF EXISTS mv$checkRow$ExistsOnSourceTable;
DROP PROCEDURE IF EXISTS mv$addRow$ToMv$Table;
DROP PROCEDURE IF EXISTS mv$addRow$ToSourceTable;
DROP FUNCTION IF EXISTS mv$checkIfOuterJoinedTable;
DROP PROCEDURE IF EXISTS mv$clearSpentPgMviewLogs;
DROP PROCEDURE IF EXISTS mv$createMvLog$Table;
DROP PROCEDURE IF EXISTS mv$createMvLogTrigger;
DROP FUNCTION IF EXISTS mv$createRow$Column;
DROP FUNCTION IF EXISTS mv$deconstructSqlStatement;
DROP PROCEDURE IF EXISTS mv$deleteMaterializedViewRows;
DROP PROCEDURE IF EXISTS mv$deletePgMview;
DROP PROCEDURE IF EXISTS mv$deletePgMviewOjDetails;
DROP PROCEDURE IF EXISTS mv$deletePgMviewLog;
DROP PROCEDURE IF EXISTS mv$dropTable;
DROP PROCEDURE IF EXISTS mv$dropTrigger;
DROP FUNCTION IF EXISTS mv$extractCompoundViewTables;
DROP FUNCTION IF EXISTS mv$findFirstFreeBit;
DROP FUNCTION IF EXISTS mv$getBitValue;
DROP FUNCTION IF EXISTS mv$getPgMviewLogTableData;
DROP FUNCTION IF EXISTS mv$getPgMviewTableData;
DROP FUNCTION IF EXISTS mv$getPgMviewOjDetailsTableData;
DROP FUNCTION IF EXISTS mv$getPgMviewViewColumns;
DROP FUNCTION IF EXISTS mv$getSourceTableSchema;
DROP PROCEDURE IF EXISTS mv$grantSelectPrivileges;
DROP PROCEDURE IF EXISTS mv$insertPgMviewLogs;
DROP PROCEDURE IF EXISTS mv$removeRow$FromSourceTable;
DROP FUNCTION IF EXISTS mv$replaceCommandWithToken;
DROP PROCEDURE IF EXISTS mv$truncateMaterializedView;
DROP PROCEDURE IF EXISTS mv$createindexestemptable;
DROP PROCEDURE IF EXISTS mv$dropmvindexes;
DROP PROCEDURE IF EXISTS mv$readdmvindexes;
DROP PROCEDURE IF EXISTS mv$dropindexestemptable;
DROP PROCEDURE IF EXISTS mv$setQueryJoinsMultiTableCount;
DROP PROCEDURE IF EXISTS mv$setQueryJoinsMultiTablePosition;
DROP FUNCTION IF EXISTS mv$setCronSchedule;
DROP FUNCTION IF EXISTS mv$setFromAndToTimestampRange;
DROP PROCEDURE IF EXISTS mv$updatePgMviewTableParallelData;
DROP FUNCTION IF EXISTS mv$CheckTriggerExists;
DROP FUNCTION IF EXISTS mv$CheckMvLogExists;

----------------------- Write CREATE-FUNCTION-stage scripts --------------------
SET CLIENT_MIN_MESSAGES = NOTICE;

CREATE OR REPLACE
PROCEDURE    mv$addIndexToMvLog$Table
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$addIndexToMvLog$Table
Author:       Mike Revitt
Date:         07/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
05/11/2019  | M Revitt      | mv$clearPgMvLogTableBits is now a complex function so move it into the conplex script
07/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This procedure creates an index on the materilized view log table to speed up bit manipulation

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pPgLog$Name         The name of the materialized view log table
Returns:                VOID
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tIndexName      TEXT;
    tSqlStatement   TEXT;

BEGIN
    tIndexName      :=  pPgLog$Name || pConst.UNDERSCORE_CHARACTER  || pConst.BITMAP_COLUMN     || pConst.MV_INDEX_SUFFIX;

    tSqlStatement   :=  pConst.CREATE_INDEX || tIndexName           ||
                        pConst.ON_COMMAND   || pOwner               || pConst.DOT_CHARACTER     || pPgLog$Name ||
                                               pConst.OPEN_BRACKET  || pConst.BITMAP_COLUMN     || pConst.CLOSE_BRACKET;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$addIndexToMvLog$Table';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$checkRow$ExistsOnSourceTable
            (
                pOwner          IN      TEXT,
                pTableName      IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$checkRow$ExistsOnSourceTable
Author:       David Day
Date:         27/07/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
27/07/2021  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    PostGre does not have a ROWID pseudo column and so a ROWID column has to be added to the source table. This procedure
				checks to confirm if the m_row$ exists as this is a pre-requisite if the materialized view log creation has been 
				selected to NOT add this column as part of the build of the source table log. This option has been added to provide 
				the user to add this outside of the log creation process due to performance issues on how long this can take to add 
				due to populating the UUID values during automated routines.

Arguments:      IN      pOwner              The owner of the object
                IN      pTableName          The name of the materialized view source table

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

BEGIN

	IF NOT EXISTS(SELECT
				  FROM   information_schema.columns
				  WHERE  table_schema    = LOWER( pOwner )
				  AND    table_name      = LOWER( pTableName )
				  AND	 column_name 	 = 'm_row$') THEN
	
		RAISE INFO      'Exception in procedure mv$createMaterializedViewlog';
		RAISE EXCEPTION 'Error: Column m_row$ does not exist. If parameter pAddRow$ToSourceTable is set to N there is a pre-requisite to add m_row$ to materialized view log source table %.%.', pOwner, pTableName; 

	ELSE
	
		IF NOT EXISTS(SELECT
					  FROM pg_indexes
					  WHERE tablename  = LOWER( pTableName )
					  AND   schemaname = LOWER( pOwner ) 
					  AND   indexdef   LIKE '%m_row$%' ) THEN
					  
		RAISE INFO      'Exception in procedure mv$createMaterializedViewlog';
		RAISE EXCEPTION 'Error: Column m_row$ exists however there has been no index built on the column. If parameter pAddRow$ToSourceTable is set to N there is a pre-requisite to build index on column m_row$ a to materialized view log source table %.%.', pOwner, pTableName;  
		
		END IF;
		
	END IF;

EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$checkRow$ExistsOnSourceTable';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$addRow$ToMv$Table
            (
                pConst              IN      mv$allConstants,
                pOwner              IN      TEXT,
                pViewName           IN      TEXT,
                pAliasArray         IN      TEXT[],
                pRowidArray         IN      TEXT[],
				pParallel			IN		TEXT,
                pViewColumns        INOUT   TEXT,
                pSelectColumns      INOUT   TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$addRow$ToMv$Table
Author:       Mike Revitt
Date:         15/01/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
23/07/2021	| D Day			| Added logic to support running build in parallel.
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
15/01/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    For every table that is used to construct this materialized view, add a MV_M_ROW$_COLUMN to the base table.

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view table
                IN      pAliasArray         An array containing the table aliases that make up the materialized view
                IN      pRowidArray         An array containing the MV_M_ROW$_COLUMN column name for the base table
				IN		pParallel			Optional, build in parallel				
                INOUT   pViewColumns        This is the list of view columns to which the MV_M_ROW$_COLUMNs will be added
                INOUT   pSelectColumns      The columns from the SQL Statement that created the materialised view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tAddColumn      TEXT;
    tCreateIndex    TEXT;
    tIndexName      TEXT;
    tSqlStatement   TEXT;
    tRowidColumn    TEXT;
    iTableArryPos   INT     := 0;

BEGIN

    tAddColumn      := pConst.ALTER_TABLE || pOwner || pConst.DOT_CHARACTER || pViewName || pConst.NEW_LINE || pConst.ADD_COLUMN;
    tCreateIndex    := pConst.CREATE_INDEX;

    FOR i IN array_lower( pAliasArray, 1 ) .. array_upper( pAliasArray, 1 )
    LOOP
        tIndexName      := pViewName    || pConst.UNDERSCORE_CHARACTER  || pRowidArray[i]       || pConst.MV_INDEX_SUFFIX;
        tSqlStatement   := tAddColumn   || pRowidArray[i]               || pConst.MV_M_ROW$_COLUMN_FORMAT;

		IF pParallel = 'Y' THEN
		
			PERFORM * FROM dblink('pgmv$_instance',tSqlStatement) AS p (ret TEXT);
		
		ELSE

			EXECUTE tSqlStatement;
			
		END IF;

        tSqlStatement   :=  tCreateIndex    || tIndexName               || pConst.ON_COMMAND    ||
                                               pOwner                   || pConst.DOT_CHARACTER || pViewName ||
                                               pConst.OPEN_BRACKET      || pRowidArray[i]       || pConst.CLOSE_BRACKET;
		IF pParallel = 'Y' THEN
		
			PERFORM * FROM dblink('pgmv$_instance',tSqlStatement) AS p (ret TEXT);
		
		ELSE

			EXECUTE tSqlStatement;
			
		END IF;

        pViewColumns    :=  pViewColumns    || pConst.COMMA_CHARACTER   || pRowidArray[i];
        pSelectColumns  :=  pSelectColumns  || pConst.COMMA_CHARACTER   || pAliasArray[i]       || pConst.MV_M_ROW$_COLUMN;
        iTableArryPos   := iTableArryPos + 1;
    END LOOP;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$addRow$ToMv$Table';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$addRow$ToSourceTable
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pTableName      IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$addRow$ToSourceTable
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
27/07/2021	| D Day			| Added check to only add m_row$ column on the source table if it does not already exist. Added committing
			|				| to support running in parallel.
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    PostGre does not have a ROWID pseudo column and so a ROWID column has to be added to the source table, ideally this
                should be a hidden column, but I can't find any way of doing this

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pTableName          The name of the materialized view source table

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;

BEGIN

	tSqlStatement := pConst.ALTER_TABLE || pOwner || pConst.DOT_CHARACTER || pTableName || pConst.ADD_M_ROW$_COLUMN_TO_TABLE;
	
	IF NOT EXISTS(SELECT
			  FROM   information_schema.columns
			  WHERE  table_schema    = LOWER( pOwner )
			  AND    table_name      = LOWER( pTableName )
			  AND	 column_name 	 = 'm_row$') THEN
			  
	    EXECUTE tSqlStatement;
		
		RETURN;
		
		COMMIT;
	
	END IF;
	
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$checkIfOuterJoinedTable
            (
                pConst              IN      mv$allConstants,
                pTableName          IN      TEXT,
                pOuterTable		    IN      TEXT
            )
    RETURNS BOOLEAN
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$checkIfOuterJoinedTable
Author:       Mike Revitt
Date:         04/04/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
03/03/2020	| D Day			| Defect fix Replaced IN pOuterTableArray parameter with pOuterTable and TEXT[] array variable as TEXT. 
			|				| This was not considering if the table name check has a corresponding inner and outer join condition
			|				| related to the same table name. By passing only the outer table value this will confirm if it's and
			|				| outer join table or not.
18/06/2019  | M Revitt      | Added an Exception Handler
05/06/2019  | M Revitt      | Change ARRAY_UPPER and ARRYA_LOWER to FOREACH ... IN ARRAY
04/04/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Some actions against outer joined tables need to be performed differently, so this function checks to see if the
                table is outer joined

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pTableName          The name of the table to check
                IN      pOuterTable		    The outer join table value for the same pTableName value position
Returns:                BOOLEAN             TRUE if we can find the record, otherwise FALSE
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    bResult     BOOLEAN := FALSE;

BEGIN

    IF pTableName = pOuterTable
    THEN
            bResult := TRUE;
    END IF;

    RETURN( bResult );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$checkIfOuterJoinedTable';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$clearSpentPgMviewLogs
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$clearSpentPgMviewLogs
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Bitmaps are how we manage multiple registrations against the same base table, once all interested materialized
                views have removed their interest in the materialized log row the bitmap will be set to 0 and can be deleted

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pPgLog$Name         The name of the materialized view log table

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT;

BEGIN

    tSqlStatement := pConst.DELETE_FROM || pOwner || pConst.DOT_CHARACTER || pPgLog$Name || pConst.MV_LOG$_WHERE_BITMAP_ZERO;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$clearSpentPgMviewLogs';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$createMvLog$Table
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pStorageClause  IN      TEXT     DEFAULT NULL
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createMvLog$Table
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This function creates the materilized view log table against the source table for the materialized view

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pPgLog$Name         The name of the materialized view log table
                IN      pStorageClause      Optional, storage clause for the materialized view log

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;

BEGIN
    tSqlStatement := pConst.CREATE_TABLE || pOwner || pConst.DOT_CHARACTER || pPgLog$Name || pConst.MV_LOG_COLUMNS;

    IF pStorageClause IS NOT NULL
    THEN
        tSqlStatement := tSqlStatement || pStorageClause;
    END IF;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$createMvLog$Table';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$createMvLogTrigger
            (
                pConst              IN      mv$allConstants,
                pOwner              IN      TEXT,
                pTableName          IN      TEXT,
                pMvTriggerName      IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createMvLogTrigger
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
04/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    After the materialized view log table has been created a trigger is required on the source table to populate the
                materialized view log

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pTableName          The name of the materialized view source table
                IN      pMvTriggerName      The name of the materialized view source trigger

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;

BEGIN

    tSqlStatement :=    pConst.TRIGGER_CREATE          || pMvTriggerName   ||
                        pConst.TRIGGER_AFTER_DML       || pOwner           || pConst.DOT_CHARACTER  || pTableName ||
                        pConst.TRIGGER_FOR_EACH_ROW;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$createMvLogTrigger';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$createRow$Column
            (
                pConst      IN      mv$allConstants,
                pTableAlias IN      TEXT
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createRow$Column
Author:       Mike Revitt
Date:         15/01/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
18/06/2019  | M Revitt      | Add and Exception Handler
15/01/2019  | M Revitt      | Initial version
11/07/2019  | D Day         | Defect fix - changed code to use base table alias instead of base table name for the m_row$ column name
            |               | used to refresh the materialized view as this was not working when query joined against the same table
            |               | more than once.
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    For every table that is used to construct this materialized view, add a MV_M_ROW$_COLUMN to the base table.

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pTableName          The name of the materialized view source table
Returns:                TEXT                The name for the MV_M_ROW$_COLUMN added
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tAddColumn      TEXT;
    tCreateIndex    TEXT;
    tIndexName      TEXT;
    tSqlStatement   TEXT;
    tRowidColumn    TEXT;
    iTableArryPos   INT     := 0;
	tTableAlias		TEXT;

BEGIN

	tTableAlias := LOWER(TRIM(replace(pTableAlias,'.','')));

    tRowidColumn := SUBSTRING( tTableAlias, 1, pConst.MV_MAX_BASE_TABLE_LEN ) || pConst.UNDERSCORE_CHARACTER ||
                                                                                 pConst.MV_M_ROW$_COLUMN;

    RETURN( tRowidColumn );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$createRow$Column';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  pTableName;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$deconstructSqlStatement
            (
                pConst              IN      mv$allConstants,
                pSqlStatement       IN      TEXT,
                pTableNames           OUT   TEXT,
                pSelectColumns        OUT   TEXT,
                pWhereClause          OUT   TEXT
            )
    RETURNS RECORD
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$deconstructSqlStatement
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
18/06/2019  | M Revitt      | Add an exception handler
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    One of the most difficult tasks with the materialized view fast refresh process is programatically determining
                the columns, base tables and select criteria that have been used to construct the view.

                This function deconstructs the SQL statement that was used to create the materialized view and stores the
                information in the data dictionary tables for future use

Notes:          The technique used here is to search for each of the key words in a SQL statement, FROM, WHERE and replace them
                whith an unprintable character which can be searched for later.

                To locate the keywords they are searched for with acceptable command delimination characters either side of the
                key word, the delimiators currently used are
                o   SPACE
                o   NEW LINE
                o   CARRIAGE RETURN

                The SELECT keyword is assumed to be the leading key word and is simply removed from the string with the use of a
                SUBSTRING command

                Once all of the replacements have been completed it becomes a simple task to extract the necessary information later
                when required

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pSqlStatement       The SQL Statement used to create the materialized view
                OUT 	pTableNames         The name of the materialized view source tables
                                                all text between the FROM and WHERE clauses
                OUT 	pSelectColumns      The list of columns in the SQL Statement used to create the materialized view
                                                all text between the SELECT and FROM clauses
                OUT 	pWhereClause        The where clause from the SQL Statement used to create the materialized view
                                                all text after the WHERE clause
Returns:        RECORD              		The three out parameters
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT := pSqlStatement;

BEGIN
    tSqlStatement := SUBSTRING( tSqlStatement,  POSITION( pConst.SELECT_DML_TYPE IN tSqlStatement ) +
                                                LENGTH(   pConst.SELECT_DML_TYPE ));

    tSqlStatement := TRIM( LEADING pConst.SPACE_CHARACTER FROM tSqlStatement );

    tSqlStatement := mv$replaceCommandWithToken( pConst, tSqlStatement,    pConst.FROM_DML_TYPE,    pConst.FROM_TOKEN  );
    tSqlStatement := mv$replaceCommandWithToken( pConst, tSqlStatement,    pConst.WHERE_DML_TYPE,   pConst.WHERE_TOKEN );

    tSqlStatement := tSqlStatement || pConst.WHERE_TOKEN; -- Append a Where Token incase Where does not appear in the string

    pTableNames   := TRIM(  SUBSTRING( tSqlStatement,
                            POSITION(  pConst.FROM_TOKEN  IN tSqlStatement )  + LENGTH(   pConst.FROM_TOKEN  ),
                            POSITION(  pConst.WHERE_TOKEN IN tSqlStatement )  - LENGTH(   pConst.WHERE_TOKEN )
                                                                              - POSITION( pConst.FROM_TOKEN IN tSqlStatement )));

    pSelectColumns := TRIM( SUBSTRING( tSqlStatement,
                            1,
                            POSITION(  pConst.FROM_TOKEN  IN tSqlStatement )  - LENGTH( pConst.FROM_TOKEN )));

    pWhereClause   := TRIM( SUBSTRING( tSqlStatement,
                            POSITION(  pConst.WHERE_TOKEN IN tSqlStatement )  + LENGTH( pConst.WHERE_TOKEN )));

    IF  LENGTH(   pWhereClause )                        > 0
    AND POSITION( pConst.WHERE_TOKEN IN pWhereClause )  > 0     -- We have to get rid of the appended token
    THEN
        pWhereClause   := TRIM( SUBSTRING( pWhereClause,
                                1,
                                POSITION( pConst.WHERE_TOKEN IN pWhereClause ) - LENGTH( pConst.WHERE_TOKEN )));
    END IF;

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$deconstructSqlStatement';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$deleteMaterializedViewRows
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pViewName       IN      TEXT,
				pDmlType		IN		TEXT,
                pRowidColumn    IN      TEXT,
                pRowIDs         IN      UUID[]
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$deleteMaterializedViewRows
Author:       Mike Revitt
Date:         12/011/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
24/09/2020  | D Day			| Removed previous change done on the 10/09/2020 as still getting this error.
10/09/2020  | D Day         | CDL specific - Removed Parent to Child delete relationship for inner joins to support materialized view
            |               | primary key violation error(s).
12/08/2020	| D Day			| CDL specific - Workaround fix to support materialized views with more than one column primary keys.
29/06/2020	| D Day			| Added new DELETE statement to handle DML Type INSERT when row already exists.
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
07/05/2019  | M Revitt      | Convert to array processing
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Gets called to remove the row from the Materialized View when a delete is detected

Note:           This function was modified to array processing to address some performance concerns found during testing

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pViewName           The name of the underlying table for the materialized view
				IN 		pDmlType			The DML Type of change i.e. DELETE, UPDATE or INSERT.
                IN      pRowidColumn        The MV_M_ROW$_COLUMN for this table in the base table
                IN      pRowIDs             An array holding the unique identifiers to locate the modified row

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   			TEXT;
    aPgMview        			pg$mviews;
	tInnerJoinOtherRowidColumn	TEXT;
	tInnerJoinOtherAlias		TEXT	:= 'none';
	tInnerJoinAlias				TEXT;
	tMultiPrimaryKeyMview		CHAR(1)	:= 'N';
BEGIN

	aPgMview    		 := mv$getPgMviewTableData( pConst, pOwner, pViewName );
	
	SELECT inline.ijo_rowid
	,	   inline.ijo_alias
	,      inline.ij_alias
	FROM (
		SELECT 	UNNEST(aPgMview.inner_join_other_rowid_array) AS ijo_rowid
		,		UNNEST(aPgMview.inner_join_other_alias_array) AS ijo_alias
		,		UNNEST(aPgMview.inner_join_alias_array) AS ij_alias
		,		UNNEST(aPgMview.inner_join_rowid_array) AS ij_rowid) inline
	WHERE inline.ij_rowid = pRowidColumn 
	INTO 	tInnerJoinOtherRowidColumn
	,		tInnerJoinOtherAlias
	,		tInnerJoinAlias;
	
	IF (pDmlType = 'INSERT' AND pViewName = 'mv_instalments_ref' AND tInnerJoinAlias = 'idref.') THEN	
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_mta' AND tInnerJoinAlias = 'be.') THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF ((pDmlType = 'INSERT' AND pViewName = 'mv_named_driver' AND tInnerJoinAlias = 'lcncctgy.') OR (pDmlType = 'INSERT' AND pViewName = 'mv_named_driver' AND tInnerJoinAlias = 'dvnglcnc.')) THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_vehicle_modification' AND tInnerJoinAlias = 'mr.') THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_imtm_employment_activity' AND tInnerJoinAlias = 'a.') THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_imtm_professional_indemn' AND tInnerJoinAlias = 'q.') THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_imtm_prsnal_accident_cover' AND tInnerJoinAlias = 'm.') THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_imtm_travel_addtional_cover' AND tInnerJoinAlias = 'a.') THEN
		tMultiPrimaryKeyMview := 'Y';
	ELSIF (pDmlType = 'INSERT' AND pViewName = 'mv_bstr_addl_cover_options' AND tInnerJoinAlias = 'o.') THEN
		tMultiPrimaryKeyMview := 'Y';		
	END IF;
	
	IF (pDmlType IN ('DELETE','UPDATE') OR (pDmlType IN ('INSERT') AND tInnerJoinOtherAlias = 'none') OR tMultiPrimaryKeyMview = 'Y') THEN

		tSqlStatement :=    pConst.DELETE_FROM || pOwner  || pConst.DOT_CHARACTER   || pViewName        ||
							pConst.WHERE_COMMAND          || pRowidColumn           || pConst.IN_ROWID_LIST;
							
	ELSE
	
		tSqlStatement :=    pConst.DELETE_FROM || pOwner  || pConst.DOT_CHARACTER   || pViewName        ||
							pConst.WHERE_COMMAND          || tInnerJoinOtherRowidColumn || pConst.IN_SELECT_COMMAND	||
							tInnerJoinOtherAlias || pConst.MV_M_ROW$_COLUMN || pConst.FROM_COMMAND || aPgMview.table_names;
							
		IF aPgMview.where_clause != pConst.EMPTY_STRING
		THEN
			tSqlStatement := tSqlStatement || pConst.WHERE_COMMAND || aPgMview.where_clause || pConst.AND_COMMAND;
		ELSE
			tSqlStatement := tSqlStatement || pConst.WHERE_COMMAND;
		END IF;

        tSqlStatement :=  tSqlStatement || tInnerJoinAlias || pConst.MV_M_ROW$_SOURCE_COLUMN || pConst.IN_ROWID_LIST ||
						  pConst.CLOSE_BRACKET;
	END IF;

	EXECUTE tSqlStatement
	USING   pRowIDs;
	
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$deleteMaterializedViewRows';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$deletePgMview
            (
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$deletePgMview
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
04/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Every time a new materialized view is created, a record of that view is also created in the data dictionary table
                pg$mviews.

                This procedure removes that row when a materialized view is removed.

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
BEGIN

    DELETE
    FROM    pg$mviews
    WHERE
            owner       = pOwner
    AND     view_name   = pViewName;

    RETURN;
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$deletePgMview';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$deletePgMviewOjDetails
            (
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$deletePgMviewOjDetails
Author:       David Day
Date:         01/07/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
01/07/2019  | D Day	    	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Every time a new materialized view is created, a record of the outer join table(s) details is also created in the data dictionary table
                pg$mviews_oj_details which is used as part of the outer join source table(s) DELETE process.

                This procedure removes that row(s) when a materialized view is removed.

Arguments:      IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
BEGIN

    DELETE
    FROM    pg$mviews_oj_details
    WHERE
            owner       = pOwner
    AND     view_name   = pViewName;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$deletePgMviewOjDetails';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$deletePgMviewLog
            (
                pOwner      IN      TEXT,
                pTableName  IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$deletePgMviewLog
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
04/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Every time a new materialized view log is created, a record of that log is also created in the data dictionary table
                pg$mview_logs.

                This procedure removes that row when a materialized view log is removed.

Arguments:      IN      pOwner              The owner of the object
                IN      pTableName          The name of the materialized view log
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
BEGIN

    DELETE
    FROM    pg$mview_logs
    WHERE
            owner       = pOwner
    AND     table_name  = pTableName;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$deletePgMviewLog';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$dropTable
            (
                pConst              IN      mv$allConstants,
                pOwner              IN      TEXT,
                pTableName          IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$dropTable
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
04/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Generic function to drop any tables in a Postgres database, used in this context to remove the Materialized View
                and Materialized View Log tables

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pTableName          The name of the table to be dropped

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement   TEXT    := NULL;

BEGIN

    tSqlStatement   :=  pConst.DROP_TABLE || pOwner || pConst.DOT_CHARACTER || pTableName;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$dropTable';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$dropTrigger
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pTriggerName    IN      TEXT,
                pTableName      IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$dropTrigger
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Generic function to drop any trigger in a Postgres database, used in this context to remove the trigger from the
                Materialized View Log tables

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pOwner              The owner of the object
                IN      pTriggerName        The name of the materialized view source trigger
                IN      pTableName          The name of the materialized view source table
				
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;

BEGIN

    tSqlStatement := pConst.TRIGGER_DROP || pTriggerName || pConst.ON_COMMAND || pOwner || pConst.DOT_CHARACTER || pTableName;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$dropTrigger';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$extractCompoundViewTables
            (
                pConst              IN      mv$allConstants,
                pTableNames         IN      TEXT,
                pTableArray           OUT   TEXT[],
                pAliasArray           OUT   TEXT[],
                pRowidArray           OUT   TEXT[],
                pOuterTableArray      OUT   TEXT[],
                pInnerAliasArray      OUT   TEXT[],
                pInnerRowidArray      OUT   TEXT[],
				pOuterLeftAliasArray  OUT	TEXT[],
				pOuterRightAliasArray OUT	TEXT[],
				pLeftOuterJoinArray   OUT	TEXT[],
				pRightOuterJoinArray  OUT	TEXT[],
				pInnerJoinTableNameArray	OUT	TEXT[],
				pInnerJoinTableAliasArray	OUT	TEXT[],
				pInnerJoinTableRowidArray		OUT TEXT[],
				pInnerJoinOtherTableNameArray	OUT	TEXT[],		
				pInnerJoinOtherTableAliasArray	OUT	TEXT[],
				pInnerJoinOtherTableRowidArray	OUT TEXT[],
				pQueryJoinsMultiTabCntArray		OUT SMALLINT[],
				pQueryJoinsMultiTabPosArray		OUT SMALLINT[]
            )
    RETURNS RECORD
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$extractCompoundViewTables
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
23/10/2020	| D Day			| Defect fix - added OUT parameters pQueryJoinsMultiTabCntArray and pQueryJoinsMultiTabPosArray
			|				| populated by calling procedures mv$setQueryJoinsMultiTablePosition and mv$setQueryJoinsMultiTableCount.
			|				| Used to populated data dictionary table pg$mviews to resolve handling clearing mview log bitmap values
			|				| for mview logs being processed more than once within the mview stored query.
29/06/2020	| D Day			| Defect fix - added inner joinings details to resolve duplicate issue when row already exists for
			|				| DML change type INSERT.
12/02/2020	| D Day 		| Defect fix - changed logic to populate OUT parameters pInnerAliasArray and pInnerRowidArray correctly 
			|				| with the parent alias and rowid for the corresponding LEFT and RIGHT outer table joining conditions. This
			|				| is then populated in the data dictionary table pg$mviews columns inner_alias_array and inner_rowid_array
			|				| and used during the INSERT DML changes registered in the outer join mlog table(s). If there is a matching
			|               | rowid populated for the parent on the same row(s) based on the rowid array for the child table the record
			|				| will be DELETED and INSERTED back into the mview table. If there is no parent rowid then this will be treated
			|				| as a new INSERTED row(s).
23/07/2019	| D Day			| Defect fix - added logic to get the LEFT and RIGHT outer join columns joining condition aliases to
			|				| build dynamic UPDATE statement for outer join DELETE changes.
11/07/2019  | D Day         | Defect fix - changed mv$createRow$Column input parameter to use alias array instead of table array
            |               | as this will be used as part of the m_row$ column name used to refresh the materialized view.
18/06/2019  | M Revitt      | Fix a logic bomb with the contruct of the inner table and outer table arrays
            |               | Add an exception handler
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    One of the most difficult tasks with the materialized view fast refresh process is programatically determining
                the columns, base tables and select criteria that have been used to construct the view.

                This function deconstructs the FROM clause that was used to create the materialized view and determines
                o   All of the outer joined tables
                o   All of the inner joined tables
                o   All inner joined table aliases
                o   All
                primary table and a complete list of all tables involved in the query

Notes:          The technique used here is to search for each of the key words in FROM clause, COMMA, RIGHT, ON, JOIN and replace
                them with an unprintable character which can be search for later.

                To locate the keywords they are searched for with acceptable command delimination characters either side of the
                key word, SPACE, NEW LINE, CARRIAGE RETURN

                Once all of the replacements have been completed it becomes a simple task to extract the necessary information

PostGres Notes:
            Qualified joins
                T1 { [INNER] | { LEFT | RIGHT | FULL } [OUTER] } JOIN T2 ON boolean_expression
                T1 { [INNER] | { LEFT | RIGHT | FULL } [OUTER] } JOIN T2 USING ( join column list )
                T1 NATURAL { [INNER] | { LEFT | RIGHT | FULL } [OUTER] } JOIN T2
                The words INNER and OUTER are optional in all forms.
                INNER is the default; LEFT, RIGHT, and FULL imply an outer join.

                The join condition is specified in the ON or USING clause, or implicitly by the word NATURAL. The join condition
                determines which rows from the two source tables are considered to “match”, as explained in detail below.

                The possible types of qualified join are:

            INNER JOIN
                For each row R1 of T1, the joined table has a row for each row in T2 that satisfies the join condition with R1.

            LEFT OUTER JOIN
                First, an inner join is performed. Then, for each row in T1 that does not satisfy the join condition with any row
                in T2, a joined row is added with null values in columns of T2. Thus, the joined table always has at least one row
                for each row in T1.

            RIGHT OUTER JOIN
                First, an inner join is performed. Then, for each row in T2 that does not satisfy the join condition with any row
                in T1, a joined row is added with null values in columns of T1. This is the converse of a left join: the
                result table will always have a row for each row in T2.

            FULL OUTER JOIN
                First, an inner join is performed. Then, for each row in T1 that does not satisfy the join condition with any row
                in T2, a joined row is added with null values in columns of T2. Also, for each row of T2 that does not satisfy the
                join condition with any row in T1, a joined row with null values in the columns of T1 is added.

            ON CLAUSE
                The ON clause is the most general kind of join condition: it takes a Boolean value expression of the same kind as
                is used in a WHERE clause. A pair of rows from T1 and T2 match if the ON expression evaluates to true.

            USING CLAUSE
                The USING clause is a shorthand that allows you to take advantage of the specific situation where both sides of the
                join use the same name for the joining column(s). It takes a comma-separated list of the shared column names and
                forms a join condition that includes an equality comparison for each one. For example, joining T1 and T2 with USING
                (a, b) produces the join condition ON T1.a = T2.a AND T1.b = T2.b.

                Furthermore, the output of JOIN USING suppresses redundant columns: there is no need to print both of the matched
                columns, since they must have equal values. While JOIN ON produces all columns from T1 followed by all columns
                from T2, JOIN USING produces one output column for each of the listed column pairs (in the listed order), followed
                by any remaining columns from T1, followed by any remaining columns from T2.

                Finally, NATURAL is a shorthand form of USING: it forms a USING list consisting of all column names that appear in
                both input tables. As with USING, these columns appear only once in the output table. If there are no common
                column names, NATURAL JOIN behaves like JOIN ... ON TRUE, producing a cross-product join.

Arguments:      IN      pConst              	The memory structure containing all constants
                IN      pTableNames       		The materialized view query SQL statement taken from the position of the FROM clause including all source tables and joins to be
												used for the OUT parameters logic
                OUT 	pTableArray				An array containing the source tables 
				OUT		pAliasArray				An array containing the table aliases			
				OUT		pRowidArray				An array containing the m_row$ column names
				OUT		pOuterTableArray		An array containing the outer join source tables
				OUT		pInnerAliasArray		An array containing the inner join table aliases
				OUT		pInnerRowidArray		An array containing the inner table m_row$ column aliases
				OUT		pOuterLeftAliasArray	An array containing the left outer join column name joining condition aliases  
				OUT		pOuterRightAliasArray	An array containing the right outer join column name joining condition aliases  
				OUT		pLeftOuterJoinArray		An array confirming whether the source table is from a left outer join condition
				OUT		pRightOuterJoinArray	An array confirming whether the source table is from a right outer join condition
				OUT		pInnerJoinTableNameArray	An array confirming the INNER JOIN table names
				OUT		pInnerJoinTableAliasArray	An array confirming the INNER JOIN table aliases
				OUT		pInnerJoinTableRowidArray	An array confirming the INNER JOIN table rowid column names
				OUT		pInnerJoinOtherTableNameArray	An array confirming the INNER JOIN other joining table names		
				OUT		pInnerJoinOtherTableAliasArray	An array confirming the INNER JOIN other joining table aliases 
				OUT		pInnerJoinOtherTableRowidArray	An array confirming the INNER JOIN other joining table rowid column names
				OUT     pQueryJoinsMultiTabCntArray		An array containing the materialized view query joins multi table counts for each table alias if the table exists once or more
				OUT     pQueryJoinsMultiTabPosArray		An array containing the materialized view query joins multi table position when table occurs once or more linked to each table alias
					
Returns:                RECORD              The ten out parameters
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tOuterTable     TEXT    := NULL;
    tInnerAlias     TEXT    := pConst.NO_INNER_TOKEN;
    tInnerRowid     TEXT    := pConst.NO_INNER_TOKEN;
    tTableName      TEXT;
    tTableNames     TEXT;
    tTableAlias     TEXT;
    iTableArryPos   INTEGER := pConst.ARRAY_LOWER_VALUE;
	
	tOuterLeftAlias  TEXT;
	tOuterRightAlias TEXT;
	tLeftOuterJoin 	 TEXT;
	tRightOuterJoin  TEXT;
	
	tInnerLeftAlias  			TEXT;
	tInnerRightAlias 			TEXT;
	tInnerJoinTableAlias		TEXT;
	tInnerJoinTableName			TEXT;
	tInnerJoinTableRowid		TEXT;
	tInnerJoinOtherTableAlias	TEXT;
	tInnerJoinOtherTableName	TEXT;
	tInnerJoinOtherTableRowid	TEXT;
	
	tInnerJoin					CHAR(1);
	
	iLoopCounter				INTEGER := 0;
	iJoinCount					INTEGER := 0;

BEGIN
--  Replacing a single space with a double space is only required on the first pass to ensure that there is padding around all
--  special commands so we can find then in the future replace statments
    tTableNames :=  REPLACE(                            pTableNames, pConst.SPACE_CHARACTER,  pConst.DOUBLE_SPACE_CHARACTERS );
    tTableNames :=  mv$replaceCommandWithToken( pConst, tTableNames, pConst.JOIN_DML_TYPE,    pConst.JOIN_TOKEN );
    tTableNames :=  mv$replaceCommandWithToken( pConst, tTableNames, pConst.ON_DML_TYPE,      pConst.ON_TOKEN );
    tTableNames :=  mv$replaceCommandWithToken( pConst, tTableNames, pConst.OUTER_DML_TYPE,   pConst.OUTER_TOKEN );
    tTableNames :=  mv$replaceCommandWithToken( pConst, tTableNames, pConst.INNER_DML_TYPE,   pConst.COMMA_CHARACTER );
    tTableNames :=  mv$replaceCommandWithToken( pConst, tTableNames, pConst.LEFT_DML_TYPE,    pConst.COMMA_LEFT_TOKEN );
    tTableNames :=  mv$replaceCommandWithToken( pConst, tTableNames, pConst.RIGHT_DML_TYPE,   pConst.COMMA_RIGHT_TOKEN );
    tTableNames :=  REPLACE( REPLACE(                   tTableNames, pConst.NEW_LINE,         pConst.EMPTY_STRING ),
                                                                     pConst.CARRIAGE_RETURN,  pConst.EMPTY_STRING );

    tTableNames :=  tTableNames || pConst.COMMA_CHARACTER; -- A trailling comma is required so we can detect the final table

    WHILE POSITION( pConst.COMMA_CHARACTER IN tTableNames ) > 0
    LOOP
	
		iLoopCounter := iLoopCounter + 1;
		
		tOuterLeftAlias  := NULL;
		tOuterRightAlias := NULL;
		tLeftOuterJoin 	 := NULL;
		tRightOuterJoin  := NULL;
        tOuterTable := NULL;
        tInnerAlias := NULL;
        tInnerRowid := NULL;
		
		tInnerLeftAlias  := NULL;
		tInnerRightAlias := NULL;
		tInnerJoinTableName	 := NULL;
		tInnerJoinTableAlias := NULL;
		tInnerJoinOtherTableName  := NULL;
		tInnerJoinOtherTableAlias := NULL;
		tInnerJoinTableRowid 	  := NULL;
		tInnerJoinOtherTableRowid := NULL;
		tInnerJoin	:= 'N';

        tTableName := LTRIM( SPLIT_PART( tTableNames, pConst.COMMA_CHARACTER, 1 ));

        IF POSITION( pConst.RIGHT_TOKEN IN tTableName ) > 0
        THEN
            tOuterTable := pAliasArray[iTableArryPos - 1];  -- There has to be a table preceeding a right outer join
			
			tOuterLeftAlias := TRIM(SUBSTRING(tTableName,POSITION( pConst.ON_TOKEN IN tTableName)+2,(mv$regExpInstr(tTableName,'\.',1,1))-(POSITION( pConst.ON_TOKEN IN tTableName)+2)));
			tOuterRightAlias := TRIM(SUBSTRING(tTableName,POSITION( TRIM(pConst.EQUALS_COMMAND) IN tTableName)+1,(mv$regExpInstr(tTableName,'\.',1,2))-(POSITION( TRIM(pConst.EQUALS_COMMAND) IN tTableName)+1)));
			tRightOuterJoin := pConst.RIGHT_OUTER_JOIN;
			
			tInnerAlias := tOuterRightAlias;
			tInnerRowid := TRIM(REPLACE(REPLACE(tOuterRightAlias,'.','')||pConst.UNDERSCORE_CHARACTER||pConst.MV_M_ROW$_SOURCE_COLUMN,'"',''));
			

        ELSIF POSITION( pConst.LEFT_TOKEN IN tTableName ) > 0   -- There has to be a table preceeding a left outer join
        THEN
            tOuterTable := TRIM( SUBSTRING( tTableName,
                                            POSITION( pConst.JOIN_TOKEN   IN tTableName ) + LENGTH( pConst.JOIN_TOKEN),
                                            POSITION( pConst.ON_TOKEN     IN tTableName ) - LENGTH( pConst.ON_TOKEN)
                                            - POSITION( pConst.JOIN_TOKEN IN tTableName )));	
			
			tOuterLeftAlias := TRIM(SUBSTRING(tTableName,POSITION( pConst.ON_TOKEN IN tTableName)+2,(mv$regExpInstr(tTableName,'\.',1,1))-(POSITION( pConst.ON_TOKEN IN tTableName)+2)));	
			tOuterRightAlias := TRIM(SUBSTRING(tTableName,POSITION( TRIM(pConst.EQUALS_COMMAND) IN tTableName)+1,(mv$regExpInstr(tTableName,'\.',1,2))-(POSITION( TRIM(pConst.EQUALS_COMMAND) IN tTableName)+1)));
			tLeftOuterJoin := pConst.LEFT_OUTER_JOIN;
			
            tInnerAlias := tOuterLeftAlias;
			tInnerRowid := TRIM(REPLACE(REPLACE(tOuterLeftAlias,'.','')||pConst.UNDERSCORE_CHARACTER||pConst.MV_M_ROW$_SOURCE_COLUMN,'"',''));
			
		ELSIF POSITION( pConst.JOIN_TOKEN IN tTableName ) > 0
		THEN	
			tInnerLeftAlias		:= TRIM(SUBSTRING(tTableName,POSITION( pConst.ON_TOKEN IN tTableName)+2,(mv$regExpInstr(tTableName,'\.',1,1))-(POSITION( pConst.ON_TOKEN IN tTableName)+2))) || pConst.DOT_CHARACTER;	
			tInnerRightAlias 	:= TRIM(SUBSTRING(tTableName,POSITION( TRIM(pConst.EQUALS_COMMAND) IN tTableName)+1,(mv$regExpInstr(tTableName,'\.',1,2))-(POSITION( TRIM(pConst.EQUALS_COMMAND) IN tTableName)+1))) || pConst.DOT_CHARACTER;
			tInnerJoin			:= 'Y';
			
		END IF;

        -- The LEFT, RIGHT and JOIN tokens are only required for outer join pattern matching
        tTableName  := REPLACE( tTableName, pConst.JOIN_TOKEN,  pConst.EMPTY_STRING );
        tTableName  := REPLACE( tTableName, pConst.LEFT_TOKEN,  pConst.EMPTY_STRING );
        tTableName  := REPLACE( tTableName, pConst.RIGHT_TOKEN, pConst.EMPTY_STRING );
        tTableName  := REPLACE( tTableName, pConst.OUTER_TOKEN, pConst.EMPTY_STRING );
        tTableName  := LTRIM(   tTableName );

        pTableArray[iTableArryPos]  := (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[1];
        tTableAlias                 := (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[2];
        pAliasArray[iTableArryPos]  :=  COALESCE( NULLIF( NULLIF( tTableAlias, pConst.EMPTY_STRING), pConst.ON_TOKEN),
                                                                  pTableArray[iTableArryPos] ) || pConst.DOT_CHARACTER;
		pRowidArray[iTableArryPos]  :=  mv$createRow$Column( pConst, pAliasArray[iTableArryPos] );
		
		IF tInnerJoin = 'Y' THEN
		
			tInnerJoinTableName		:= (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[1];
			tInnerJoinTableAlias    := (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[2];
			tInnerJoinTableAlias  	:=  COALESCE( NULLIF( NULLIF( tInnerJoinTableAlias, pConst.EMPTY_STRING), pConst.ON_TOKEN),
																	  pTableArray[iTableArryPos] ) || pConst.DOT_CHARACTER;
																	  
			SELECT (CASE WHEN tInnerJoinTableAlias = tInnerLeftAlias THEN tInnerRightAlias
					ELSE tInnerLeftAlias END) INTO tInnerJoinOtherTableAlias;
					
			SELECT inline.table_name
			FROM (
					SELECT 	UNNEST(pTableArray) AS table_name
					,		UNNEST(pAliasArray) AS table_alias) inline
			WHERE inline.table_alias = tInnerJoinOtherTableAlias
			INTO tInnerJoinOtherTableName;
			
			tInnerJoinTableRowid	:= mv$createRow$Column( pConst, tInnerJoinTableAlias );
			tInnerJoinOtherTableRowid	:= mv$createRow$Column( pConst, tInnerJoinOtherTableAlias );
					
		END IF;
		
		SELECT count(1) INTO iJoinCount FROM regexp_matches(pTableNames,pConst.JOIN_TOKEN,'g');
		
		IF iLoopCounter = 1 AND iJoinCount = 0 THEN
		
			tInnerJoinTableName			:= (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[1];
			tInnerJoinTableAlias    	:= (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[2];
			tInnerJoinTableAlias  		:=  COALESCE( NULLIF( NULLIF( tInnerJoinTableAlias, pConst.EMPTY_STRING), pConst.ON_TOKEN),
																	  pTableArray[iTableArryPos] ) || pConst.DOT_CHARACTER;
			tInnerJoinTableRowid		:= mv$createRow$Column( pConst, tInnerJoinTableAlias );
			
			tInnerJoinOtherTableName 	:= 'none';
			tInnerJoinOtherTableRowid	:= 'none';
			tInnerJoinOtherTableAlias	:= 'none';

		END IF;
			

        pOuterTableArray[iTableArryPos]  :=(REGEXP_SPLIT_TO_ARRAY( tOuterTable, pConst.REGEX_MULTIPLE_SPACES ))[1];

        tTableNames     := TRIM( SUBSTRING( tTableNames,
                                 POSITION( pConst.COMMA_CHARACTER IN tTableNames ) + LENGTH( pConst.COMMA_CHARACTER )));
								 
		pInnerAliasArray[iTableArryPos] 		:= tInnerAlias;
		pInnerRowidArray[iTableArryPos]			:= tInnerRowid;
		
		pOuterLeftAliasArray[iTableArryPos] 	:= tOuterLeftAlias;
		pOuterRightAliasArray[iTableArryPos] 	:= tOuterRightAlias;
		pLeftOuterJoinArray[iTableArryPos] 		:= tLeftOuterJoin;
		pRightOuterJoinArray[iTableArryPos] 	:= tRightOuterJoin;
		
		pInnerJoinTableNameArray[iTableArryPos]  := tInnerJoinTableName;
		pInnerJoinTableAliasArray[iTableArryPos] := tInnerJoinTableAlias;
		pInnerJoinTableRowidArray[iTableArryPos] := tInnerJoinTableRowid;	
		pInnerJoinOtherTableNameArray[iTableArryPos] := tInnerJoinOtherTableName;		
		pInnerJoinOtherTableAliasArray[iTableArryPos] := tInnerJoinOtherTableAlias;
		pInnerJoinOtherTableRowidArray[iTableArryPos] := tInnerJoinOtherTableRowid;			
		
        iTableArryPos   := iTableArryPos + 1;

    END LOOP;
	
	CALL mv$setQueryJoinsMultiTablePosition(pTableArray,pInnerAliasArray,pQueryJoinsMultiTabPosArray);
	CALL mv$setQueryJoinsMultiTableCount(pTableArray,pInnerAliasArray,pQueryJoinsMultiTabCntArray);	

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$extractCompoundViewTables';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tTableNames;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$findFirstFreeBit
            (
                pConst      IN      mv$allConstants,
                pBitMap     IN      BIGINT[]
            )
    RETURNS mv$bitValue
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$findFirstFreeBit
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
30/10/2019  | M Revitt      | Changed return value to mv$bitValue and pBitMap to BIGINT[] to accomodate more than 62 MV's per table
08/10/2019  | D DAY			| Change returns type from INTEGER to SMALLINT to match the bit data type.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    When a new materialized view is registered against a base table, it is assigned a unique bit against which all
                interest is registered.

                The bit that is assigned is the lowest value bit that has not yet been assigned, as long as that balue is lower
                then the maximum number of pg$mviews per table

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pBitMap[]           The bit map value constructed from assigned bits
Returns:                mv$bitValue         A record containing the next free bit, the array row it is in and it's map
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    iBit                SMALLINT    := pConst.FIRST_PGMVIEW_BIT;
    iRowBit             SMALLINT    := pConst.FIRST_PGMVIEW_BIT;
    iBitRow             SMALLINT    := pConst.ARRAY_LOWER_VALUE;
    iBitValue           mv$bitValue;

BEGIN

    WHILE ( pBitMap[iBitRow] & POWER( pConst.BASE_TWO, iRowBit )::BIGINT ) <> pConst.BITMAP_NOT_SET
    AND     pConst.MAX_PGMVIEWS_PER_TABLE   >= iBit
    LOOP
        IF pConst.FIRST_PGMVIEW_BIT < iRowBit -- Only increment the row if this is not the first loop
        THEN
            iBitRow := iBitRow + 1;
        END IF;
        
        iRowBit := pConst.FIRST_PGMVIEW_BIT;
        
        WHILE ( pBitMap[iBitRow] & POWER( pConst.BASE_TWO, iRowBit )::BIGINT ) <> pConst.BITMAP_NOT_SET
        AND     pConst.MAX_PGMVIEWS_PER_ROW >= iRowBit
        LOOP
            iRowBit := iRowBit + 1;
            iBit    := iBit    + 1;
        END LOOP;
        
        IF pConst.MAX_PGMVIEWS_PER_ROW < iRowBit
        THEN
            iBitRow := iBitRow + 1;
            iRowBit := pConst.FIRST_PGMVIEW_BIT;
            
            IF pBitMap[iBitRow] IS NULL
            THEN
                pBitMap[iBitRow] := pConst.BITMAP_NOT_SET;
            END IF;
        END IF;
    END LOOP;
    
    IF pConst.MAX_PGMVIEWS_PER_TABLE < iBit
    THEN
        RAISE EXCEPTION 'Maximum number of pg$mviews (%s) for table exceeded', pConst.MAX_PGMVIEWS_PER_TABLE;
    ELSE
        iBitValue.BIT_VALUE := iBit;
        iBitValue.BIT_ROW   := iBitRow;
        iBitValue.ROW_BIT   := iRowBit;
        iBitValue.BIT_MAP   := POWER(  pConst.BASE_TWO, iBitValue.ROW_BIT );

        RETURN( iBitValue );
    END IF;
    
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$findFirstFreeBit';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getBitValue
            (
                pConst  IN      mv$allConstants,
                pBit    IN      SMALLINT
            )
    RETURNS mv$bitValue
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getBitValue
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
15/01/2020  | M Revitt      | Need to decrement the Bit Value to allow for the bitmap offset, tables start at 1 bits at 0
30/10/2019  | M Revitt      | Modified to populate the BitValue record type to accomodate > 63 MV's per Table
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Converts a bit into it's binary value.

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pBit                The bit
Returns:                mv$bitValue         The record containing all the pertinant bit information
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    iBitValue   mv$bitValue;
    
BEGIN

    iBitValue.BIT_VALUE := pBit;
    iBitValue.BIT_ROW   := FLOOR( iBitValue.BIT_VALUE / ( pConst.BITMAP_OFFSET + pConst.MAX_PGMVIEWS_PER_ROW )) + pConst.ARRAY_LOWER_VALUE;
    iBitValue.ROW_BIT   := MOD(   iBitValue.BIT_VALUE,  ( pConst.BITMAP_OFFSET + pConst.MAX_PGMVIEWS_PER_ROW ));
    iBitValue.BIT_MAP   := POWER( pConst.BASE_TWO, iBitValue.ROW_BIT );
    
    RETURN( iBitValue );
    
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getBitValue';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getPgMviewLogTableData
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pTableName  IN      TEXT
            )
    RETURNS pg$mview_logs
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getPgMviewLogTableData
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
Description:    Returns all of the data stored in the data dictionary about this materialized view log.

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pPgLog$Name         The name of the materialized view log table
Returns:                RECORD              The row of data from the data dictionary relating to this materialized view log
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    aPgMviewLog            pg$mview_logs;

    cgetPgMviewLogTableData    CURSOR
    FOR
    SELECT
            *
    FROM    pg$mview_logs
    WHERE   owner       = pOwner
    AND     table_name  = pTableName;

BEGIN
    OPEN    cgetPgMviewLogTableData;
    FETCH   cgetPgMviewLogTableData
    INTO    aPgMviewLog;
    CLOSE   cgetPgMviewLogTableData;

    IF aPgMviewLog.table_name IS NULL
    THEN
        RAISE EXCEPTION 'Materialised View ''%'' does not have a PgMview Log', pOwner || pConst.DOT_CHARACTER || pTableName;
    ELSE
        RETURN( aPgMviewLog );
    END IF;
    
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getPgMviewLogTableData';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getPgMviewLogTableData
            (
                pConst      IN      mv$allConstants,
                pTableName  IN      TEXT
            )
    RETURNS pg$mview_logs
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getPgMviewLogTableData
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
Description:    Returns all of the data stored in the data dictionary about this materialized view log.

Note:           This function is used when the table owner is not know
                This function also requires the SEARCH_PATH to be set to the current value so that the select statement can find
                the source tables.
                The default for PostGres functions is to not use the search path when executing with the privileges of the creator


Arguments:      IN      pConst              The memory structure containing all constants
				IN      pPgLog$Name         The name of the materialized view log table
Returns:                RECORD              The row of data from the data dictionary relating to this materialized view log
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tOwner      TEXT    := NULL;

BEGIN

    tOwner  := mv$getSourceTableSchema( pConst, pTableName );

    RETURN( mv$getPgMviewLogTableData( pConst, tOwner, pTableName ));

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getPgMviewLogTableData';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getPgMviewTableData
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS pg$mviews
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getPgMviewTableData
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
24/07/2019  | D Day         | Added COALESCE function to replace null with 0 as this was not raising expection if materialized view
			|				| does not exist.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Returns all of the data stored in the data dictionary about this materialized view.

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                RECORD              The row of data from the data dictionary relating to this materialized view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    aPgMview           pg$mviews;

    cgetPgMviewTableData   CURSOR
    FOR
    SELECT
            *
    FROM    pg$mviews
    WHERE   owner       = pOwner
    AND     view_name   = pViewName;
BEGIN
    OPEN    cgetPgMviewTableData;
    FETCH   cgetPgMviewTableData
    INTO    aPgMview;
    CLOSE   cgetPgMviewTableData;

    IF 0 = COALESCE( CARDINALITY( aPgMview.table_array ), 0 )
    THEN
        RAISE EXCEPTION 'Materialised View ''%'' does not have a base table', pOwner || pConst.DOT_CHARACTER || pViewName;
    ELSE
        RETURN( aPgMview );
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getPgMviewTableData';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getPgMviewOjDetailsTableData
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
				pTableAlias IN      TEXT
            )
    RETURNS pg$mviews_oj_details
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getPgMviewOjDetailsTableData
Author:       David Day
Date:         28/04/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
28/04/2020  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Returns all of the data stored in the data dictionary about this materialized view outer join table alias details.

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
				IN		pTableAlias			The outer join table alias of the materialized view
Returns:                RECORD              The row of data from the data dictionary relating to this materialized view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    aPgMviewOjDetails           pg$mviews_oj_details;
	
	iAliasExists				INTEGER := 0;

    cgetPgMviewOjDetailTableData   CURSOR
    FOR
    SELECT
            *
    FROM    pg$mviews_oj_details
    WHERE   owner       = pOwner
    AND     view_name   = pViewName
	AND 	table_alias = pTableAlias;
BEGIN

    OPEN    cgetPgMviewOjDetailTableData;
    FETCH   cgetPgMviewOjDetailTableData
    INTO    aPgMviewOjDetails;
    CLOSE   cgetPgMviewOjDetailTableData;
	
	SELECT count( aPgMviewOjDetails.table_alias ) INTO iAliasExists;

	IF iAliasExists = 0
    THEN
        RAISE EXCEPTION 'Materialised View ''%'' does not have a alias % table', pOwner || pConst.DOT_CHARACTER || pViewName, pTableAlias;
    ELSE
        RETURN( aPgMviewOjDetails );
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getPgMviewOjDetailsTableData';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getPgMviewViewColumns
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getPgMviewViewColumns
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
Description:    The easiest way to get the names of the columns that have been created, it is possible that the select statement
                used aliases, is to extract them from the data dictionary table after creation. Which is what I am doing here.

Notes:          Because the final column is always the ROWID column, we add that manually at the end

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pViewName           The name of the materialized view
Returns:                TEXT                A comma delimited string of the column names in the materialized view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tColumnNames            TEXT    := '';  -- Has to be initialised to work in loop
    rPgMviewColumnNames     RECORD;

BEGIN

    FOR rPgMviewColumnNames
    IN
        SELECT
                column_name
        FROM    information_schema.columns
        WHERE   table_schema    = LOWER( pOwner )
        AND     table_name      = LOWER( pViewName )
    LOOP
        tColumnNames := tColumnNames || rPgMviewColumnNames.column_name || pConst.COMMA_CHARACTER;
    END LOOP;

    IF tColumnNames IS NULL
    THEN
        RAISE EXCEPTION 'Materialised View ''%'' does not have any columns', pOwner || pConst.DOT_CHARACTER || pViewName;
    ELSE
        tColumnNames   := LEFT( tColumnNames,  LENGTH( tColumnNames  ) - 1 );  -- Remove trailing comma
        RETURN( tColumnNames );
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getPgMviewViewColumns';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getSourceTableSchema
            (
                pConst      IN      mv$allConstants,
                pTableName  IN      TEXT
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getSourceTableSchema
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
21/02/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Looks down the search path to determine which schema is being used to locate the table.

Note:           This function also requires the SEARCH_PATH to be set to the current value so that the select statement can find
                the source tables.
                The default for PostGres functions is to not use the search path when executing with the privileges of the creator

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pTableName          The name of the table we are trying to locate
Returns:                TEXT                The name of the schema where the table was located
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tOwner      TEXT    := NULL;
    tTableList  TEXT;
    tSearchPath TEXT[];

    cGetOwner   CURSOR( cTableName TEXT, cSchemaName TEXT )
    FOR
        SELECT
                table_schema
        FROM
                information_schema.tables
        WHERE
                table_name      = cTableName
        AND     table_schema    = cSchemaName;
BEGIN

    tTableList  :=  CURRENT_SCHEMAS( FALSE );
    tTableList  :=  REPLACE( REPLACE( REPLACE( tTableList,
                    pConst.LEFT_BRACE_CHARACTER,      pConst.EMPTY_STRING),
                    pConst.RIGHT_BRACE_CHARACTER,     pConst.EMPTY_STRING),
                    pConst.COMMA_CHARACTER,           pConst.SPACE_CHARACTER);
    tSearchPath :=  REGEXP_SPLIT_TO_ARRAY( tTableList,  pConst.REGEX_MULTIPLE_SPACES);

    FOR i IN array_lower( tSearchPath, 1 ) .. array_upper( tSearchPath, 1 )
    LOOP
        IF  tOwner IS NULL
        THEN
            OPEN    cGetOwner( pTableName, tSearchPath[i] );
            FETCH   cGetOwner
            INTO    tOwner;
            CLOSE   cGetOwner;
        END IF;
    END LOOP;

    IF tOwner IS NULL
    THEN
        RAISE INFO      'Exception in function mv$getSourceTableSchema';
        RAISE EXCEPTION 'Table ''%'' can not be located in the search path', pTableName;
    ELSE
        RETURN( tOwner );
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getSourceTableSchema';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$grantSelectPrivileges
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pObjectName     IN      TEXT,
				pParallel		IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$grantSelectPrivileges
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
23/07/2021	| D Day			| Added logic to support running build in parallel.
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Whilst objects are created into the named schema, the ownership remains with the package owner, mike_pgmview, so in
                order to allow other users to access these materialized views it is necessary to grant select privileges to the
                default role 'PGMV_ROLE_NAME'

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pObjectName         The name of the object to receive select privileges
				IN		pParallel			Optional, build in parallel
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;
	ret			  TEXT;

BEGIN

    tSqlStatement :=    pConst.GRANT_SELECT_ON    || pOwner   || pConst.DOT_CHARACTER   || pObjectName  ||
                        pConst.TO_COMMAND                     || pConst.PGMV_SELECT_ROLE;

	IF pParallel = 'Y' THEN
	
		PERFORM * FROM dblink('pgmv$_instance',tSqlStatement) AS p (ret TEXT);
	
	ELSE

		EXECUTE tSqlStatement;
		
	END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$grantSelectPrivileges';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$insertPgMviewLogs
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pPgLog$Name     IN      TEXT,
                pTableName      IN      TEXT,
                pTriggerName    IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertPgMviewLogs
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    inserts the row into the materialized view log data dictionary table

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pPgLog$Name         The name of the materialized view log table
                IN      pTableName          The name of the materialized view source table
                IN      pMvSequenceName     The name of the materialized view sequence

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
BEGIN

    INSERT  INTO
            pg$mview_logs
            (
                owner,  pglog$_name, table_name, trigger_name
            )
    VALUES  (
                pOwner, pPgLog$Name, pTableName, pTriggerName
            );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$insertPgMviewLogs';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$removeRow$FromSourceTable
            (
                pConst          IN      mv$allConstants,
                pOwner          IN      TEXT,
                pTableName      IN      TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$removeRow$FromSourceTable
Author:       Mike Revitt
Date:         04/06/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
04/06/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    PostGre does not have a ROWID pseudo column and so a ROWID column has to be added to the source table, ideally this
                should be a hidden column, but I can't find any way of doing this

Arguments:      IN      pConst              The memory structure containing all constants
				IN      pOwner              The owner of the object
                IN      pTableName          The name of the materialized view source table
Returns:                VOID
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;

BEGIN

    tSqlStatement := pConst.ALTER_TABLE || pOwner || pConst.DOT_CHARACTER || pTableName || pConst.DROP_M_ROW$_COLUMN_FROM_TABLE;

    EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$removeRow$FromSourceTable';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$replaceCommandWithToken
            (
                pConst          IN      mv$allConstants,
                pSearchString   IN      TEXT,
                pSearchValue    IN      TEXT,
                pTokan          IN      TEXT
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$replaceCommandWithToken
Author:       Mike Revitt
Date:         15/01/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------

10/09/2019  | D Day         | Change code to handle tab characters
15/01/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    A huge amount of coding in this program is to locate specific key words within text strings. This is largely a
                repetative process which I have now moved to a common function


Arguments:      IN      pConst              The memory structure containing all constants
				IN      pSearchString       The string to be searched
                IN      pSearchValue        The value to look for in the string
                IN      pTokan              The value to replace the search value with within the string
Returns:                TEXT                The tokanised string
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tTokanisedString    TEXT;

BEGIN

 tTokanisedString :=
        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(
														REPLACE( REPLACE( REPLACE(
        pSearchString,
        pConst.SPACE_CHARACTER || pSearchValue  || pConst.SPACE_CHARACTER,   pTokan ),
        pConst.SPACE_CHARACTER || pSearchValue  || pConst.NEW_LINE,          pTokan ),
        pConst.SPACE_CHARACTER || pSearchValue  || pConst.CARRIAGE_RETURN,   pTokan ),
        pConst.SPACE_CHARACTER || pSearchValue  || pConst.TAB_CHARACTER,   	 pTokan ),
        pConst.NEW_LINE        || pSearchValue  || pConst.SPACE_CHARACTER,   pTokan ),
        pConst.NEW_LINE        || pSearchValue  || pConst.NEW_LINE,          pTokan ),
        pConst.NEW_LINE        || pSearchValue  || pConst.CARRIAGE_RETURN,   pTokan ),
        pConst.NEW_LINE 	   || pSearchValue  || pConst.TAB_CHARACTER,   	 pTokan ),
        pConst.CARRIAGE_RETURN || pSearchValue  || pConst.SPACE_CHARACTER,   pTokan ),
        pConst.CARRIAGE_RETURN || pSearchValue  || pConst.NEW_LINE,          pTokan ),
        pConst.CARRIAGE_RETURN || pSearchValue  || pConst.CARRIAGE_RETURN,   pTokan ),
        pConst.CARRIAGE_RETURN || pSearchValue  || pConst.TAB_CHARACTER,   	 pTokan ),
		pConst.TAB_CHARACTER   || pSearchValue  || pConst.SPACE_CHARACTER,   pTokan ),
		pConst.TAB_CHARACTER   || pSearchValue  || pConst.NEW_LINE,          pTokan ),
		pConst.TAB_CHARACTER   || pSearchValue  || pConst.CARRIAGE_RETURN,   pTokan ),
		pConst.TAB_CHARACTER   || pSearchValue  || pConst.TAB_CHARACTER,   	 pTokan );

    RETURN( tTokanisedString );

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$replaceCommandWithToken';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$truncateMaterializedView
            (
                pConst      IN      mv$allConstants,
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
				pParallel 	IN 		TEXT
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$truncateMaterializedView
Author:       Mike Revitt
Date:         11/03/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
23/07/2021	| D Day			| Added parallel running logic into truncate as this needs committing in the background due to locking
			|				| the cron jobs for the split insert sessions.
05/06/2020  | D Day         | Change functions with RETURNS VOID to procedures allowing support/control of COMMITS during refresh process.
11/03/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    When performing a full refresh, we first have to truncate the materialized view

Arguments:      IN      pConst             The memory structure containing all constants
				IN      pOwner             The owner of the object
                IN      pViewName          The name of the materialized view base table
				IN 		pParallel		   Build of materialized view set to run in Parallel
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement TEXT;
	ret			  TEXT;

BEGIN

	tSqlStatement := pConst.TRUNCATE_TABLE || pOwner || pConst.DOT_CHARACTER || pViewName;

	IF pParallel = 'Y' THEN
							   
		PERFORM * FROM dblink('pgmv$_instance',tSqlStatement) AS p (ret TEXT);
	
	ELSE

		EXECUTE tSqlStatement;
		
	END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$truncateMaterializedView';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;

------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$createindexestemptable
            (
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
				pParallel 	IN 		TEXT	
            )
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$createindexestemptable
Author:       Jack Bills
Date:         13/08/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
17/08/2021	| D Day			| Added logic to create temp table via dblink to support parallel rollback to not loose the indexes
			|				| list that needs recreating as part of the complete refresh process.
13/08/2020  | J Bills       | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This will store the create indexes SQL in a temporary table for when its removed from the pg_indexes database table

Arguments:      IN      pOwner             The owner of the object
                IN      pViewName          The name of the materialized view base table
				IN		pParallel		   Optional, build in parallel
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/

DECLARE
  i RECORD;
  tSqlStatement 	TEXT;
  tTempTableName	TEXT;
BEGIN
  FOR i IN 
    (
		select distinct tablename , schemaname
from pg_indexes 
where tablename = pViewName
and schemaname = pOwner
	)
  LOOP

tSqlStatement := 'CREATE TEMP TABLE temp' ||'_'|| i.tablename ||' AS select indexdef from pg_indexes 
where indexname  NOT IN ((SELECT  distinct i.relname
                      FROM
                          pg_class t,
                          pg_class i,
                          pg_index ix,
                          pg_attribute a,
                          pg_namespace s
                      WHERE
                          t.oid = ix.indrelid
                          AND i.oid = ix.indexrelid
                          AND t.relnamespace = s.oid
                          AND a.attrelid = t.oid
                          AND a.attnum = ANY(ix.indkey)
                          AND t.relkind = ''r''
                          AND s.nspname =  '|| ''''|| i.schemaname ||''''||'
                          AND t.relname =  '||''''||  i.tablename ||''''||' 
                          AND    ix.indisprimary))  
and tablename = '|| ''''|| i.tablename ||''''||' and schemaname =  '||''''||  i.schemaname ||''''||' 	
 ';
 
IF pParallel = 'Y' THEN

	tTempTableName := 'temp_'|| i.tablename;

	IF NOT EXISTS (
	  SELECT
	  FROM   pg_tables
	  WHERE  tablename = tTempTableName) THEN

		--PERFORM * FROM dblink('pgmv$_instance',tSqlStatement) AS p (ret TEXT);
		EXECUTE tSqlStatement;
		
		COMMIT;
		
	END IF;
	
ELSE

	EXECUTE tSqlStatement;
	
END IF;
 
  END LOOP;
END;
$BODY$
LANGUAGE    plpgsql;


------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$dropmvindexes
            (
                pOwner      IN      TEXT,
                pViewName   IN      TEXT,
				pParallel	IN		TEXT
            )
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$dropmvindexes
Author:       Jack Bills
Date:         13/08/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
17/08/2021	| D Day			| Added logic to commit dropping the indexes to support running in parallel
13/08/2020  | J Bills       | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This will drop the indexes from the materialised view

Arguments:      IN      pOwner             The owner of the object
                IN      pViewName          The name of the materialized view base table
				IN		pParallel		   Optional, build in parallel
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/

DECLARE
  i RECORD;
BEGIN
  FOR i IN 
    (
		select schemaname || '.' || indexname as relname
from pg_indexes 
where tablename = pViewName
and schemaname = pOwner
and indexname  NOT IN
(SELECT  distinct c.relname
                      FROM
                          pg_class t,
                          pg_class c,
                          pg_index ix,
                          pg_attribute a,
                          pg_namespace s
                      WHERE
                          t.oid = ix.indrelid
                          AND c.oid = ix.indexrelid
                          AND t.relnamespace = s.oid
                          AND a.attrelid = t.oid
                          AND a.attnum = ANY(ix.indkey)
                          AND t.relkind = 'r'
                          AND s.nspname = pOwner
                          AND t.relname = pViewName
                          AND    ix.indisprimary)	
	)
  LOOP

    EXECUTE 'DROP INDEX ' || i.relname;
	
  END LOOP;
  
  IF pParallel = 'Y' THEN

	COMMIT;
	
  END IF;
	
END;
$BODY$
LANGUAGE    plpgsql;

------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$readdmvindexes
            (
                pViewName   IN      TEXT
            )
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$readdmvindexes
Author:       Jack Bills
Date:         13/08/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
13/08/2020  | J Bills      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This will re-add the indexes on the materialised view following the insert of the rows

Arguments:      IN      pViewName          The name of the materialized view base table
                
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/


DECLARE
rec RECORD;
table_name TEXT;
BEGIN
table_name := pViewName ;
FOR rec IN EXECUTE format('SELECT indexdef FROM temp_%I', table_name) LOOP
EXECUTE rec.indexdef;
END LOOP;
END;
$BODY$
LANGUAGE    plpgsql;


------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$dropindexestemptable
            (
                pViewName   IN      TEXT
            )
AS
$BODY$


/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$dropindexestemptable
Author:       Jack Bills
Date:         13/08/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
13/08/2020  | J Bills      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This will drop the temporary table created holding indexes scripts

Arguments:      IN      pViewName          The name of the materialized view base table
                
Returns:                VOID

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/

DECLARE
    tSqlStatement   TEXT    := NULL;
BEGIN
    tSqlStatement   :=  'DROP TABLE IF EXISTS temp' || '_' || pViewName;
    EXECUTE tSqlStatement;
    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$dropindexestemptable';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END ;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$setQueryJoinsMultiTablePosition
            (
                pTableNames         		IN   TEXT[],
                pAliasArray         		IN   TEXT[],
				pQueryJoinsMultiTabPosArray INOUT  SMALLINT[]		
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setQueryJoinsMultiTablePosition
Author:       David Day
Date:         21/10/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
21/10/2020  | D Day         | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Procedure to generate the query joins multi table position array for when the same table name is used more than once.

Arguments:      IN      pTableNames         
                IN      pAliasArray
                INOUT     pQueryJoinsMultiTabPosArray
				
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rTableNames       	RECORD;
	
	iTableCount		  	INTEGER;
	
	iCounter		  	INTEGER := 0;
	
	tTableArray  		TEXT[] := '{}';
	
	iMultiTableCount 	INTEGER := 0;
	

BEGIN

FOR rTableNames IN (SELECT UNNEST(pTableNames) AS table_name, UNNEST(pAliasArray) AS alias_name) LOOP

	iCounter 	:= iCounter +1;
	
	SELECT count(1) INTO iTableCount
	FROM (SELECT UNNEST(pTableNames) AS table_name) inline
	WHERE inline.table_name = rTableNames.table_name;
	
	tTableArray[iCounter] := rTableNames.table_name;
	
	IF iTableCount = 1 THEN
	
		pQueryJoinsMultiTabPosArray[iCounter] := 1;
		
	ELSE
	
		SELECT count(1) INTO iMultiTableCount
		FROM (SELECT UNNEST(tTableArray) AS table_name) inline
		WHERE inline.table_name = rTableNames.table_name;
	
		pQueryJoinsMultiTabPosArray[iCounter] := iMultiTableCount;		
	
	END IF;

END LOOP;

EXCEPTION
WHEN OTHERS
THEN
	RAISE INFO      'Exception in procedure mv$setQueryJoinsMultiTablePosition';
	RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
	RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$setQueryJoinsMultiTableCount
            (
                pTableNames         		IN   TEXT[],
                pAliasArray         		IN   TEXT[],
				pQueryJoinsMultiTabCntArray			INOUT  SMALLINT[]		
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setQueryJoinsMultiTableCount
Author:       David Day
Date:         21/10/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
21/10/2020  | D Day         | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Procedure to generate the query joins multi table total count array for when the same table name is used more than once.

Arguments:      IN      pTableNames         
                IN      pAliasArray
                INOUT   pQueryJoinsMultiTabPosArray
				
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rTableNames       RECORD;
	
	iTableCount		  INTEGER;
	
	iCounter		  INTEGER := 0;

BEGIN

FOR rTableNames IN (SELECT UNNEST(pTableNames) AS table_name, UNNEST(pAliasArray) AS alias_name) LOOP

	iCounter 	:= iCounter +1;
	
	SELECT count(1) INTO iTableCount
	FROM (SELECT UNNEST(pTableNames) AS table_name) inline
	WHERE inline.table_name = rTableNames.table_name;
	
	pQueryJoinsMultiTabCntArray[iCounter] := iTableCount;

END LOOP;

EXCEPTION
WHEN OTHERS
THEN
	RAISE INFO      'Exception in procedure mv$setQueryJoinsMultiTableCount';
	RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
	RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$setCronSchedule()
    RETURNS text
AS $BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setCronSchedule
Author:       David Day
Date:         13/07/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
13/07/2021  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description: 	Function to set cron schedule time for parallel insert statements

Arguments:      	    	

Returns:                text
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

tSql 				TEXT;
tsCurrentDate 		TIMESTAMP;
iMinutes			INTEGER;
iHours				INTEGER;
tCronJobSchedule	TEXT;

BEGIN

	tsCurrentDate := clock_timestamp() AT TIME ZONE 'GMT';
		
	tSql := 'SELECT extract(
	MINUTE FROM TIMESTAMP '''||tsCurrentDate||''')';

	EXECUTE tSql INTO iMinutes;

	tSql := 'SELECT extract(
	HOUR FROM TIMESTAMP '''||tsCurrentDate||''')';

	EXECUTE tSql INTO iHours;

	IF iMinutes IN (58,59) THEN

		IF iMinutes = 58 THEN
			iMinutes := 0;
		ELSE
			iMinutes := 1;
		END IF;
			
		IF iHours = 23 THEN
		
			iHours   := 0;
			
		ELSE
		
			iHours   := iHours+1;
			
		END IF;
		
	ELSE
	
		iMinutes := iMinutes+2;
	
	END IF;

	tCronJobSchedule := iMinutes||' '||iHours||' * * *';
		
	RETURN tCronJobSchedule;
	
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mv$setFromAndToTimestampRange(pMinDate       	IN      DATE,
														 pMaxDate       	IN      DATE,
														 pParallelJobs		IN		INTEGER,
														 pSequence			IN		INTEGER,
														 pParallelColumn	IN		TEXT,
														 pParallelAlias		IN		TEXT)
    RETURNS text
AS $BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$setFromAndToTimestampRange
Author:       David Day
Date:         13/07/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
16/02/2022  | D Day         | Defect fix to handle milliseconds for date ranges split
13/07/2021  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description: 	Function to set timestamp date range for cron insert statements

Arguments:      	    	

Returns:                text
************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

lSql				TEXT;

iDaysSplit			TEXT;

tSql 				INTEGER;
tsCurrentDate 		TIMESTAMP;

iMinutes			INTEGER;
iHours				INTEGER;
tCronJobSchedule	TEXT;

iSequencePlusOne	INTEGER := pSequence+1;

tFrom_Date			TEXT;
tTo_Date			TEXT;

tWhereClauseSql 	TEXT;

BEGIN

	SELECT (pMaxDate-pMinDate)/pParallelJobs AS days_diff INTO iDaysSplit;
	
	iDaysSplit := iDaysSplit||' day';
	
	lSql := 'SELECT CAST(min(inline.dates)::date||'' 00:00:00'' AS TIMESTAMP) AS from_date, CAST(max(inline.dates)::date||'' 23:59:59'' AS TIMESTAMP) AS to_date
	FROM (SELECT dates, ROW_NUMBER() OVER(ORDER BY dates ASC) AS id
	FROM generate_series
			( '''||pMinDate||'''::timestamp 
			, '''||pMaxDate||'''::timestamp
			, '''||iDaysSplit||'''::interval) dates) inline
	WHERE inline.id IN ('||pSequence||','||iSequencePlusOne||')';
	
	EXECUTE lSql INTO tFrom_Date, tTo_Date;
	
	IF pParallelJobs = pSequence THEN
	
		tTo_Date := pMaxDate||' 23:59:59.999999';
		

	ELSE

	    tTo_Date := tTo_Date||'.999999';

	END IF;
	
	IF pSequence > 1 THEN
	
		tFrom_Date := tFrom_Date::date+1||' 00:00:00';
		
	END IF;	
	
	tWhereClauseSql := pParallelAlias||'.'||pParallelColumn||' BETWEEN '''||tFrom_Date||''' AND '''||tTo_Date||'''';

	RETURN tWhereClauseSql;
	
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$updatePgMviewTableParallelData
            (
                pViewName    	IN      TEXT,
				pParallel		IN      TEXT DEFAULT 'N',
                pParallelJobs   IN      INTEGER DEFAULT 0,
                pParallelColumn IN      TEXT DEFAULT NULL,
                pParallelAlias  IN      TEXT DEFAULT NULL,
                pParallelUser   IN      TEXT DEFAULT NULL,
				pParallelDbname IN      TEXT DEFAULT NULL
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$updatePgMviewTableParallelData
Author:       David Day
Date:         17/08/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
28/01/2022	|D Day 			| Defect fix to allow null values to be dynamically updated.
17/08/2021  |D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This procedure updates parallel values held in pg$mviews to support full refresh

Arguments:      IN      pViewName           The name of the materialized view
				IN		pParallel			Optional, set to Y if you want to run build insert in parallel
				IN		pParallelJobs		Optional, if pParallel set to Y then set how many parallel jobs are required
				IN		pParallelColumn		Optional, if pParallel set to Y then add date column you want to split insert into parallel 
											cron sessions by date range.
				IN		pParallelAlias		Optional, if pParallel set to Y then add date column alias you want to split insert into parallel 
											cron sessions by date range.
				IN		pParallelUser		Optional, if pParallel set to Y then add user for pg_cron job sessions.
				IN		pParallelDbname		Optional, if pParallel set to Y then add dbname for pg_cron job sessions.
Returns:                RECORD              The row of data from the data dictionary relating to this materialized view

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE
    tSqlStatement TEXT;
BEGIN

	IF (pParallelColumn IS NULL OR pParallelAlias IS NULL OR pParallelUser IS NULL OR pParallelDbname IS NULL) THEN

		tSqlStatement := 'UPDATE pg$mviews SET
			parallel 		= '''||pParallel||''',
			parallel_jobs   = '''||pParallelJobs||''',
			parallel_column = null,
			parallel_alias  = null,
			parallel_user   = null,
			parallel_dbname = null
		WHERE view_name = '''||pViewName||'''';
		
	ELSE

		tSqlStatement := 'UPDATE pg$mviews SET
			parallel 		= '''||pParallel||''',
			parallel_jobs   = '''||pParallelJobs||''',
			parallel_column = '''||pParallelColumn||''',
			parallel_alias  = '''||pParallelAlias||''',
			parallel_user   = '''||pParallelUser||''',
			parallel_dbname = '''||pParallelDbname||'''
		WHERE view_name = '''||pViewName||'''';
		
	END IF;
	
	EXECUTE tSqlStatement;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure mv$updatePgMviewTableParallelData';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$CheckTriggerExists
            (
                pConst          IN      mv$allConstants,
                pTableName      IN      TEXT
            )
RETURNS boolean
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$CheckTriggerExists
Author:       David Day
Date:         26/08/2022
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
26/08/2022  | D Day			| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Generic function to check triggers in a Postgres database, used in this context to check the trigger exists
                from the Materialized View Log tables when building Materialized View.

Arguments:      IN      pConst              The memory structure containing all constants
                IN      pTableName          The name of the materialized view source table
				
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement	TEXT;
	
	iTriggerExists	INTEGER := 0;
	
	bResult			BOOLEAN := FALSE;

BEGIN

	tSqlStatement := pConst.SELECT_COMMAND || 'COUNT(1) ' || pConst.FROM_PG$MVIEW_LOGS ||' l '|| pConst.JOIN_DML_TYPE || pConst.OPEN_BRACKET 
	|| pConst.SELECT_COMMAND || 'table_name, table_schema'||pConst.FROM_COMMAND||'information_schema.tables'|| pConst.CLOSE_BRACKET 
	|| ' t' || pConst.ON_COMMAND || 't.table_name = l.pglog$_name' || pConst.AND_COMMAND || 't.table_schema = l.owner' || pConst.AND_COMMAND
	|| 'l.table_name = '''|| pTableName || ''''
	|| pConst.WHERE_COMMAND || 'l.trigger_name'|| pConst.IN_SELECT_COMMAND || pConst.DISTINCT_CLAUSE || 'trigger_name' || pConst.FROM_COMMAND
	|| 'information_schema.triggers' || pConst.WHERE_COMMAND || 'event_object_table' || pConst.EQUALS_COMMAND || '''' || pTableName || '''' 
	|| pConst.CLOSE_BRACKET;
	
    EXECUTE tSqlStatement INTO iTriggerExists;
	
	IF iTriggerExists = 0 THEN
		bResult := FALSE;
	ELSE	
		bResult := TRUE;
	END IF;

    RETURN( bResult );	

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$CheckTriggerExists';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
PROCEDURE    mv$CheckMvLogExists
            (
                pConst          IN      mv$allConstants,
				pViewName		IN 		TEXT,
                pTableNames     IN      TEXT[]
            )
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$CheckMvLogExists
Author:       David Day
Date:         26/08/2022
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
26/08/2022  | D Day			| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Generic procedure to check mv logs in a Postgres database, used in this context to check the Materialized View Log 
				tables exists when building Materialized View.

Arguments:      IN      pConst              The memory structure containing all constants
				IN 		pViewName			The view name
                IN      pTableNames         List of materialized view source tables
				
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement 			TEXT;
	rLogNames 	  			RECORD;
	iLogTableExists 		INTEGER := 0;
	bResultTriggerExists  	BOOLEAN := FALSE;
	tMvLogName				TEXT;
	tTriggerNotExistsInd	TEXT := 'N';
	tMvLogNotExistsInd		TEXT := 'N';
	
BEGIN

	FOR rLogNames IN SELECT DISTINCT UNNEST( pTableNames ) AS table_name LOOP
	
		tSqlStatement := pConst.SELECT_COMMAND || 'COUNT(1) ' || pConst.FROM_PG$MVIEW_LOGS ||' l '|| pConst.JOIN_DML_TYPE || pConst.OPEN_BRACKET 
		|| pConst.SELECT_COMMAND || 'table_name, table_schema'||pConst.FROM_COMMAND||'information_schema.tables'|| pConst.CLOSE_BRACKET 
		|| ' t' || pConst.ON_COMMAND || 't.table_name = l.pglog$_name' || pConst.AND_COMMAND || 't.table_schema = l.owner' || pConst.AND_COMMAND
		|| 'l.table_name'||pConst.EQUALS_COMMAND|| '''' ||rLogNames.table_name|| '''';
		
		EXECUTE tSqlStatement INTO iLogTableExists;
		
		IF iLogTableExists = 0 THEN
			tSqlStatement := pConst.SELECT_COMMAND || 'pglog$_name' || pConst.FROM_COMMAND || 'pg$mview_logs' || pConst.WHERE_COMMAND 
			|| 'table_name' || pConst.EQUALS_COMMAND || '''' || rLogNames.table_name || '''';
			
			EXECUTE tSqlStatement INTO tMvLogName;
			tMvLogNotExistsInd := 'Y';		
			RAISE INFO 'Error: Mv Log % does not exist for table %.', tMvLogName, rLogNames.table_name;
			
		END IF;
		
		SELECT mv$CheckTriggerExists(pConst, rLogNames.table_name ) INTO bResultTriggerExists;
		IF bResultTriggerExists = FALSE THEN
			tTriggerNotExistsInd := 'Y';
			RAISE INFO 'Error: Trigger does not exist for table %.', rLogNames.table_name; 			
		END IF;
	
	END LOOP;
	
	IF tTriggerNotExistsInd = 'Y' AND tMvLogNotExistsInd = 'Y' THEN
	
		RAISE INFO 'Exception in function mv$CheckTriggerExists';
		RAISE EXCEPTION 'Error: Unable to build materialized view %. Missing mv log(s) and trigger(s) identifed.
This trigger used for logging into the mv log will need adding back onto this table. Full review of pg$mviews and pg$mview_logs will need to be done to identify any additional materialized views linked to this table. 
These will need rebuilding afterwards due to potential missing data changes not being applied', pViewName; 
	
	ELSIF tTriggerNotExistsInd = 'Y' AND tMvLogNotExistsInd = 'N' THEN
	
		RAISE INFO 'Exception in function mv$CheckTriggerExists';
		RAISE EXCEPTION 'Error: Unable to build materialized view %. Missing trigger(s) identified.
This trigger used for logging into the mv log will need adding back onto this table. Full review of pg$mviews and pg$mview_logs will need to be done to identify any additional materialized views linked to this table. 
These will need rebuilding afterwards due to potential missing data changes not being applied', pViewName;

	ELSIF tTriggerNotExistsInd = 'N' AND tMvLogNotExistsInd = 'Y' THEN

		--RAISE INFO 'Exception in procedure mv$CheckMvLogExists';
		RAISE EXCEPTION 'Error: Unable to build materialized view %. Missing mv log(s) identified.
Full review of pg$mviews and pg$mview_logs will need to be done to identify any additional materialized views linked to this table. 
These will need rebuilding afterwards due to potential missing data changes not being applied', pViewName;

	END IF;

EXCEPTION
WHEN OTHERS
THEN
	RAISE INFO      'Exception in procedure mv$CheckMvLogExists';
	RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
	RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
	RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE
FUNCTION    mv$getPgMviewSettingsNameValue
            (
                pName		IN 		TEXT				
            )
    RETURNS TEXT
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$getPgMviewSettingsNameValue
Author:       David Day
Date:         20/11/2023
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
20/11/2023  | D Day      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Returns name value from table pg$mview_settings.

Arguments:     	IN      pName               The name linked to setting value
Returns:                TEXT                The setting value returned from the matching input parameter pName

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

tSetting TEXT;

BEGIN
	
	SELECT COALESCE(setting,'SKIP') AS setting INTO tSetting
	FROM pg$mviews_settings
	WHERE name = pName;

    RETURN( tSetting );	

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function mv$getPgMviewSettingsNameValue';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;