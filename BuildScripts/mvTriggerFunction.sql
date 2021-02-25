/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mvTriggerFunction.sql
Author:       David Day
Date:         06/05/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
06/05/2020  | D Day      	| Initial version - Moved out of mvApplicationFunctions.sql to support UPDATE patching
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This is the build script for the Application Trigger database function that are required to support the Materialized View
                fast refresh process.

                This script contains functions that rely on other database functions having been previously created and must
                therefore be run last in the build process.

Notes:          Some of the functions in this file rely on functions that are created within this file and so whilst the functions
                should be maintained in alphabetic order, this is not always possible.

                All functions must be created with SECURITY DEFINER to ensure they run with the privileges of the owner.

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Debug:          Add a variant of the following command anywhere you need some debug inforaiton
                RAISE NOTICE '<Funciton Name> % %',  CHR(10), <Variable to be examined>;

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

-- psql -h localhost -p 5432 -d postgres -U pgrs_mview -q -f mvTriggerFunction.sql

-- -------------------- Write DROP-FUNCTION-stage scripts ----------------------

SET     CLIENT_MIN_MESSAGES = ERROR;

DROP FUNCTION IF EXISTS mv$insertMaterializedViewLogRow CASCADE;

SET CLIENT_MIN_MESSAGES = NOTICE;

-- -------------------- Write CREATE-FUNCTION-stage scripts --------------------
CREATE OR REPLACE
FUNCTION    mv$insertMaterializedViewLogRow()
    RETURNS TRIGGER
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$insertMaterializedViewLogRow
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
23/03/2020	| D Day			| Removed exception block from trigger as they can be particularly troublesome because they burn extra 7
			|				| transaction IDs. If there is a trigger on a table with an exception block, the sub transaction 
			|				| consumes a transaction ID for each row being inserted causing the situation much sooner. 
12/11/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    This is the function that is called by the trigger on the base table.

Notes:          If the trigger is activated via a delete command then we have to get the original value of the MV_M_ROW$_COLUMN,
                otherwise we must use the new value

                If no materialized view has registered an interest in this table, no rows will be created

Arguments:      NONE
Returns:                TRIGGER     PostGre required return array for all functions called from a trigger

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    tSqlStatement       TEXT;
    uRow$               UUID;
    aMikePgMviewLogs    pg$mview_logs;
    rConst              mv$allConstants;

BEGIN

    rConst              := mv$buildTriggerConstants();
    aMikePgMviewLogs    := mv$getPgMviewLogTableData( rConst, TG_TABLE_SCHEMA::TEXT, TG_TABLE_NAME::TEXT );

    IF rConst.BITMAP_NOT_SET < ANY( aMikePgMviewLogs.pg_mview_bitmap )
    THEN
        IF TG_OP = rConst.DELETE_DML_TYPE
        THEN
            uRow$ := OLD.m_row$;
        ELSE
            uRow$ := NEW.m_row$;
        END IF;
        
        tSqlStatement := rConst.INSERT_INTO             ||  aMikePgMviewLogs.pglog$_name    || rConst.MV_LOG$_INSERT_COLUMNS    ||
                         rConst.SELECT_COMMAND          ||
                         rConst.SINGLE_QUOTE_CHARACTER  ||  uRow$                           || rConst.QUOTE_COMMA_CHARACTERS    ||
                                                            rConst.PG_MVIEW_BITMAP          || rConst.COMMA_CHARACTER           ||
                         rConst.SINGLE_QUOTE_CHARACTER  ||  TG_OP                           || rConst.SINGLE_QUOTE_CHARACTER    ||
                         rConst.FROM_PG$MVIEW_LOGS      ||
                         rConst.WHERE_OWNER_EQUALS      ||  TG_TABLE_SCHEMA                 || rConst.SINGLE_QUOTE_CHARACTER    ||
                         rConst.AND_TABLE_NAME_EQUALS   ||  TG_TABLE_NAME                   || rConst.SINGLE_QUOTE_CHARACTER;

        EXECUTE tSqlStatement;
    END IF;
    RETURN  NULL;
	
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;
------------------------------------------------------------------------------------------------------------------------------------