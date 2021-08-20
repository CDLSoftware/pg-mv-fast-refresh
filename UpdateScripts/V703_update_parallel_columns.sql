CREATE OR REPLACE FUNCTION V702_update_parallel_columns()
    RETURNS VOID
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: V701_update_parallel_columns
Author:       David Day
Date:         20/08/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
20/08/2021  | D Day		    | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     Postgres does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this project is to fold these changes into the PostGre kernel.
Description:    This is a patch script to populate the data dictionary pg$mviews table new parallel columns with default values.
Notes:          
Issues:        	https://forums.aws.amazon.com/thread.jspa?messageID=860564
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

DECLARE

iColumnCnt						INTEGER := 0;

rMviewsOjDetails				RECORD;
tOuterJoinDeleteStatement 		TEXT;
iDeleteSqlIsNull				INTEGER := 0;

rConst              			mv$allConstants;

BEGIN

rConst      := mv$buildAllConstants();

SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name = 'pg$mviews'
AND column_name like 'parallel%';

IF iColumnCnt = 6 THEN

	FOR rPgMviews IN (		SELECT  m.view_name
							 FROM 	pg$mviews m
							 WHERE  m.parallel IS NULL) LOOP
							 
	UPDATE pg$mviews
	SET parallel = 'N',
	parallel_jobs = 0,
	parallel_column = null,
	parallel_alias = null,
	parallel_user = null,
	parallel_dbname = null
	WHERE view_name = rPgMviews.view_name;
						
	END LOOP;

	SELECT COUNT(1) INTO iParallelIsNull
	FROM   pg$mviews
	WHERE  parallel IS NULL;

	IF iParallelIsNull > 0 THEN
			RAISE EXCEPTION 'The UPDATE patch script V702_update_parallel_columns.sql has not successfully updated all the linking parallel data for each mview in data dictionary table pg$mviews';
	END IF;
	
END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

SELECT V702_update_parallel_columns();

DROP FUNCTION V702_update_parallel_columns;