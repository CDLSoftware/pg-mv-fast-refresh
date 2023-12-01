CREATE OR REPLACE FUNCTION v1301_create_table_pg$mviews_settings(IN pis_module_owner TEXT)
    RETURNS VOID
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v1301_create_table_pg$mviews_settings
Author:       David Day
Date:         20/11/2023
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/11/2021  | D Day		    | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     Postgres does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this project is to fold these changes into the PostGre kernel.

Description:    This is a patch script to create the data dictionary pg$mviews_settings table.

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

iTableExists						INTEGER := 0;

BEGIN

SELECT COUNT(1) INTO STRICT iTableExists 
FROM pg_class c
JOIN pg_namespace s on c.relnamespace = s.oid
WHERE relname = 'pg$mviews_settings'
AND s.nspname = pis_module_owner;

IF iTableExists = 0 THEN
	
	EXECUTE 'CREATE TABLE IF NOT EXISTS '||pis_module_owner||'.pg$mviews_settings
(
		name 						TEXT NOT NULL PRIMARY KEY,
		setting 					TEXT,
		unit						TEXT,
		description					TEXT
)';

	EXECUTE 'INSERT INTO '||pis_module_owner||'.pg$mviews_settings (name, setting)
VALUES
    (''freeable_mem'',null,''MB'',''Total freeable memory for calculating work memory, to be used when creating parallel inserts cron jobs. Recommended no more than 60 perecentage of the total freeable memory available on this database.'')
ON CONFLICT (name) DO NOTHING';

END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

SELECT v1301_create_table_pg$mviews_settings(:'MODULEOWNER');

DROP FUNCTION v1301_create_table_pg$mviews_settings;