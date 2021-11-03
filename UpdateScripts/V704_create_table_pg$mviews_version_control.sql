CREATE OR REPLACE FUNCTION v704_create_table_pg$mviews_version_control(IN pis_module_owner TEXT)
    RETURNS VOID
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v704_create_table_pg$mviews_version_control
Author:       David Day
Date:         03/11/2021
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

Description:    This is a patch script to create the data dictionary pg$mviews table new parallel columns.

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
WHERE relname = 'pg$mviews_version_control'
AND s.nspname = pis_module_owner;

IF iTableExists = 0 THEN

	EXECUTE 'CREATE TABLE IF NOT EXISTS '||pis_module_owner||'.pg$mviews_version_control
	(
			version_control_id 			SERIAL NOT NULL PRIMARY KEY,
			version 					CHARACTER VARYING(150) NOT NULL UNIQUE,
			live_version_flag 			CHARACTER VARYING(3),
			created 					TIMESTAMP(0) WITHOUT TIME ZONE
	)';

END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

SELECT v704_create_table_pg$mviews_version_control(:'MODULEOWNER');

DROP FUNCTION v704_create_table_pg$mviews_version_control;