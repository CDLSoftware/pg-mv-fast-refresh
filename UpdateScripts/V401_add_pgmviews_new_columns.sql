CREATE OR REPLACE PROCEDURE v401_addColumnsToPg$mviews()
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v401_addColumnsToPg$mviews
Author:       David Day
Date:         09/07/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
09/07/2020  | D Day		    | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     Postgres does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this project is to fold these changes into the PostGre kernel.

Description:    This is a patch script to create the data dictionary pg$mviews_oj_details table new column join_replacement_from_sql.

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

BEGIN

SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name= 'pg$mviews'
AND column_name='inner_join_table_array';

IF iColumnCnt = 0 THEN

	ALTER TABLE pg$mviews
	ADD COLUMN inner_join_table_array text[];
	
	ALTER TABLE pg$mviews
	ADD COLUMN inner_join_alias_array text[];

	ALTER TABLE pg$mviews
	ADD COLUMN inner_join_rowid_array text[];
	
	ALTER TABLE pg$mviews
	ADD COLUMN inner_join_other_table_array text[];
	
	ALTER TABLE pg$mviews
	ADD COLUMN inner_join_other_alias_array text[];

	ALTER TABLE pg$mviews
	ADD COLUMN inner_join_other_rowid_array text[];

END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

CALL v401_addColumnsToPg$mviews();

DROP PROCEDURE v401_addColumnsToPg$mviews;