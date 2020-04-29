CREATE OR REPLACE FUNCTION v102_mv$outerJoinToInnerJoinReplacement(
	pTableNames TEXT,
	pTableAlias TEXT)
    RETURNS text
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v102_mv$outerJoinToInnerJoinReplacement(
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
				
				ONLY to be used by UPDATE patch release v102 and will be dropped once patch has been applied.

Arguments:      IN      pTableNames             
                IN      pTableAlias              
Returns:                TEXT

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE	
	
	iLeftJoinCnt 			INTEGER := 0;
	iLeftOuterJoinCnt		INTEGER := 0;
	iLeftJoinOverallCnt		INTEGER := 0;
	
	iRightJoinCnt 			INTEGER := 0;
	iRightOuterJoinCnt		INTEGER := 0;
	iRightJoinOverallCnt	INTEGER := 0;	
	
	iLoopLeftJoinCnt		INTEGER := 0;
	iLoopLeftOuterJoinCnt	INTEGER := 0;
	iLoopJoinLeftCnt		INTEGER := 0;
	
	iLoopRightJoinCnt		INTEGER := 0;
	iLoopRightOuterJoinCnt	INTEGER := 0;
	iLoopJoinRightCnt		INTEGER := 0;
	
	iLeftJoinLoopAliasCnt 	INTEGER := 0;
	iRightJoinLoopAliasCnt 	INTEGER := 0;
	
	tTablesSQL 				TEXT := pTableNames;
	  
	tSQL					TEXT;
	tLeftJoinLine			TEXT;
	tOrigLeftJoinLine 		TEXT;
	
	tRightJoinLine			TEXT;
	tOrigRightJoinLine 		TEXT;
	
	tLeftJoinSyntax			TEXT;
	tRightJoinSyntax		TEXT;
	
	tTableAlias 			TEXT := REPLACE(pTableAlias,'.','\.');
	
	iStartPosition			INTEGER := 0;
	iEndPosition			INTEGER := 0;
	
	tRegExpLeftOuterJoinSyntax	TEXT;
	
BEGIN

tTablesSQL := regexp_replace(tTablesSQL,'left join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left outer join join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right outer join','RIGHT OUTER JOIN','gi');
	  
SELECT count(1) INTO iLeftJoinCnt FROM regexp_matches(tTablesSQL,'LEFT JOIN','g');
SELECT count(1) INTO iLeftOuterJoinCnt FROM regexp_matches(tTablesSQL,'LEFT JOIN','g');
SELECT count(1) INTO iLeftJoinOverallCnt FROM regexp_matches(tTablesSQL,'LEFT JOIN|LEFT OUTER JOIN','g');

SELECT count(1) INTO iRightJoinCnt FROM regexp_matches(tTablesSQL,'RIGHT JOIN','g');
SELECT count(1) INTO iRightOuterJoinCnt FROM regexp_matches(tTablesSQL,'RIGHT JOIN','g');
SELECT count(1) INTO iRightJoinOverallCnt FROM regexp_matches(tTablesSQL,'RIGHT JOIN|RIGHT OUTER JOIN','g');

tSQL := tTablesSQL;

IF iLeftJoinOverallCnt > 0 THEN
	  
	FOR i IN 1..iLeftJoinOverallCnt
	LOOP

	IF iLeftJoinCnt > 0 AND iLoopLeftJoinCnt < iLeftJoinCnt THEN

		iLoopLeftJoinCnt = iLoopLeftJoinCnt + 1;
		
		tRegExpLeftOuterJoinSyntax := 'LEFT+[[:space:]]+JOIN+';
		iLoopJoinLeftCnt := iLoopLeftJoinCnt;
		tLeftJoinSyntax := 'LEFT JOIN';

		iStartPosition := mv$regexpinstr(tTablesSQL,
		tRegExpLeftOuterJoinSyntax,
		1,
		iLoopLeftJoinCnt,
		1,
		'i')-9;	
		
	END IF;

	IF iLeftOuterJoinCnt > 0 AND iLoopLeftOuterJoinCnt < iLeftOuterJoinCnt THEN

		iLoopLeftOuterJoinCnt = iLoopLeftOuterJoinCnt + 1; 

		tRegExpLeftOuterJoinSyntax := 'LEFT+[[:space:]]+OUTER+[[:space:]]+JOIN+';
		iLoopJoinLeftCnt := iLoopLeftOuterJoinCnt;
		tLeftJoinSyntax := 'LEFT OUTER JOIN';

		iStartPosition := mv$regexpinstr(tTablesSQL,
		tRegExpLeftOuterJoinSyntax,
		1,
		iLoopLeftOuterJoinCnt,
		1,
		'i')-15;
		
	END IF;

	iEndPosition := mv$regexpinstr(tTablesSQL,
	tRegExpLeftOuterJoinSyntax||'[[:space:]]+[a-zA-Z0-9_]+[[:space:]]+[a-zA-Z0-9_]+[[:space:]]+[a-zA-Z0-9_]+[[:space:]]+[a-zA-Z0-9_]+[.]+[a-zA-Z0-9_]+[[:space:]]+[=]+[[:space:]]+[a-zA-Z0-9_]+[.]+[a-zA-Z0-9_]+',
	1,
	iLoopJoinLeftCnt,
	1,
	'i');

	tLeftJoinLine := substr(tTablesSQL,iStartPosition, iEndPosition - iStartPosition);

	SELECT count(1) INTO iLeftJoinLoopAliasCnt FROM regexp_matches(tLINE,'[[:space:]]+'||tTableAlias,'g');

	IF iLeftJoinOverallCnt > 0 THEN

		tOrigLeftJoinLine := tLeftJoinLine;

		tLeftJoinLine := replace(tLeftJoinLine,tLeftJoinSyntax,'INNER JOIN');

		IF i = 1 THEN

			tSQL := replace(tTablesSQL,tOrigLeftJoinLine,tLeftJoinLine);
					  
		ELSE 
					  
			tSQL := replace(tSQL,tOrigLeftJoinLine,tLeftJoinLine);
			
		END IF;

	END IF;

	END LOOP;

ELSIF iRightJoinOverallCnt > 0 THEN

	FOR i IN 1..iRightJoinOverallCnt
	LOOP

	IF iRightJoinCnt > 0 AND iLoopRightJoinCnt < iRightJoinCnt THEN

		iLoopRightJoinCnt = iLoopRightJoinCnt + 1;
		
		tRegExpLeftOuterJoinSyntax := 'RIGHT+[[:space:]]+JOIN+';
		iLoopJoinRightCnt := iLoopRightJoinCnt;
		tRightJoinSyntax := 'RIGHT JOIN';

		iStartPosition := mv$regexpinstr(tTablesSQL,
		tRegExpRightOuterJoinSyntax,
		1,
		iLoopRightJoinCnt,
		1,
		'i')-10;	
		
	END IF;

	IF iRightOuterJoinCnt > 0 AND iLoopRightOuterJoinCnt < iRightOuterJoinCnt THEN

		iLoopRightOuterJoinCnt = iLoopRightOuterJoinCnt + 1; 

		tRegExpLeftOuterJoinSyntax := 'RIGHT+[[:space:]]+OUTER+[[:space:]]+JOIN+';
		iLoopJoinRightCnt := iLoopRightOuterJoinCnt;
		tRightJoinSyntax := 'RIGHT OUTER JOIN';

		iStartPosition := mv$regexpinstr(tTablesSQL,
		tRegExpRightOuterJoinSyntax,
		1,
		iLoopRightOuterJoinCnt,
		1,
		'i')-16;
		
	END IF;

	iEndPosition := mv$regexpinstr(tTablesSQL,
	tRegExpRightOuterJoinSyntax||'[[:space:]]+[a-zA-Z0-9_]+[[:space:]]+[a-zA-Z0-9_]+[[:space:]]+[a-zA-Z0-9_]+[[:space:]]+[a-zA-Z0-9_]+[.]+[a-zA-Z0-9_]+[[:space:]]+[=]+[[:space:]]+[a-zA-Z0-9_]+[.]+[a-zA-Z0-9_]+',
	1,
	iLoopJoinRightCnt,
	1,
	'i');

	tRightJoinLine := substr(tTablesSQL,iStartPosition, iEndPosition - iStartPosition);

	SELECT count(1) INTO iRightJoinLoopAliasCnt FROM regexp_matches(tRightJoinLine,'[[:space:]]+'||tTableAlias,'g');

	IF iRightJoinLoopAliasCnt > 0 THEN

		tOrigRightJoinLine := tRightJoinLine;

		tRightJoinLine := replace(tRightJoinLine,tRightJoinSyntax,'INNER JOIN');

		IF i = 1 THEN

			tSQL := replace(tTablesSQL,tOrigRightJoinLine,tRightJoinLine);
					  
		ELSE 
					  
			tSQL := replace(tSQL,tOrigRightJoinLine,tRightJoinLine);
			
		END IF;

	END IF;

	END LOOP;

END IF;

RETURN tSQL;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

CREATE OR REPLACE FUNCTION pgrs_mview.V103_update_join_replacement_from_sql()
    RETURNS VOID
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: V102_update_join_replacement_from_sql.sql
Author:       David Day
Date:         29/04/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
29/04/2020  | D Day		    | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     Postgres does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this project is to fold these changes into the PostGre kernel.

Description:    This is a patch script to update the data dictionary pg$mviews_oj_details table new column join_replacement_from_sql
				with the from tables sql to replace the alias outer join conditions to inner join condition to be used by the outer join delete
				and insert sql statements to help improve performance for large scale data volumes.

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
tClauseJoinReplacement 			TEXT;
iJoinReplacementFromSqlIsNull	INTEGER := 0;

BEGIN


SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name= 'pg$mviews_oj_details'
AND column_name='join_replacement_from_sql';

IF iColumnCnt = 0 THEN

	ALTER TABLE pg$mviews_oj_details
	ADD COLUMN join_replacement_from_sql text;

	FOR rMviewsOjDetails IN (SELECT moj.owner,
									moj.view_name,
									moj.table_alias,
									m.table_names
							 FROM 	pg$mviews_oj_details moj
							 ,      pg$mviews m
							 WHERE  moj.view_name = m.view_name) LOOP
							 
	tClauseJoinReplacement := v102_mv$outerJoinToInnerJoinReplacement(rMviewsOjDetails.table_names, rMviewsOjDetails.table_alias);
			
	UPDATE pg$mviews_oj_details
	SET join_replacement_from_sql = tClauseJoinReplacement
	WHERE view_name = rMviewsOjDetails.view_name
	AND table_alias = rMviewsOjDetails.table_alias;
						
	END LOOP;

	SELECT COUNT(1) INTO iJoinReplacementFromSqlIsNull
	FROM   pg$mviews_oj_details
	WERE   join_replacement_from_sql IS NULL;

	IF iJoinReplacementFromSqlIsNull > 0 THEN
			RAISE EXCEPTION 'The UPDATE patch script 001_update_join_replacement_from_sql.sql has not successfully updated all the linking aliases for each mview in data dictionary table pg$mviews_oj_details column join_replacement_from_sql';
	END IF;
	
END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

SELECT V102_update_join_replacement_from_sql();

DROP FUNCTION V102_update_join_replacement_from_sql;
DROP FUNCTION v102_mv$outerJoinToInnerJoinReplacement;