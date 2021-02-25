CREATE OR REPLACE FUNCTION pgrs_mview.v102_mv$outerJoinToInnerJoinReplacement(
	pConst          IN      mv$allConstants,
	pTableNames 	IN		TEXT,
	pTableAlias 	IN		TEXT)
    RETURNS text
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v102_mv$outerJoinToInnerJoinReplacement
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

CREATE OR REPLACE FUNCTION V102_update_join_replacement_from_sql()
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

rConst              			mv$allConstants;

BEGIN

rConst      := mv$buildAllConstants();

SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name= 'pg$mviews_oj_details'
AND column_name='join_replacement_from_sql';

IF iColumnCnt = 1 THEN

	FOR rMviewsOjDetails IN (SELECT moj.owner,
									moj.view_name,
									moj.table_alias,
									m.table_names
							 FROM 	pg$mviews_oj_details moj
							 ,      pg$mviews m
							 WHERE  moj.view_name = m.view_name
							 AND 	moj.join_replacement_from_sql IS NULL) LOOP
							 
	tClauseJoinReplacement := v102_mv$outerJoinToInnerJoinReplacement(rConst, rMviewsOjDetails.table_names, rMviewsOjDetails.table_alias);
			
	UPDATE pg$mviews_oj_details
	SET join_replacement_from_sql = tClauseJoinReplacement
	WHERE view_name = rMviewsOjDetails.view_name
	AND table_alias = rMviewsOjDetails.table_alias;
						
	END LOOP;

	SELECT COUNT(1) INTO iJoinReplacementFromSqlIsNull
	FROM   pg$mviews_oj_details
	WHERE   join_replacement_from_sql IS NULL;

	IF iJoinReplacementFromSqlIsNull > 0 THEN
			RAISE EXCEPTION 'The UPDATE patch script V102_update_join_replacement_from_sql.sql has not successfully updated all the linking aliases for each mview in data dictionary table pg$mviews_oj_details column join_replacement_from_sql';
	END IF;
	
END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

SELECT V102_update_join_replacement_from_sql();

DROP FUNCTION V102_update_join_replacement_from_sql;
DROP FUNCTION v102_mv$outerJoinToInnerJoinReplacement;