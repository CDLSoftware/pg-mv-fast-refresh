CREATE OR REPLACE FUNCTION pgrs_mview.mv$outerJoinDeleteStatement(
	pConst          IN      mv$allConstants,
	pTableNames 	IN		TEXT,
	pTableAlias 	IN		TEXT,
	pViewName		IN      TEXT,
	pRowidArray		IN      TEXT[],
	pWhereClause	IN		TEXT,
	pTableName		IN		TEXT,
    pTableArray		IN		TEXT[],
	pAliasArray		IN		TEXT[])
    RETURNS text
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: mv$outerJoinDeleteStatement
Author:       David Day
Date:         23/02/2021
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
23/02/2021  | D Day      	| Initial version
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

	tAllROWIDs				TEXT;
	tROWIDs					TEXT;

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

BEGIN

tTablesSQL := regexp_replace(tTablesSQL,'left join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'left outer join','LEFT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right join','RIGHT JOIN','gi');
tTablesSQL := regexp_replace(tTablesSQL,'right outer join','RIGHT JOIN','gi');
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
tTablesMarkerSQL :=	mv$regexpreplace(tTablesMarkerSQL, ' ON ',pConst.ON_TOKEN);
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
			
			SELECT count(1) INTO iLeftJoinAliasCnt FROM regexp_matches(tLeftJoinLine,tTableName||'+[[:space:]]+'||tTableAlias,'g');

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
			
			SELECT count(1) INTO iRightJoinAliasCnt FROM regexp_matches(tRightJoinLine,tTableName||'+[[:space:]]+'||tTableAlias,'g');

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
			
	END IF;
	
ELSE

	IF tLeftColumnAlias = tTable_Alias THEN
	
		tOtherTableName := rec.table_name;
		tOtherAlias := tLeftColumnAlias;
		tJoinConditions := COALESCE(tLeftJoinConditions,tRightJoinConditions);
			
	END IF;

END IF;

END LOOP;

tJoinConditions := REPLACE(tJoinConditions,tTableAlias||'.','src$.');
tJoinConditions := REPLACE(tJoinConditions,tOtherAlias||'.','src$99.');

FOR rec IN (SELECT UNNEST(pRowidArray) rowid) LOOP
	
	tROWIDS := rec.rowid||',';	
	tAllROWIDs := CONCAT(tAllROWIDs,tROWIDS);
	
END LOOP;

tROWIDs := REPLACE(tROWIDs,'.','');

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

tJoinConditions := CONCAT(tJoinConditions,addWhereClauseJoinConditions);
	
tAllROWIDs := SUBSTR(tAllROWIDs,1,length(tAllROWIDs)-1);

tSQL := 'DELETE FROM '||pViewName||'
	  WHERE ctid IN ( SELECT 
							ctid
					  FROM
						  (SELECT ctid,
								  ROW_NUMBER() OVER(
                        PARTITION BY '||tAllROWIDs||'
						ORDER BY '||pTableAlias||'_m_row$ NULLS FIRST
						) r,
						COUNT(*) OVER(
						PARTITION BY '||tAllROWIDs||'
						) t_cnt,
						COUNT('||pTableAlias||'_m_row$) OVER(
						PARTITION BY '||tAllROWIDs||'
						) nonnull_cnt
						FROM
							'||pViewName||' mv$1
						WHERE
						'||tOtherAlias||'_m_row$ IN (
								SELECT src$99.m_row$
								FROM
									(
										SELECT sna$.m_row$ rid$,
											   sna$.*
										FROM
											'||tTableName||' sna$
										WHERE
											m_row$ IN (SELECT UNNEST(pRowIDArray))
									) src$,
								'||tOtherTableName||' src$99
								WHERE '||tJoinConditions||'
							)
						) mv$1
					WHERE
					t_cnt >= 1
					AND ( ( nonnull_cnt = 0
							AND r >= 1 )
							OR ( nonnull_cnt > 0
							   AND r <= t_cnt - nonnull_cnt ) ) )'; 


--RAISE INFO '%', tSQL;

RETURN tSQL;

END;
$BODY$
LANGUAGE    plpgsql;
