CREATE OR REPLACE
FUNCTION    pgrs_mview.V502_mv$extractCompoundViewTables
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
				pInnerJoinOtherTableRowidArray	OUT TEXT[]
            )
    RETURNS RECORD
AS
$BODY$
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
	
	--CALL mv$setQueryJoinsMultiTablePosition(pTableArray,pInnerAliasArray,pQueryJoinsMultiTabPosArray);
	--CALL mv$setQueryJoinsMultiTableCount(pTableArray,pInnerAliasArray,pQueryJoinsMultiTabCntArray);	

    RETURN;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in function V502_mv$extractCompoundViewTables';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tTableNames;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;

CREATE OR REPLACE
PROCEDURE    pgrs_mview.v502_update_pgmviews_oj_details_column_update_sql()
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v502_update_pgmviews_oj_details_column_update_sql
Author:       David Day
Date:         28/10/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
28/10/2020  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    One-off procedure to update the affected data dictionary values held in table pg$mviews_oj_details column update_sql.

Arguments:      None

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rConst              			mv$allConstants;
	
	rMain							RECORD;
	
	iColumnNameAliasCnt				INTEGER DEFAULT 0;	
	
	tRegexp_rowid					TEXT;
	tSelectColumns					TEXT;
	tColumnNameAlias				TEXT;
	tRegExpColumnNameAlias			TEXT;
	tColumnNameArray 				TEXT[];	
	tColumnNameSql 					TEXT;
	tMvColumnName					TEXT;
	tTableName						TEXT;
	tMvRowidColumnName				TEXT;
	iMvColumnNameExists				INTEGER DEFAULT 0;	
	iMvColumnNameLoopCnt			INTEGER DEFAULT 0;
	
	tUpdateSetSql					TEXT;
	tSqlStatement					TEXT;
	tWhereClause					TEXT;

    rPgMviewColumnNames     		RECORD;
	rMvOuterJoinDetails				RECORD;
	rAliasJoinLinks					RECORD;
	rBuildAliasArray				RECORD;
	rMainAliasArray					RECORD;
	rLeftOuterJoinAliasArray		RECORD;
	rRightJoinAliasArray			RECORD;
	rRightOuterJoinAliasArray		RECORD;
	rLeftJoinAliasArray				RECORD;
	
	iWhileCounter					INTEGER DEFAULT 0;
	iAliasJoinLinksCounter			INTEGER DEFAULT 0;	
	iMainLoopCounter				INTEGER DEFAULT 0;
	iWhileLoopCounter			    INTEGER DEFAULT 0;
	iLoopCounter					INTEGER DEFAULT 0;
	iRightLoopCounter				INTEGER DEFAULT 0;
	iLeftAliasLoopCounter			INTEGER DEFAULT 0;
	iRightAliasLoopCounter			INTEGER DEFAULT 0;
	iLeftLoopCounter				INTEGER DEFAULT 0;
	iColumnNameAliasLoopCnt			INTEGER DEFAULT 0;
	
	tOuterJoinAlias					TEXT;	
	tAlias							TEXT;	
	
	tParentToChildAliasArray		TEXT[];	
	tAliasArray						TEXT[];
	tMainAliasArray					TEXT[];
	tRightJoinAliasArray			TEXT[];
	tBuildAliasArray				TEXT[];
	tLeftJoinAliasArray				TEXT[];
	
	tRightJoinAliasExists			TEXT DEFAULT 'N';	
	tLeftJoinAliasExists			TEXT DEFAULT 'N';
	
	tIsTrueOrFalse					TEXT;
	
	tClauseJoinReplacement			TEXT;

-- Added compound variables

    tRowidArray         	TEXT[];
    tTableArray         	TEXT[];
    xtAliasArray         	TEXT[];
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
	
BEGIN

    rConst      := mv$buildAllConstants();

	FOR rMain IN (SELECT * FROM pg$mviews WHERE view_name IN (SELECT DISTINCT view_name
															  FROM pg$mviews_oj_details
															  WHERE update_sql NOT LIKE '% SET %' OR update_sql IS NULL)) LOOP
															  
															  
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
				pInnerJoinOtherTableRowidArray

		FROM
				pgrs_mview.V502_mv$extractCompoundViewTables( rConst, rMain.table_names )
		INTO
				tTableArray,
				xtAliasArray,
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
				tInnerJoinOtherTableRowidArray;
				
		FOR rMvOuterJoinDetails IN (SELECT inline.oj_table AS table_name
								,      inline.oj_table_alias AS table_name_alias
								,	   inline.oj_rowid AS rowid_column_name
								,      inline.oj_outer_left_alias AS outer_left_alias
								,      inline.oj_outer_right_alias AS outer_right_alias
								,      inline.oj_left_outer_join AS left_outer_join
								,      inline.oj_right_outer_join AS right_outer_join
								FROM (
									SELECT 	UNNEST(tOuterTableArray) AS oj_table
									, 		UNNEST(xtAliasArray) AS oj_table_alias
									, 		UNNEST(tRowidArray) AS oj_rowid
								    ,       UNNEST(tOuterLeftAliasArray) AS oj_outer_left_alias
									,		UNNEST(tOuterRightAliasArray) AS oj_outer_right_alias
									,		UNNEST(tLeftOuterJoinArray) AS oj_left_outer_join
									,		UNNEST(tRightOuterJoinArray) AS oj_right_outer_join) inline
								WHERE inline.oj_table IS NOT NULL) LOOP	
	
			iMainLoopCounter := iMainLoopCounter +1;		
			tOuterJoinAlias := TRIM(REPLACE(rMvOuterJoinDetails.table_name_alias,'.',''));
			iWhileLoopCounter := 0;
			iWhileCounter := 0;	
			tParentToChildAliasArray[iMainLoopCounter] := tOuterJoinAlias;
			tAliasArray[iMainLoopCounter] := tOuterJoinAlias;
											
			WHILE iWhileCounter = 0 LOOP
			
				IF rMvOuterJoinDetails.left_outer_join = rConst.LEFT_OUTER_JOIN THEN			
				
					iWhileLoopCounter := iWhileLoopCounter +1;
					tMainAliasArray := '{}';
					
					IF tAliasArray <> '{}' THEN
				
						tMainAliasArray[iWhileLoopCounter] := tAliasArray;
		
						FOR rMainAliasArray IN (SELECT UNNEST(tMainAliasArray) AS left_alias) LOOP
						
							tOuterJoinAlias := TRIM(REPLACE(rMainAliasArray.left_alias,'{',''));
							tOuterJoinAlias := TRIM(REPLACE(tOuterJoinAlias,'}',''));
							iLeftAliasLoopCounter := 0;
						
							FOR rLeftOuterJoinAliasArray IN (SELECT UNNEST(tOuterLeftAliasArray) as left_alias) LOOP
					
								IF rLeftOuterJoinAliasArray.left_alias = tOuterJoinAlias THEN
									iLeftAliasLoopCounter := iLeftAliasLoopCounter +1;
								END IF;
				
							END LOOP;
							
							IF iLeftAliasLoopCounter > 0 THEN 
									
								SELECT 	pChildAliasArray 
								FROM 	mv$checkParentToChildOuterJoinAlias(
																	rConst
															,		tOuterJoinAlias
															,		rMvOuterJoinDetails.left_outer_join
															,		tOuterLeftAliasArray
															,		tOuterRightAliasArray
															,		tLeftOuterJoinArray) 
								INTO	tRightJoinAliasArray;

								IF tRightJoinAliasArray = '{}' THEN
									tRightJoinAliasExists := 'N';
									--RAISE INFO 'No Left Aliases Match Right Aliases';
								ELSE
									iRightLoopCounter := 0;

									FOR rRightJoinAliasArray IN (SELECT UNNEST(tRightJoinAliasArray) as right_join_alias) LOOP
										
										iRightLoopCounter := iRightLoopCounter +1;
										iMainLoopCounter := iMainLoopCounter +1;
										tParentToChildAliasArray[iMainLoopCounter] := rRightJoinAliasArray.right_join_alias;
										tRightJoinAliasExists := 'Y';
										tBuildAliasArray[iRightLoopCounter] := rRightJoinAliasArray.right_join_alias;

									END LOOP;
								END IF;

								IF (tRightJoinAliasArray <> '{}' OR tRightJoinAliasExists = 'Y') THEN

									tAliasArray := '{}';

									FOR rBuildAliasArray IN (SELECT UNNEST(tBuildAliasArray) AS right_join_alias) LOOP

										iLoopCounter = iLoopCounter +1;
										iLeftAliasLoopCounter := 0;

										FOR rLeftOuterJoinAliasArray IN (SELECT UNNEST(tOuterLeftAliasArray) AS left_alias) LOOP

											IF rMainAliasArray.left_alias = rBuildAliasArray.right_join_alias THEN
												iLeftAliasLoopCounter := iLeftAliasLoopCounter +1;
											END IF;

										END LOOP;

										IF iLeftAliasLoopCounter > 0 THEN
											tAliasArray[iLoopCounter] := rBuildAliasArray.right_join_alias;
										END IF;

									END LOOP;

								ELSE

									tRightJoinAliasExists = 'N';
									tRightJoinAliasArray = '{}';
									tAliasArray = '{}';

								END IF;

							ELSE
							
								tRightJoinAliasExists = 'N';
								tRightJoinAliasArray = '{}';
								tAliasArray = '{}';					
							
							END IF;
							
						END LOOP;
					
					ELSE
						iWhileCounter := 1;	
					END IF;
					
				ELSIF rMvOuterJoinDetails.right_outer_join = rConst.RIGHT_OUTER_JOIN THEN
				
					iWhileLoopCounter := iWhileLoopCounter +1;
					tMainAliasArray := '{}';
					
					IF tAliasArray <> '{}' THEN
				
						tMainAliasArray[iWhileLoopCounter] := tAliasArray;
		
						FOR rMainAliasArray IN (SELECT UNNEST(tMainAliasArray) AS right_alias) LOOP
						
							tOuterJoinAlias := TRIM(REPLACE(rMainAliasArray.right_alias,'{',''));
							tOuterJoinAlias := TRIM(REPLACE(tOuterJoinAlias,'}',''));
							iRightAliasLoopCounter := 0;
						
							FOR rRightOuterJoinAliasArray IN (SELECT UNNEST(tOuterRightAliasArray) as right_alias) LOOP
					
								IF rRightOuterJoinAliasArray.right_alias = tOuterJoinAlias THEN
									iRightAliasLoopCounter := iRightAliasLoopCounter +1;
								END IF;
				
							END LOOP;
							
							IF iRightAliasLoopCounter > 0 THEN 
									
								SELECT 	pChildAliasArray 
								FROM 	mv$checkParentToChildOuterJoinAlias(
																	rConst
															,		tOuterJoinAlias
															,		rMvOuterJoinDetails.right_outer_join
															,		tOuterLeftAliasArray
															,		tOuterRightAliasArray
															,		tRightOuterJoinArray) 
								INTO	tLeftJoinAliasArray;

								IF tLeftJoinAliasArray = '{}' THEN
									tLeftJoinAliasExists := 'N';
									--RAISE INFO 'No Right Aliases Match Left Aliases';
								ELSE
									iLeftLoopCounter := 0;

									FOR rLeftJoinAliasArray IN (SELECT UNNEST(tLeftJoinAliasArray) as left_join_alias) LOOP
										
										iLeftLoopCounter := iLeftLoopCounter +1;
										iMainLoopCounter := iMainLoopCounter +1;
										tParentToChildAliasArray[iMainLoopCounter] := rLeftJoinAliasArray.left_join_alias;
										tLeftJoinAliasExists := 'Y';
										tBuildAliasArray[iLeftLoopCounter] := rLeftJoinAliasArray.left_join_alias;

									END LOOP;
								END IF;

								IF (tLeftJoinAliasArray <> '{}' OR tLeftJoinAliasExists = 'Y') THEN

									tAliasArray := '{}';

									FOR rBuildAliasArray IN (SELECT UNNEST(tBuildAliasArray) AS left_join_alias) LOOP

										iLoopCounter = iLoopCounter +1;
										iRightAliasLoopCounter := 0;

										FOR rRightOuterJoinAliasArray IN (SELECT UNNEST(tOuterRightAliasArray) AS right_alias) LOOP

											IF rMainAliasArray.right_alias = rBuildAliasArray.left_join_alias THEN
												iRightAliasLoopCounter := iRightAliasLoopCounter +1;
											END IF;

										END LOOP;

										IF iRightAliasLoopCounter > 0 THEN
											tAliasArray[iLoopCounter] := rBuildAliasArray.left_join_alias;
										END IF;

									END LOOP;

								ELSE

									tLeftJoinAliasExists = 'N';
									tLeftJoinAliasArray = '{}';
									tAliasArray = '{}';

								END IF;

							ELSE
							
								tLeftJoinAliasExists = 'N';
								tLeftJoinAliasArray = '{}';
								tAliasArray = '{}';					
							
							END IF;
							
						END LOOP;
					
					ELSE
						iWhileCounter := 1;	
					END IF;
				
				END IF;
				
			END LOOP;
			
			-- Key values for the main UPDATE statement breakdown
			tMvRowidColumnName 		:= rMvOuterJoinDetails.rowid_column_name;
			tWhereClause 			:= rConst.WHERE_COMMAND || tMvRowidColumnName  || rConst.IN_ROWID_LIST;
			tColumnNameAlias 		:= rMvOuterJoinDetails.table_name_alias;
			tTableName 				:= rMvOuterJoinDetails.table_name;
			tColumnNameArray	 	:= '{}';
			tUpdateSetSql 		 	:= ' ';
			iMvColumnNameLoopCnt 	:= 0;
			iAliasJoinLinksCounter 	:= 0;
			iColumnNameAliasLoopCnt := 0;
			
			-- Building the UPDATE statement including any child relationship columns and m_row$ based on these aliases
			FOR rAliasJoinLinks IN (SELECT UNNEST(tParentToChildAliasArray) AS alias) LOOP
			
				IF rAliasJoinLinks.alias IS NOT NULL THEN
			
					iAliasJoinLinksCounter 	:= iAliasJoinLinksCounter +1;
					tAlias 					:= rAliasJoinLinks.alias||'.';
					tSelectColumns 			:= SUBSTRING(rMain.select_columns,1,mv$regExpInstr(rMain.select_columns,'[,]+[[:alnum:]]+[.]+'||'m_row\$'||''));
					tRegExpColumnNameAlias 	:= REPLACE(tAlias,'.','\.');
					iColumnNameAliasCnt 	:= mv$regExpCount(tSelectColumns, '[A-Za-z0-9]+('||tRegExpColumnNameAlias||')', 1);
					iColumnNameAliasCnt 	:= mv$regExpCount(tSelectColumns, '('||tRegExpColumnNameAlias||')', 1) - iColumnNameAliasCnt;
				
					IF iColumnNameAliasCnt > 0 THEN
				
						FOR i IN 1..iColumnNameAliasCnt 
						LOOP
						
							tColumnNameSql := SUBSTRING(tSelectColumns,mv$regExpInstr(tSelectColumns,
									 '([^A-Za-z0-9]+('||tRegExpColumnNameAlias||'))',
									 1,
									 i)-1);
							tColumnNameSql := mv$regExpReplace(tColumnNameSql,'(^[[:space:]]+)',null);
							tColumnNameSql := mv$regExpSubstr((tColumnNameSql),'(.*'||tRegExpColumnNameAlias||'+[[:alnum:]]+(.*?[^,|$]))',1,1,'i');
							tColumnNameSql := mv$regExpReplace(tColumnNameSql,'\s+$','');
							tMvColumnName  := TRIM(REPLACE(mv$regExpSubstr(tColumnNameSql, '\S+$'),',',''));
							tMvColumnName  := LOWER(TRIM(REPLACE(tMvColumnName,tAlias,'')));
							
							FOR rPgMviewColumnNames IN (SELECT column_name
														FROM   information_schema.columns
														WHERE  table_schema    = LOWER( rMain.owner )
														AND    table_name      = LOWER( rMain.view_name ) )
							LOOP
							
								IF rPgMviewColumnNames.column_name = tMvColumnName THEN
												
									iMvColumnNameLoopCnt := iMvColumnNameLoopCnt + 1;	

									-- Check for duplicates
									SELECT tMvColumnName = ANY (tColumnNameArray) INTO tIsTrueOrFalse;	
									
									IF tIsTrueOrFalse = 'false' THEN

										iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;	
										
										tColumnNameArray[iColumnNameAliasLoopCnt] := tMvColumnName;
										
										IF iMvColumnNameLoopCnt = 1 THEN 	
											tUpdateSetSql := rConst.SET_COMMAND || tMvColumnName || rConst.EQUALS_NULL || rConst.COMMA_CHARACTER;
										ELSE	
											tUpdateSetSql := tUpdateSetSql || tMvColumnName || rConst.EQUALS_NULL || rConst.COMMA_CHARACTER ;
										END IF;
										
									END IF;
								
									EXIT WHEN iMvColumnNameLoopCnt > 0;
									
								END IF;

							END LOOP;
							
						END LOOP;
						
						iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;
						tColumnNameArray[iColumnNameAliasLoopCnt] := rAliasJoinLinks.alias|| rConst.UNDERSCORE_CHARACTER || rConst.MV_M_ROW$_COLUMN;
						tUpdateSetSql := tUpdateSetSql || rAliasJoinLinks.alias|| rConst.UNDERSCORE_CHARACTER || rConst.MV_M_ROW$_COLUMN || rConst.EQUALS_NULL || rConst.COMMA_CHARACTER;
						
					ELSE
						IF iAliasJoinLinksCounter = 1 THEN
							iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;
							tColumnNameArray[iColumnNameAliasLoopCnt] := rAliasJoinLinks.alias|| rConst.UNDERSCORE_CHARACTER || rConst.MV_M_ROW$_COLUMN;
							tUpdateSetSql := rConst.SET_COMMAND || rAliasJoinLinks.alias|| rConst.UNDERSCORE_CHARACTER || rConst.MV_M_ROW$_COLUMN || rConst.EQUALS_NULL || rConst.COMMA_CHARACTER;			
						ELSE
							iColumnNameAliasLoopCnt := iColumnNameAliasLoopCnt + 1;
							tColumnNameArray[iColumnNameAliasLoopCnt] := rAliasJoinLinks.alias|| rConst.UNDERSCORE_CHARACTER || rConst.MV_M_ROW$_COLUMN;
							tUpdateSetSql := tUpdateSetSql || rAliasJoinLinks.alias || rConst.UNDERSCORE_CHARACTER || rConst.MV_M_ROW$_COLUMN || rConst.EQUALS_NULL || rConst.COMMA_CHARACTER;		
						END IF;
							
					END IF;
				
				END IF;
			
			END LOOP;
			
			tUpdateSetSql := SUBSTRING(tUpdateSetSql,1,length(tUpdateSetSql)-1);
			
			tSqlStatement := rConst.UPDATE_COMMAND ||
							 rMain.owner		|| rConst.DOT_CHARACTER		|| rMain.view_name	|| rConst.NEW_LINE		||
							 tUpdateSetSql || rConst.NEW_LINE ||
							 tWhereClause;
			
			UPDATE pg$mviews_oj_details
			SET update_sql = tSqlStatement
			,   column_name_array = tColumnNameArray
			WHERE view_name = rMain.view_name
			AND   owner		= rMain.owner
			AND   table_alias = tColumnNameAlias
			AND   rowid_column_name = tMvRowidColumnName;
		
			iMainLoopCounter := 0;
			tParentToChildAliasArray := '{}';
			tAliasArray  := '{}';
			tMainAliasArray := '{}';
			iWhileCounter := 0;
			iWhileLoopCounter := 0;
			iLoopCounter := 0;
		
	END LOOP;
	
    tRowidArray  			:= '{}';
    tTableArray  			:= '{}';
    xtAliasArray 			:= '{}';
    tOuterTableArray    	:= '{}';
    tInnerAliasArray    	:= '{}';
    tInnerRowidArray    	:= '{}';
	tOuterLeftAliasArray 	:= '{}';
	tOuterRightAliasArray 	:= '{}';
	tLeftOuterJoinArray 	:= '{}';
	tRightOuterJoinArray 	:= '{}';
	tInnerJoinTableNameArray	:= '{}';
	tInnerJoinTableAliasArray	:= '{}';
	tInnerJoinTableRowidArray	:= '{}';
	tInnerJoinOtherTableNameArray	:= '{}';
	tInnerJoinOtherTableAliasArray	:= '{}';
	tInnerJoinOtherTableRowidArray	:= '{}';
	
END LOOP;

EXCEPTION
WHEN OTHERS
THEN
	RAISE INFO      'Exception in procedure v502_update_pgmviews_oj_details_column_update_sql';
	RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
	RAISE INFO      'Error Context:% %',CHR(10),  tSqlStatement;
	RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;

CALL v502_update_pgmviews_oj_details_column_update_sql();

DROP FUNCTION V502_mv$extractCompoundViewTables;
DROP PROCEDURE v502_update_pgmviews_oj_details_column_update_sql;

