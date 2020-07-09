CREATE OR REPLACE PROCEDURE pgrs_mview.v402_updateMviewInnerJoinValues(
	pConst          IN      mv$allConstants,
	pTableNames 	IN		TEXT,
	pViewName		IN		TEXT,
	pOwner			IN      TEXT)
AS $BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v402_updateMviewInnerJoinValues
Author:       David Day
Date:         08/07/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
28/04/2020  | D Day      	| Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    Procedure to update existing data dictionary table pg$mviews new columns to support a defect fix to stop
				inner joining log table DML type INSERT changes to be duplicated.

Arguments:      IN		pConst
				IN      pTableNames 
				IN		pViewName
				IN		pOwner
Returns:                TEXT

************************************************************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
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
	
	aInnerAliasArray				TEXT[];
	aInnerRowidArray				TEXT[];
	
	aOuterLeftAliasArray			TEXT[];
	aOuterRightAliasArray			TEXT[];
	aLeftOuterJoinArray				TEXT[];
	aRightOuterJoinArray			TEXT[];
	
	aInnerJoinTableNameArray		TEXT[];
	aInnerJoinTableAliasArray				TEXT[];
	aInnerJoinTableRowidArray				TEXT[];
	aInnerJoinOtherTableNameArray			TEXT[];	
	aInnerJoinOtherTableAliasArray			TEXT[];
	aInnerJoinOtherTableRowidArray			TEXT[];
	
	aOuterTableArray						TEXT[];
	
	aTableArray								TEXT[];
	aAliasArray								TEXT[];
	aRowidArray								TEXT[];
	
	

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

        aTableArray[iTableArryPos]  := (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[1];
        tTableAlias                 := (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[2];
        aAliasArray[iTableArryPos]  :=  COALESCE( NULLIF( NULLIF( tTableAlias, pConst.EMPTY_STRING), pConst.ON_TOKEN),
                                                                  aTableArray[iTableArryPos] ) || pConst.DOT_CHARACTER;
		aRowidArray[iTableArryPos]  :=  mv$createRow$Column( pConst, aAliasArray[iTableArryPos] );
		
		IF tInnerJoin = 'Y' THEN
		
			tInnerJoinTableName		:= (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[1];
			tInnerJoinTableAlias    := (REGEXP_SPLIT_TO_ARRAY( tTableName,  pConst.REGEX_MULTIPLE_SPACES ))[2];
			tInnerJoinTableAlias  	:=  COALESCE( NULLIF( NULLIF( tInnerJoinTableAlias, pConst.EMPTY_STRING), pConst.ON_TOKEN),
																	  aTableArray[iTableArryPos] ) || pConst.DOT_CHARACTER;
																	  
			SELECT (CASE WHEN tInnerJoinTableAlias = tInnerLeftAlias THEN tInnerRightAlias
					ELSE tInnerLeftAlias END) INTO tInnerJoinOtherTableAlias;
					
			SELECT inline.table_name
			FROM (
					SELECT 	UNNEST(aTableArray) AS table_name
					,		UNNEST(aAliasArray) AS table_alias) inline
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
																	  aTableArray[iTableArryPos] ) || pConst.DOT_CHARACTER;
			tInnerJoinTableRowid		:= mv$createRow$Column( pConst, tInnerJoinTableAlias );
			
			tInnerJoinOtherTableName 	:= 'none';
			tInnerJoinOtherTableRowid	:= 'none';
			tInnerJoinOtherTableAlias	:= 'none';

		END IF;
			

        aOuterTableArray[iTableArryPos]  :=(REGEXP_SPLIT_TO_ARRAY( tOuterTable, pConst.REGEX_MULTIPLE_SPACES ))[1];

        tTableNames     := TRIM( SUBSTRING( tTableNames,
                                 POSITION( pConst.COMMA_CHARACTER IN tTableNames ) + LENGTH( pConst.COMMA_CHARACTER )));
								 
		aInnerAliasArray[iTableArryPos] 		:= tInnerAlias;
		aInnerRowidArray[iTableArryPos]			:= tInnerRowid;
		
		aOuterLeftAliasArray[iTableArryPos] 	:= tOuterLeftAlias;
		aOuterRightAliasArray[iTableArryPos] 	:= tOuterRightAlias;
		aLeftOuterJoinArray[iTableArryPos] 		:= tLeftOuterJoin;
		aRightOuterJoinArray[iTableArryPos] 	:= tRightOuterJoin;
		
		aInnerJoinTableNameArray[iTableArryPos]  := tInnerJoinTableName;
		aInnerJoinTableAliasArray[iTableArryPos] := tInnerJoinTableAlias;
		aInnerJoinTableRowidArray[iTableArryPos] := tInnerJoinTableRowid;	
		aInnerJoinOtherTableNameArray[iTableArryPos] := tInnerJoinOtherTableName;		
		aInnerJoinOtherTableAliasArray[iTableArryPos] := tInnerJoinOtherTableAlias;
		aInnerJoinOtherTableRowidArray[iTableArryPos] := tInnerJoinOtherTableRowid;			
		
        iTableArryPos   := iTableArryPos + 1;

    END LOOP;
	
	UPDATE pg$mviews_oj_details
	SET inner_join_table_array = aInnerJoinTableNameArray
	,	inner_join_alias_array = aInnerJoinTableAliasArray
	,	inner_join_rowid_array = aInnerJoinTableRowidArray
	,	inner_join_other_table_array = aInnerJoinOtherTableNameArray
	,	inner_join_other_alias_array = aInnerJoinOtherTableAliasArray
	,	inner_join_other_rowid_array = paInnerJoinOtherTableRowidArray
	WHERE view_name = pViewName
	AND	  owner = pOwner;

    EXCEPTION
    WHEN OTHERS
    THEN
        RAISE INFO      'Exception in procedure v402_updateMviewInnerJoinValues';
        RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
        RAISE INFO      'Error Context:% %',CHR(10),  tTableNames;
        RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

CREATE OR REPLACE PROCEDURE v402_actionUpdateMviewInnerJoinValues()
AS
$BODY$

/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: v402_actionUpdateMviewInnerJoinValues
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
rMviews							RECORD;
iInnerJoinTableArrayIsNull		INTEGER := 0;
rConst              			mv$allConstants;

BEGIN

rConst      := mv$buildAllConstants();

SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name= 'pg$mviews'
AND column_name='inner_join_table_array';

IF iColumnCnt = 1 THEN

	FOR rMviews IN (SELECT owner,
									view_name,
									table_names
							 FROM 	pg$mviews
							 WHERE  inner_join_table_array IS NULL) LOOP
							 
	CALL v402_updateMviewInnerJoinValues(rConst, rMviews.table_names, rMviews.view_name, rMviews.owner);
						
	END LOOP;

	SELECT COUNT(1) INTO iInnerJoinTableArrayIsNull
	FROM   pg$mviews
	WHERE  inner_join_table_array IS NULL;

	IF iInnerJoinTableArrayIsNull > 0 THEN
			RAISE EXCEPTION 'The UPDATE patch script V402_update_pgmviews_new_columns.sql has not successfully updated all the linking inner joins values for each mview in data dictionary table pg$mviews new columns';
	END IF;
	
END IF;

END;
$BODY$
LANGUAGE    plpgsql
SECURITY    DEFINER;

CALL v402_actionUpdateMviewInnerJoinValues();

DROP PROCEDURE v402_updateMviewInnerJoinValues;
DROP PROCEDURE v402_actionUpdateMviewInnerJoinValues;