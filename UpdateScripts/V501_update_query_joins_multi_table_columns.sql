CREATE OR REPLACE
PROCEDURE    V501_update_query_joins_multi_table_columns()
AS
$BODY$
/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: V501_update_query_joins_multi_table_columns
Author:       David Day
Date:         27/10/2020
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
27/10/2020  | D Day         | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Description:    One-off procedure to update existing builds with new columns query_joins_multi_table_cnt_array and 
				query_joins_multi_table_pos_array including populating array columns with values for each mview.

Arguments:      None
				
************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier: MIT-0
***********************************************************************************************************************************/
DECLARE

    rTableNames       	RECORD;
	
	rPgMviews			RECORD;
	
	iTableCount		  	INTEGER;
	
	iCounter		  	INTEGER := 0;
	
	tTableArray  		TEXT[] := '{}';
	
	iMultiTableCount 	INTEGER := 0;
	
	iColumnCnt			INTEGER := 0;
	
	iQueryJoinsMultiTabPosArray			SMALLINT[] := '{}';
	
	iQueryJoinsMultiTabCntArray			SMALLINT[] := '{}';
	

BEGIN

SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name= 'pg$mviews'
AND column_name='query_joins_multi_table_cnt_array' AND column_name='query_joins_multi_table_pos_array';

IF iColumnCnt = 0 THEN

	EXECUTE 'ALTER table pg$mviews ADD COLUMN query_joins_multi_table_cnt_array	SMALLINT[], ADD COLUMN query_joins_multi_table_pos_array SMALLINT[]';

END IF;

SELECT count(1) INTO iColumnCnt
FROM information_schema.columns 
WHERE table_name= 'pg$mviews'
AND column_name='query_joins_multi_table_cnt_array' AND column_name='query_joins_multi_table_pos_array';

IF iColumnCnt = 2 THEN

FOR rPgMviews IN (SELECT owner, view_name, table_array, alias_array
					FROM pg$mviews
					WHERE query_joins_multi_table_cnt_array IS NULL AND query_joins_multi_table_pos_array IS NULL) LOOP
					
					
	FOR rTableNames IN (SELECT UNNEST(rPgMviews.table_array) AS table_name, UNNEST(rPgMviews.alias_array) AS alias_name) LOOP
	
		iCounter 	:= iCounter +1;
	
		SELECT count(1) INTO iTableCount
		FROM (SELECT UNNEST(pTableNames) AS table_name) inline
		WHERE inline.table_name = rTableNames.table_name;
		
		tTableArray[iCounter] := rTableNames.table_name;
		
		IF iTableCount = 1 THEN
		
			iQueryJoinsMultiTabPosArray[iCounter] := 1;
			
		ELSE
		
			SELECT count(1) INTO iMultiTableCount
			FROM (SELECT UNNEST(tTableArray) AS table_name) inline
			WHERE inline.table_name = rTableNames.table_name;
		
			iQueryJoinsMultiTabPosArray[iCounter] := iMultiTableCount;		
		
		END IF;
		
		SELECT count(1) INTO iTableCount
		FROM (SELECT UNNEST(rPgMviews.table_array) AS table_name) inline
		WHERE inline.table_name = rTableNames.table_name;
		
		iQueryJoinsMultiTabCntArray[iCounter] := iTableCount;
		
	END LOOP;
	
	UPDATE pg$mviews a
	SET a.query_joins_multi_table_cnt_array = iQueryJoinsMultiTabCntArray
	,   a.query_joins_multi_table_pos_array = iQueryJoinsMultiTabPosArray
	WHERE a.view_name = rPgMviews.view_name;
	
	iQueryJoinsMultiTabCntArray := '{}';
	iQueryJoinsMultiTabPosArray := '{}';
	tTableArray					:= '{}';
	iCounter := 0;
	
END LOOP;

EXCEPTION
WHEN OTHERS
THEN
	RAISE INFO      'Exception in procedure V501_update_query_joins_multi_table_columns';
	RAISE INFO      'Error %:- %:',     SQLSTATE, SQLERRM;
	RAISE EXCEPTION '%',                SQLSTATE;
END;
$BODY$
LANGUAGE    plpgsql;

CALL V501_update_query_joins_multi_table_columns();

DROP PROCEDURE V501_update_query_joins_multi_table_columns;