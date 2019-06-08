/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: generateTestDataPackages.sql
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
03/06/2019  | M Revitt      | Change so that it commits after every T1 row by calling from a bash script
12/11/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This script creates the SCHEMA and USER to hold the Materialized View Fast Refresh code along with the necessary
                data dictionary views

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Help:           Help can be invoked by running the rollowing command from within PostGre

                DO $$ BEGIN RAISE NOTICE '%', mv$stringConstants('HELP_TEXT'); END $$;

*************************************************************************************
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U mike_data -q -f generateTestData.sql

CREATE  OR  REPLACE
FUNCTION    loadParentData( pId  INTEGER )
    RETURNS VOID
AS
$DATA$
BEGIN

    INSERT  INTO
    t1(     code,   description          )
    values( pId,   'Desctiption ' || pId );

    RETURN;

END $DATA$
LANGUAGE  plpgsql;

CREATE  OR  REPLACE
FUNCTION    loadChildData( pId  INTEGER, pParentId  INTEGER )
    RETURNS VOID
AS
$DATA$
BEGIN
    INSERT  INTO
    t2(     code,   parent,     description                               )
    values( pId,    pParentId, 'Desctiption ' || pParentId || '.' || pId  );
    RETURN;
END $DATA$
LANGUAGE  plpgsql;

CREATE  OR  REPLACE
FUNCTION    loadGrandChildData( pId  INTEGER, pParentId  INTEGER, pGrandParentId  INTEGER )
    RETURNS VOID
AS
$DATA$
BEGIN
    INSERT  INTO
    t3(     code,   parent,     description                                                        )
    values( pId,    pParentId, 'Desctiption ' || pGrandParentId || '.' || pParentId || '.' || pId  );
    RETURN;
END $DATA$
LANGUAGE  plpgsql;

CREATE  OR  REPLACE
FUNCTION    loadTestData( pCurrId INTEGER, pMinId  INTEGER, pMAxId  INTEGER )
    RETURNS TEXT
AS
$DATA$
DECLARE

    cResult     CHAR(1);
    t1StartTime TIMESTAMP   := clock_timestamp();
    t2StartTime TIMESTAMP   := clock_timestamp();
    iT3Loops    INTEGER     := 0;
    iNoOfLoops  INTEGER     := 0;
    iT2Code     INTEGER;
    iT3Code     INTEGER;
    tOutput     TEXT;

BEGIN
    cResult     := loadParentData( pCurrId );
    iT3Loops    := 1;
    t2StartTime := clock_timestamp();

    FOR iT2 IN pMinId..pMaxId
    LOOP
        iT2Code     := ( pCurrId * pMAxId ) + iT2;
        cResult     := loadChildData( iT2Code, pCurrId );
        iNoOfLoops  := iNoOfLoops + 1;
        iT3Loops    := iT3Loops + 1;

        IF  MOD( pCurrId, 3 ) > 0
        AND MOD (iT2Code, 4 ) > 0
        THEN
            FOR iT3 IN pMinId..pMaxId
            LOOP
                iNoOfLoops  := iNoOfLoops + 1;
                iT3Loops    := iT3Loops + 1;
                iT3Code := ( pCurrId * pMAxId * pMaxId ) + ( iT2 * pMAxID ) + iT3;
                cResult := loadGrandChildData( iT3Code, iT2Code, pCurrId );
            END LOOP;
        END IF;
    END LOOP;
    tOutput :=   'Loop '    || TO_CHAR( pCurrId - pMinId + 1, '999' )   ||
                ' Created ' || TO_CHAR( iT3Loops, '999,990' )           || ' rows of data in ' ||
                  CLOCK_TIMESTAMP() - t2StartTime;
    RETURN tOutput;
END $DATA$
LANGUAGE  plpgsql;


