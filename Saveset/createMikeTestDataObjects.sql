/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: CreateMikeTestData.sql
Author:       Mike Revitt
Date:         14/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
14/11/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    Sample script to create the test database objects, T1, T2 and T3 as Parent, Child, Grandchild


Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

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

-- psql -h localhost -p 5432 -d postgres -U mike_pgmview -q -f createMikeTestData.sql

SET CLIENT_MIN_MESSAGES = NOTICE;

\prompt "Create user to own the base tables" mike
CREATE  USER        mike_data       WITH    PASSWORD    'aws-oracle';
CREATE  USER        mike_view       WITH    PASSWORD    'aws-oracle';

ALTER   USER        mike_data       SET     SEARCH_PATH=mike_data,PUBLIC,mike_pgmview,mike_view;
ALTER   USER        mike_view       SET     SEARCH_PATH=mike_view,PUBLIC,mike_pgmview,mike_data;
ALTER   USER        mike_pgmview    SET     SEARCH_PATH=mike_pgmview,PUBLIC,mike_data,mike_view;

GRANT   mike_view,  mike_data       TO      mike;
GRANT   mike_view,  mike_data       TO      mike_pgmview;

GRANT   pgmv$_view, pgmv$_usage                     TO      mike_view;
GRANT   pgmv$_view, pgmv$_usage,    pgmv$_execute   TO      mike_data;

CREATE  SCHEMA  mike_data       AUTHORIZATION   mike_data;
CREATE  SCHEMA  mike_view       AUTHORIZATION   mike_view;

ALTER   DATABASE    postgres        SET     SEARCH_PATH="$user",PUBLIC,mike_pgmview,mike_data,mike_view;

\c postgres mike_data

\prompt "Create base tables t1, t2 and t3" mike
CREATE  TABLE
        t1
(
    code            INTEGER NOT NULL    PRIMARY     KEY,
    created         DATE    NOT NULL    DEFAULT     clock_timestamp(),
    description     TEXT        NULL,
    updated         DATE        NULL
);

CREATE  OR  REPLACE
FUNCTION    updateT1Updated()
    RETURNS TRIGGER
AS
$TRIG$
BEGIN
    NEW.updated = clock_timestamp();
    RETURN  NEW;
END;
$TRIG$
LANGUAGE  plpgsql;

CREATE  TRIGGER updateT1UpdatedCol
    BEFORE  UPDATE  ON  t1
    FOR     EACH    ROW
    EXECUTE PROCEDURE   updateT1Updated();

CREATE  TABLE
        t2
(
    code            INTEGER NOT NULL    PRIMARY     KEY,
    parent          INTEGER NOT NULL    REFERENCES  t1,
    created         DATE    NOT NULL    DEFAULT     clock_timestamp(),
    description     TEXT        NULL,
    updated         DATE        NULL
);

CREATE  OR  REPLACE
FUNCTION    updateT2Updated()
    RETURNS TRIGGER
AS
$TRIG$
BEGIN
    NEW.updated = clock_timestamp();
    RETURN  NEW;
END;
$TRIG$
LANGUAGE  plpgsql;

CREATE  TRIGGER updateT2UpdatedCol
    BEFORE  UPDATE  ON  t2
    FOR     EACH    ROW
    EXECUTE PROCEDURE   updateT2Updated();

CREATE  TABLE
        t3
(
    code            INTEGER NOT NULL    PRIMARY     KEY,
    parent          INTEGER NOT NULL    REFERENCES  t2,
    created         DATE    NOT NULL    DEFAULT     clock_timestamp(),
    description     TEXT        NULL,
    updated         DATE        NULL
);

CREATE  OR  REPLACE
FUNCTION    updateT3Updated()
    RETURNS TRIGGER
AS
$TRIG$
BEGIN
    NEW.updated = clock_timestamp();
    RETURN  NEW;
END;
$TRIG$
LANGUAGE  plpgsql;

CREATE  TRIGGER updateT3UpdatedCol
    BEFORE  UPDATE  ON  t3
    FOR     EACH    ROW
    EXECUTE PROCEDURE   updateT3Updated();

\c postgres mike_pgmview

GRANT   USAGE   ON                      SCHEMA  mike_data, mike_view   TO  pgmv$_usage;
GRANT   SELECT  ON  ALL TABLES      IN  SCHEMA  mike_data, mike_view   TO  pgmv$_view;
ALTER   DEFAULT PRIVILEGES          IN  SCHEMA  mike_data
                GRANT   SELECT      ON  TABLES                         TO  pgmv$_view;

