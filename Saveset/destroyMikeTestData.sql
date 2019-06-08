/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: CreateMikeTestData.sql
Author:       Mike Revitt
Date:         08/04/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
08/04/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    Remove Mike Test Data objects

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

-- psql -h localhost -p 5432 -d postgres -U mike_data -q -f createMikeTestData.sql

SET CLIENT_MIN_MESSAGES = ERROR;

DROP        VIEW    IF  EXISTS  mike_data.mv1;
DROP        VIEW    IF  EXISTS  mike_data.mv2;
DROP        VIEW    IF  EXISTS  mike_data.mv3;
DROP        VIEW    IF  EXISTS  mike_data.mv4;

DROP        TRIGGER IF  EXISTS  trig$_t1        ON  mike_data.t1;
DROP        TRIGGER IF  EXISTS  trig$_t2        ON  mike_data.t2;
DROP        TRIGGER IF  EXISTS  trig$_t3        ON  mike_data.t3;

DROP        TABLE   IF  EXISTS  mike_data.pgmv$_mv1 CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.pgmv$_mv2 CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.pgmv$_mv3 CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.pgmv$_mv4 CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.log$_t1   CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.log$_t2   CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.log$_t3   CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.t1        CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.t2        CASCADE;
DROP        TABLE   IF  EXISTS  mike_data.t3        CASCADE;

\c postgres mike

DROP        SCHEMA  IF  EXISTS  mike_data           CASCADE;
DROP        SCHEMA  IF  EXISTS  mike_view           CASCADE;

DROP        OWNED   BY          mike_data;
DROP        OWNED   BY          mike_view;

DROP        USER    IF  EXISTS  mike_data;
DROP        USER    IF  EXISTS  mike_view;

TRUNCATE    TABLE   mike$_pgmviews;
TRUNCATE    TABLE   mike$_pgmview_logs;

SET CLIENT_MIN_MESSAGES = NOTICE;

