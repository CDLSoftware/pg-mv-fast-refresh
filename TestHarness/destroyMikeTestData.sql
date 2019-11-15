/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: destroyMikeTestData.sql
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

-- psql -h localhost -p 5432 -d postgres -U cdl_data -q -f createMikeTestData.sql -v v1=cdl_data -v v2=cdl_view

SET CLIENT_MIN_MESSAGES = ERROR;

DROP        VIEW    IF  EXISTS  :v1.mv1;
DROP        VIEW    IF  EXISTS  :v1.mv2;
DROP        VIEW    IF  EXISTS  :v1.mv3;
DROP        VIEW    IF  EXISTS  :v1.mv4;

DROP        TRIGGER IF  EXISTS  trig$_t1        ON  :v1.t1;
DROP        TRIGGER IF  EXISTS  trig$_t2        ON  :v1.t2;
DROP        TRIGGER IF  EXISTS  trig$_t3        ON  :v1.t3;

DROP        TABLE   IF  EXISTS  :v1.pgmv$_mv1   CASCADE;
DROP        TABLE   IF  EXISTS  :v1.pgmv$_mv2   CASCADE;
DROP        TABLE   IF  EXISTS  :v1.pgmv$_mv3   CASCADE;
DROP        TABLE   IF  EXISTS  :v1.pgmv$_mv4   CASCADE;
DROP        TABLE   IF  EXISTS  :v1.log$_t1     CASCADE;
DROP        TABLE   IF  EXISTS  :v1.log$_t2     CASCADE;
DROP        TABLE   IF  EXISTS  :v1.log$_t3     CASCADE;
DROP        TABLE   IF  EXISTS  :v1.t1          CASCADE;
DROP        TABLE   IF  EXISTS  :v1.t2          CASCADE;
DROP        TABLE   IF  EXISTS  :v1.t3          CASCADE;

DROP        SCHEMA  IF  EXISTS  :v1             CASCADE;
DROP        SCHEMA  IF  EXISTS  :v2             CASCADE;

DROP        OWNED   BY          :v1;
DROP        OWNED   BY          :v2;

DROP        USER    IF  EXISTS  :v1;
DROP        USER    IF  EXISTS  :v2;

TRUNCATE    TABLE   pg$mviews;
TRUNCATE    TABLE   pg$mview_logs;
TRUNCATE    TABLE   pg$mviews_oj_details;

SET CLIENT_MIN_MESSAGES = NOTICE;

