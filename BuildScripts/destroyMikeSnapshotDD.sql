/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: destroyMikeSnapshotDD.sql
Author:       Mike Revitt
Date:         08/04/2019
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
08/04/2019  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    Remove DDL tables, users and roles

Issues:         There is a bug in RDS for PostGres version 10.4 that prevents this code from working, this but is fixed in
                versions 10.5 and 10.3

                https://forums.aws.amazon.com/thread.jspa?messageID=860564

Help:           Help can be invoked by running the rollowing command from within PostGre

                DO $$ BEGIN RAISE NOTICE '%', mv$stringConstants('HELP_TEXT'); END $$;

************************************************************************************************************************************
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
***********************************************************************************************************************************/

-- psql -h localhost -p 5432 -d postgres -U mike -f DestroyMikeSnapshotDD.sql -v v1=cdl_data -v v2=cdl_view

SET  CLIENT_MIN_MESSAGES = ERROR;

DROP    TABLE       IF  EXISTS  :v1.pg$mviews       CASCADE;
DROP    TABLE       IF  EXISTS  :v1.pg$mview_logs   CASCADE;

DROP    SCHEMA      IF  EXISTS  :v1                 CASCADE;

DROP    OWNED       BY          :v1;

DROP    ROLE        IF  EXISTS  :v1;
DROP    ROLE        IF  EXISTS  pgmv$_execute;
DROP    ROLE        IF  EXISTS  pgmv$_usage;
DROP    ROLE        IF  EXISTS  pgmv$_view;

SET CLIENT_MIN_MESSAGES = NOTICE;

