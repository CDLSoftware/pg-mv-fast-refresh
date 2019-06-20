/* ---------------------------------------------------------------------------------------------------------------------------------
Routine Name: CreateModuleOwnerSchema.sql
Author:       Mike Revitt
Date:         12/11/2018
------------------------------------------------------------------------------------------------------------------------------------
Revision History    Push Down List
------------------------------------------------------------------------------------------------------------------------------------
Date        | Name          | Description
------------+---------------+-------------------------------------------------------------------------------------------------------
            |               |
12/11/2018  | M Revitt      | Initial version
------------+---------------+-------------------------------------------------------------------------------------------------------
Background:     PostGre does not support Materialized View Fast Refreshes, this suite of scripts is a PL/SQL coded mechanism to
                provide that functionality, the next phase of this projecdt is to fold these changes into the PostGre kernel.

Description:    This script creates the SCHEMA and USER to hold the Materialized View Fast Refresh code along with the necessary
                data dictionary views, then it calls the create function scripts in the correct order

Notes:          There are 2 data dictionary tables
                o   pgmview_logs
                o   pgmviews

                Access is controlled via database role
                o   pgmv$_role   -   is given the privileges to run the public materialized view functions
								 -	 is given usage on the pgrs_mview schema
								 -	 is given access to the DDL tables

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

SET  CLIENT_MIN_MESSAGES = ERROR;

CREATE EXTENSION    IF NOT  EXISTS "uuid-ossp";

SET CLIENT_MIN_MESSAGES = NOTICE;

CREATE OR REPLACE FUNCTION create_user_and_role(IN pis_password TEXT, IN pis_moduleowner TEXT)
RETURNS void
AS
$BODY$
DECLARE

ls_password TEXT := pis_password;

ls_moduleowner TEXT := pis_moduleowner;

ls_sql TEXT;

BEGIN
   IF NOT EXISTS (
      SELECT
      FROM   pg_user
      WHERE  usename = pis_moduleowner) THEN
	  
	  ls_sql := 'CREATE USER '||ls_moduleowner||' WITH
					LOGIN
					NOSUPERUSER
					NOCREATEDB
					NOCREATEROLE
					INHERIT
					NOREPLICATION
					CONNECTION LIMIT -1
					PASSWORD '''||pis_password||''';';
				
	  EXECUTE ls_sql;
	  
   END IF;
   
   IF NOT EXISTS (
      SELECT
      FROM   pg_roles
      WHERE  rolname = 'pgmv$_role') THEN
	  
	  ls_sql := 'CREATE ROLE pgmv$_role WITH
				  NOLOGIN
				  NOSUPERUSER
				  INHERIT
				  NOCREATEDB
				  NOCREATEROLE
				  NOREPLICATION;';
				
	  EXECUTE ls_sql;
	  
   END IF;

END;
$BODY$
LANGUAGE  plpgsql;

SELECT create_user_and_role(:'MODULEOWNERPASS',:'MODULEOWNER');

ALTER   DATABASE    :DBNAME        SET     SEARCH_PATH=:PGUSERNAME,PUBLIC,:MODULEOWNER;

GRANT   pgmv$_role     TO  :MODULEOWNER;
GRANT   :MODULEOWNER   TO  :PGUSERNAME;

CREATE  SCHEMA IF NOT EXISTS :MODULEOWNER    AUTHORIZATION   :MODULEOWNER;

CREATE TABLE IF NOT EXISTS :MODULEOWNER.pgmview_logs
(
	owner           TEXT        NOT NULL,
	pglog$_name     TEXT        NOT NULL,
	table_name      TEXT        NOT NULL,
	trigger_name    TEXT        NOT NULL,
    pg_mview_bitmap BIGINT      NOT NULL DEFAULT 0,
	CONSTRAINT
		pk$_snapshot_logs
		PRIMARY KEY
		(
			owner,
			table_name
		)
);

CREATE TABLE IF NOT EXISTS :MODULEOWNER.pgmviews
(
	owner               TEXT        NOT NULL,
	view_name           TEXT        NOT NULL,
	pgmv_columns        TEXT        NOT NULL,
	select_columns      TEXT        NOT NULL,
	table_names         TEXT        NOT NULL,
	where_clause        TEXT,
	table_array         TEXT[],
	alias_array         TEXT[],
	rowid_array         TEXT[],
	log_array           TEXT[],
	bit_array           SMALLINT[],
	outer_table_array   TEXT[],
	parent_alias_array  TEXT[],
	parent_rowid_array  TEXT[],
	CONSTRAINT
		pk$_pgmviews
		PRIMARY KEY
		(
			owner,
			view_name
		)
);

GRANT   USAGE   ON                      SCHEMA  :MODULEOWNER    TO  pgmv$_role;
GRANT   SELECT  ON  ALL TABLES      IN  SCHEMA  :MODULEOWNER    TO  pgmv$_role;

ALTER TABLE :MODULEOWNER.pgmviews       OWNER TO :MODULEOWNER;
ALTER TABLE :MODULEOWNER.pgmview_logs   OWNER TO :MODULEOWNER;

ALTER EXTENSION "uuid-ossp" SET SCHEMA public;

