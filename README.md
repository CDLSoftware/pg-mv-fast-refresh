# pg-mv-fast-refresh

# Permissions required to build a materialized view based on this structure

# kingfisher_core - schema used to create materialized view tables inside
# soe - schema used to hold the tables used as part of the query to create the materialized views
# pgrs_mview - schema used to hold the fast refresh module objects
# dbadmin - user used to build the fast refresh module
# biadmin - user used to build the materialized view logs and materialized view via the fast refresh module
# strata - database name

#Create schema to hold materialized view tables

CREATE USER kingfisher_core WITH
					LOGIN
					NOSUPERUSER
					NOCREATEDB
					NOCREATEROLE
					INHERIT
					NOREPLICATION
					CONNECTION LIMIT -1
					PASSWORD '<ENTER_PASSWORD>';
					
CREATE SCHEMA IF NOT EXISTS kingfisher_core AUTHORIZATION kingfisher_core;

GRANT ALL ON SCHEMA kingfisher_core TO kingfisher_core;

GRANT kingfisher_core to biadmin;

GRANT SELECT ON ALL TABLES IN SCHEMA soe TO kingfisher_core;

GRANT pgmv$_role TO kingfisher_core;

# Setting search path at database level
ALTER DATABASE strata SET SEARCH_PATH=public,pgrs_mview,kingfisher_core,soe,biadmin

# Setting search_path at database level should be enough - however if it needs to be set at user level as well here are the commands below:-
# ALTER   USER        soe						SET     SEARCH_PATH=soe,pgrs_mview,kingfisher_core,public;
# ALTER   USER        kingfisher_core			SET     SEARCH_PATH=kingfisher_core,soe,pgrs_mview,public;
# ALTER   USER        pgrs_mview    			SET     SEARCH_PATH=pgrs_mview,soe,kingfisher_core,public;
# ALTER   USER        dbadmin    				SET     SEARCH_PATH=soe,pgrs_mview,kingfisher_core,public;
# ALTER   USER        biadmin    				SET     SEARCH_PATH=soe,pgrs_mview,kingfisher_core,public;

# Additional permissions

GRANT pgmv$_role TO biadmin;
GRANT ALL ON SCHEMA soe TO pgrs_mview;
GRANT soe TO pgrs_mview;
GRANT USAGE ON SCHEMA soe TO pgrs_mview;
GRANT ALL PRIVILEGES ON DATABASE strata TO pgrs_mview;
GRANT ALL ON SCHEMA kingfisher_core TO pgrs_mview;
GRANT USAGE ON SCHEMA kingfisher_core TO pgrs_mview;
GRANT kingfisher_core TO pgrs_mview;
GRANT pgrs_mview TO kingfisher_core;
GRANT USAGE ON SCHEMA pgrs_mview TO kingfisher_core;



