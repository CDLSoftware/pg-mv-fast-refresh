# pg-mv-fast-refresh

# kingfisher_core - schema used to create materialized view tables inside
# soe - schema used to hold the tables used as part of the query to create the materialized views
# pgrs_mview - schema used to hold the fast refresh module objects
# dbadmin - user used to build the fast refresh module

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
					
GRANT kingfisher_core to dbadmin;
					
CREATE SCHEMA IF NOT EXISTS kingfisher_core AUTHORIZATION kingfisher_core;

GRANT ALL ON SCHEMA kingfisher_core TO kingfisher_core;

# Additional permissions to get the fast refresh module working against the owner of the materialized view logs e.g. soe and the schema you'd like to create the materialized views tables e.g. kingfisher_core

ALTER   USER        soe						SET     SEARCH_PATH=soe,PUBLIC,pgrs_mview,kingfisher_core;
ALTER   USER        kingfisher_core			SET     SEARCH_PATH=kingfisher_core,PUBLIC,pgrs_mview,soe;
ALTER   USER        pgrs_mview    			SET     SEARCH_PATH=pgrs_mview,PUBLIC,soe,kingfisher_core;

GRANT   kingfisher_core,  soe       TO      dbadmin;
GRANT   kingfisher_core,  soe       TO      pgrs_mview;

GRANT   pgmv$_role   TO      kingfisher_core;
GRANT   pgmv$_role   TO      soe;

ALTER   DATABASE    strata        SET     SEARCH_PATH=dbadmin,PUBLIC,pgrs_mview,soe,kingfisher_core;
