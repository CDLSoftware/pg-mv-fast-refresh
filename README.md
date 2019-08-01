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

ALTER DATABASE strata SET SEARCH_PATH=soe,pgrs_mview,kingfisher_core,public

# Setting search_path at database level should be enough - however if it needs to be set at user level as well here are the commands below:-
# ALTER   USER        soe						SET     SEARCH_PATH=soe,pgrs_mview,kingfisher_core,public;
# ALTER   USER        kingfisher_core			SET     SEARCH_PATH=kingfisher_core,soe,pgrs_mview,public;
# ALTER   USER        pgrs_mview    			SET     SEARCH_PATH=pgrs_mview,soe,kingfisher_core,public;
# ALTER   USER        dbadmin    				SET     SEARCH_PATH=soe,pgrs_mview,kingfisher_core,public;



