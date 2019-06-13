# pg-mv-fast-refresh

# Additional permissions to get the fast refresh module working against the owner of the materialized view logs e.g. soe and the schema you'd like to create the materialized views tables e.g. kingfisher_core

ALTER   USER        soe						SET     SEARCH_PATH=soe,PUBLIC,pgrs_mview,kingfisher_core;
ALTER   USER        kingfisher_core			SET     SEARCH_PATH=kingfisher_core,PUBLIC,pgrs_mview,soe;
ALTER   USER        pgrs_mview    			SET     SEARCH_PATH=pgrs_mview,PUBLIC,soe,kingfisher_core;

GRANT   kingfisher_core,  soe       TO      biadmin;
GRANT   kingfisher_core,  soe       TO      dbadmin;
GRANT   kingfisher_core,  soe       TO      pgrs_mview;

GRANT   pgmv$_role   TO      kingfisher_core;
GRANT   pgmv$_role   TO      soe;

ALTER   DATABASE    strata        SET     SEARCH_PATH=dbadmin,biadmin,PUBLIC,pgrs_mview,soe,kingfisher_core;
