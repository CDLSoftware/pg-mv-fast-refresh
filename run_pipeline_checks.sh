#! /bin/bash
#File: run_pipeline_checks.sh
#Desc:
#
# Amendment History:
# Date:      Who:       Desc:
# 19/11/19   T.Mullen   Initial;
# 25/11/19   T.Mullen   amended to breakdown the run types;
# 26/11/19   T.Mullen   updating the testing stages;
# 13/03/20   T.Mullen   updating the permissions for module home;
#

source /builds/cheshire-datasystems/dba-team/pg-mv-fast-refresh-githubrunner/module_set_variables.sh

export INSTALLTYPE=$1

if [ $# -lt 1 ]; then
   echo "Usage: $0 type [all|build|test|destroy;"
   exit 1
fi

export LOG_FILE=/tmp/pipelinerun_${INSTALLTYPE}_`date +%Y%m%d-%H%M%S`.log
echo "INFO: Set variables" >> $LOG_FILE
echo "INFO: MODULEOWNER parameter set to $MODULEOWNER" >> $LOG_FILE
echo "INFO: PGUSERNAME parameter set to $PGUSERNAME" >> $LOG_FILE
echo "INFO: HOSTNAME parameter set to $HOSTNAME" >> $LOG_FILE
echo "INFO: PORT parameter set to $PORT" >> $LOG_FILE
echo "INFO: DBNAME parameter set to $DBNAME" >> $LOG_FILE
echo "INFO: MODULE_HOME parameter set to $MODULE_HOME" >> $LOG_FILE
PGPASS=$PGPASSWORD

echo "Starting pipeline script with option $INSTALLTYPE" | tee -a $LOG_FILE
echo "Starting time - $(date)" | tee -a $LOG_FILE 
chmod 771 $MODULE_HOME/*.sh

function buildmodule
{

echo "INFO: Run $MODULEOWNER schema build script" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER -v PGUSERNAME=$PGUSERNAME -v DBNAME=$DBNAME -v HOSTNAME=$HOSTNAME -v PORT=$PORT << EOF1 >> $LOG_FILE 2>&1

    \i :MODULE_HOME/BuildScripts/createModuleOwnerSchema.sql;

	\q

EOF1

echo "INFO: Run Cron setup script" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER << EOFC >> $LOG_FILE 2>&1
	
	\i :MODULE_HOME/BuildScripts/createCronSetup.sql;

	\q
	
EOFC

PGPASSWORD=$MODULEOWNERPASS

echo "INFO: Run $MODULEOWNER schema object build scripts" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$MODULEOWNER --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME -v MODULEOWNER=$MODULEOWNER -v MODULEOWNERPASS=$MODULEOWNERPASS -v HOSTNAME=$HOSTNAME -v PORT=$PORT  << EOF2 >> $LOG_FILE 2>&1

	-- Used to run materialized view build insert in parallel sessions
	CREATE SERVER IF NOT EXISTS pgmv\$cron_instance FOREIGN DATA WRAPPER postgres_fdw options ( dbname 'postgres', port :'PORT', host :'HOSTNAME', connect_timeout '2', keepalives_count '5' );
	CREATE USER MAPPING IF NOT EXISTS for :MODULEOWNER SERVER pgmv\$cron_instance OPTIONS (user :'MODULEOWNER', password :'MODULEOWNERPASS');
	GRANT USAGE ON FOREIGN SERVER pgmv\$cron_instance TO :MODULEOWNER;

	SET search_path = :MODULEOWNER,catalog,public;

   \i :MODULE_HOME/BuildScripts/mvTypes.sql;
   \i :MODULE_HOME/BuildScripts/mvConstants.sql;
   \i :MODULE_HOME/BuildScripts/mvSimpleFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvComplexFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvApplicationFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvTriggerFunction.sql;

  \q

EOF2

$MODULE_HOME/module_error_chks.sh

}



function createsourceschema
{

echo "Starting building schemas " >> $LOG_FILE

echo "INFO: Creating Source Schema $SOURCEUSERNAME " >> $LOG_FILE

psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME -v SOURCEPASSWORD=$SOURCEPASSWORD -v SOURCEUSERNAME=$SOURCEUSERNAME << EOF1 >> $LOG_FILE 2>&1

DO
\$do\$
BEGIN
   IF NOT EXISTS (
      SELECT                       -- SELECT list can stay empty for this
      FROM   pg_catalog.pg_roles
      WHERE  rolname = '$SOURCEUSERNAME') THEN

       	CREATE USER $SOURCEUSERNAME WITH
 	LOGIN
 	NOSUPERUSER
 	NOCREATEDB
 	NOCREATEROLE
 	INHERIT
 	NOREPLICATION
 	CONNECTION LIMIT -1
 	PASSWORD '$SOURCEPASSWORD';

    END IF;
END
\$do\$;


 GRANT ALL PRIVILEGES ON DATABASE "$DBNAME" to $SOURCEUSERNAME;
 GRANT $SOURCEUSERNAME to $PGUSERNAME;
 CREATE SCHEMA $SOURCEUSERNAME AUTHORIZATION $SOURCEUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $SOURCEUSERNAME to $PGUSERNAME;
 GRANT $SOURCEUSERNAME to $MODULEOWNER;
 GRANT USAGE ON SCHEMA $SOURCEUSERNAME TO $MODULEOWNER;
 GRANT ALL ON SCHEMA $MODULEOWNER TO $SOURCEUSERNAME;
 GRANT USAGE ON FOREIGN SERVER pgmv\$_instance TO $SOURCEUSERNAME;
 GRANT USAGE ON FOREIGN SERVER pgmv\$cron_instance TO $SOURCEUSERNAME;
 
 CREATE USER MAPPING IF NOT EXISTS for :SOURCEUSERNAME SERVER pgmv\$cron_instance OPTIONS (user :'SOURCEUSERNAME', password :'SOURCEPASSWORD');
 CREATE USER MAPPING IF NOT EXISTS for :SOURCEUSERNAME SERVER pgmv\$_instance OPTIONS (user :'SOURCEUSERNAME', password :'SOURCEPASSWORD');

EOF1

echo "INFO: Run Cron permissions to $SOURCEUSERNAME user" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v SOURCEUSERNAME=$SOURCEUSERNAME << EOFC >> $LOG_FILE 2>&1

	GRANT USAGE ON SCHEMA cron TO :SOURCEUSERNAME;
	GRANT ALL ON SCHEMA cron TO :SOURCEUSERNAME;
	GRANT ALL PRIVILEGES ON SCHEMA cron to :SOURCEUSERNAME;
	GRANT ALL ON ALL TABLES in schema cron to :SOURCEUSERNAME;
	GRANT ALL ON ALL sequences in schema cron to :SOURCEUSERNAME;
	
	\q
	
EOFC

}

function createmvschema
{

echo "INFO: Creating MV Schema $MVUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME -v MVPASSWORD=$MVPASSWORD -v MVUSERNAME=$MVUSERNAME  << EOF2 >> $LOG_FILE 2>&1

DO
\$do\$
BEGIN
   IF NOT EXISTS (
      SELECT                       -- SELECT list can stay empty for this
      FROM   pg_catalog.pg_roles
      WHERE  rolname = '$MVUSERNAME') THEN

       	CREATE USER $MVUSERNAME WITH
 	LOGIN
 	NOSUPERUSER
 	NOCREATEDB
 	NOCREATEROLE
 	INHERIT
 	NOREPLICATION
 	CONNECTION LIMIT -1
 	PASSWORD '$MVPASSWORD';

    END IF;
END
\$do\$;

GRANT $MVUSERNAME to $PGUSERNAME;
CREATE SCHEMA IF NOT EXISTS $MVUSERNAME AUTHORIZATION $MVUSERNAME;
GRANT ALL ON SCHEMA $MVUSERNAME TO $MVUSERNAME;
GRANT SELECT ON ALL TABLES IN SCHEMA $SOURCEUSERNAME TO $MVUSERNAME;
GRANT pgmv\$_role TO $MVUSERNAME;

ALTER ROLE $MODULEOWNER SET search_path TO public,$MODULEOWNER,$MVUSERNAME,$SOURCEUSERNAME;
ALTER ROLE $MVUSERNAME SET search_path TO public,$MODULEOWNER,$MVUSERNAME,$SOURCEUSERNAME;
ALTER ROLE $SOURCEUSERNAME SET search_path TO public,$MODULEOWNER,$MVUSERNAME,$SOURCEUSERNAME;

GRANT $SOURCEUSERNAME TO $MODULEOWNER;
GRANT USAGE ON SCHEMA $SOURCEUSERNAME TO $MODULEOWNER;
GRANT ALL PRIVILEGES ON DATABASE $DBNAME TO $MODULEOWNER;
GRANT ALL ON SCHEMA $MVUSERNAME  TO $MODULEOWNER;
GRANT USAGE ON SCHEMA $MVUSERNAME  TO $MODULEOWNER;
GRANT $MODULEOWNER TO $MVUSERNAME;
GRANT USAGE ON SCHEMA $MODULEOWNER TO $MVUSERNAME;
GRANT ALL ON SCHEMA $SOURCEUSERNAME TO $MODULEOWNER;
GRANT   $SOURCEUSERNAME,$MVUSERNAME     TO $PGUSERNAME;
GRANT   $SOURCEUSERNAME TO   $MODULEOWNER;
GRANT   pgmv\$_view, pgmv\$_usage TO $MVUSERNAME;
GRANT   pgmv\$_view, pgmv\$_usage, pgmv\$_execute, pgmv\$_role TO $SOURCEUSERNAME;
GRANT USAGE ON FOREIGN SERVER pgmv\$_instance TO $MVUSERNAME;
GRANT USAGE ON FOREIGN SERVER pgmv\$cron_instance TO $MVUSERNAME;

CREATE USER MAPPING IF NOT EXISTS for :MVUSERNAME SERVER pgmv\$cron_instance OPTIONS (user :'MVUSERNAME', password :'MVPASSWORD');
CREATE USER MAPPING IF NOT EXISTS for :MVUSERNAME SERVER pgmv\$_instance OPTIONS (user :'MVUSERNAME', password :'MVPASSWORD');

EOF2

echo "INFO: Run Cron permissions to $SOURCEUSERNAME user" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MVUSERNAME=$MVUSERNAME << EOFC >> $LOG_FILE 2>&1
	
	GRANT USAGE ON SCHEMA cron TO :MVUSERNAME;
	GRANT ALL ON SCHEMA cron TO :MVUSERNAME;
	GRANT ALL PRIVILEGES ON SCHEMA cron to :MVUSERNAME;
	GRANT ALL ON ALL TABLES in schema cron to :MVUSERNAME;
	GRANT ALL ON ALL sequences in schema cron to :MVUSERNAME;

	\q
	
EOFC

}

function createtestdata
{

echo "INFO: Creating Test Data for $SCHEMAUSERNAME " >> $LOG_FILE

PGPASSWORD=$SOURCEPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF3 >> $LOG_FILE 2>&1

-- FUNCTIONAL TEST SETUP

-- create test1 table

CREATE TABLE $SOURCEUSERNAME.test1
(
    id numeric NOT NULL,
    lookup_id numeric,
    code character varying(10) COLLATE pg_catalog."default",
	created	timestamp without time zone,
    CONSTRAINT test1_pkey PRIMARY KEY (id)
);

-- create test2 table

CREATE TABLE $SOURCEUSERNAME.test2
(
    id numeric NOT NULL,
    description character varying(100) COLLATE pg_catalog."default",
    metavals_id numeric,
    age integer NOT NULL,
    CONSTRAINT test2_pkey PRIMARY KEY (id)
);

-- create test3 table

CREATE TABLE $SOURCEUSERNAME.test3
(
    lookup_id numeric NOT NULL,
    lookup_code character varying(10) COLLATE pg_catalog."default" NOT NULL,
    lookup_description character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT test3_pkey PRIMARY KEY (lookup_id)
);

-- create test4 table

CREATE TABLE $SOURCEUSERNAME.test4
(
    metavals_id numeric NOT NULL,
    code character varying(10) COLLATE pg_catalog."default" NOT NULL,
    description character varying(30) COLLATE pg_catalog."default",
    CONSTRAINT test4_pkey PRIMARY KEY (metavals_id)
);

-- create test5 table

CREATE TABLE $SOURCEUSERNAME.test5
(
    id numeric NOT NULL,
    rep_ind character varying(1) COLLATE pg_catalog."default",
    trans_id numeric,
    CONSTRAINT test5_pkey PRIMARY KEY (id)
);

-- create test6 table

CREATE TABLE $SOURCEUSERNAME.test6
(
    trans_id numeric NOT NULL,
    payment_reference character varying(20) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT test6_pkey PRIMARY KEY (trans_id)
);

-- insert records into test1 table

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code, created)
	VALUES (1, 10, 'hello','2015-04-21 09:40:27.364');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code, created)
	VALUES (2, 20, 'bye','2016-04-21 21:40:20.364');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code, created)
	VALUES (3, 30, 'cya','2016-07-01 17:00:20.364');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code, created)
	VALUES (4, 50, 'goodbye','2017-02-12 19:01:20.364');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code, created)
	VALUES (5, 50, 'hi','2018-03-27 01:01:20.364');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code, created)
	VALUES (6, 20, 'bye','2020-06-25 23:59:20.364');

-- insert records into test2 table

INSERT INTO $SOURCEUSERNAME.test2(
	id, description, metavals_id, age)
	VALUES (1, 'house', 100, 20);

INSERT INTO $SOURCEUSERNAME.test2(
	id, description, metavals_id, age)
	VALUES (2, 'flat', 200, 35);

INSERT INTO $SOURCEUSERNAME.test2(
	id, description, metavals_id, age)
	VALUES (3, 'bungalow', 300, 30);

INSERT INTO $SOURCEUSERNAME.test2(
	id, description, metavals_id, age)
	VALUES (4, 'palace', 300, 30);

INSERT INTO $SOURCEUSERNAME.test2(
	id, description, metavals_id, age)
	VALUES (5, 'office', 400, 50);

-- insert records into test3 table

INSERT INTO $SOURCEUSERNAME.test3(
	lookup_id, lookup_code, lookup_description)
	VALUES (10, 'ENG', 'ENGLAND');

INSERT INTO $SOURCEUSERNAME.test3(
	lookup_id, lookup_code, lookup_description)
	VALUES (20, 'WAL', 'WALES');

INSERT INTO $SOURCEUSERNAME.test3(
	lookup_id, lookup_code, lookup_description)
	VALUES (30, 'SCO', 'SCOTLAND');

INSERT INTO $SOURCEUSERNAME.test3(
	lookup_id, lookup_code, lookup_description)
	VALUES (40, 'IRE', 'IRELAND');

INSERT INTO $SOURCEUSERNAME.test3(
	lookup_id, lookup_code, lookup_description)
	VALUES (50, 'FRA', 'FRANCE');

-- insert records into test4 table

INSERT INTO $SOURCEUSERNAME.test4(
	metavals_id, code, description)
	VALUES (100,'CHAIR','SMALL CHAIR');

INSERT INTO $SOURCEUSERNAME.test4(
	metavals_id, code, description)
	VALUES (200,'TABLE','SMALL TABLE');

INSERT INTO $SOURCEUSERNAME.test4(
	metavals_id, code, description)
	VALUES (300,'LIGHT','BRIGHT LIGHT');

INSERT INTO $SOURCEUSERNAME.test4(
	metavals_id, code, description)
	VALUES (400,'BED','KING SIZE BED');

INSERT INTO $SOURCEUSERNAME.test4(
	metavals_id, code, description)
	VALUES (500,'CUPBOARD','BEDSIDE CUPBOARD');

-- insert records into test5 table

INSERT INTO $SOURCEUSERNAME.test5(
	id, rep_ind, trans_id)
	VALUES (1, 'Y', 1000);

INSERT INTO $SOURCEUSERNAME.test5(
	id, rep_ind, trans_id)
	VALUES (2, 'Y', 2000);

INSERT INTO $SOURCEUSERNAME.test5(
	id, rep_ind, trans_id)
	VALUES (3, 'N', 3000);

INSERT INTO $SOURCEUSERNAME.test5(
	id, rep_ind, trans_id)
	VALUES (4, 'Y', 4000);

INSERT INTO $SOURCEUSERNAME.test5(
	id, rep_ind, trans_id)
	VALUES (5, 'N', 5000);

-- insert records into test6 table

INSERT INTO $SOURCEUSERNAME.test6(
	trans_id, payment_reference)
	VALUES (1000, 'GZ-1000');

INSERT INTO $SOURCEUSERNAME.test6(
	trans_id, payment_reference)
	VALUES (2000, 'AZ-2000');

INSERT INTO $SOURCEUSERNAME.test6(
	trans_id, payment_reference)
	VALUES (3000, 'BZ-3000');

INSERT INTO $SOURCEUSERNAME.test6(
	trans_id, payment_reference)
	VALUES (4000, 'QZ-4000');

INSERT INTO $SOURCEUSERNAME.test6(
	trans_id, payment_reference)
	VALUES (5000, 'VZ-5000');

EOF3
}

function createmvlogs
{

echo "INFO: Creating MV Logs for $SCHEMAUSERNAME " >> $LOG_FILE

PGPASSWORD=$SOURCEPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF4 >> $LOG_FILE 2>&1

DO
\$do\$
DECLARE
    cResult CHAR(1) := NULL;
BEGIN
    CALL $MODULEOWNER.mv\$createMaterializedViewlog( 'test1','$SOURCEUSERNAME');
    CALL $MODULEOWNER.mv\$createMaterializedViewlog( 'test2','$SOURCEUSERNAME');
    CALL $MODULEOWNER.mv\$createMaterializedViewlog( 'test3','$SOURCEUSERNAME');
    CALL $MODULEOWNER.mv\$createMaterializedViewlog( 'test4','$SOURCEUSERNAME');
    CALL $MODULEOWNER.mv\$createMaterializedViewlog( 'test5','$SOURCEUSERNAME');
    CALL $MODULEOWNER.mv\$createMaterializedViewlog( 'test6','$SOURCEUSERNAME');

END
\$do\$;


EOF4
}

function createtestmv
{

echo "INFO: Creating MV in $MVUSERNAME " >> $LOG_FILE

PGPASSWORD=$MVPASSWORD

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF5 >> $LOG_FILE 2>&1

DO
\$do\$
DECLARE
    tStartTime      TIMESTAMP   := clock_timestamp();
    cResult         CHAR(1)     := NULL;
    iTableCounter   SMALLINT    := 10;
    pSqlStatement   TEXT;
BEGIN
    pSqlStatement := 'SELECT test1.id test1_id,
t1.lookup_id test1_lookup_id,
t1.code test1_code,
t1.created test1_created,
t1.id test2_id,
t2.description test2_desc,
t2.metavals_id test2_metavals_id,
t2.age test2_age,
t3.lookup_id test3_lookup_id,
t3.lookup_code test3_lookup_code,
t3.lookup_description test3_lookup_desc,
t4.metavals_id test4_metavals_id,
t4.code test4_code,
t4.description test4_desc,
t4.id test5_id,
t5.rep_ind test5_rep_ind,
t5.trans_id test5_trans_id,
t6.trans_id test6_trans_id,
t6.payment_reference test6_payment_ref
FROM
test1 t1
INNER JOIN test2 t2 ON t1.id = t2.id
LEFT JOIN test3 t3 ON t1.lookup_id = t3.lookup_id
LEFT JOIN test4 t4 ON t2.metavals_id = t4.metavals_id
INNER JOIN test5 t5 ON t1.id = t5.id
LEFT JOIN test6 t6 ON t5.trans_id = t6.trans_id';

    FOR iTableCounter IN 10 .. 101
    LOOP
        CALL mv\$createMaterializedView
        (
            pViewName           => 'mvtesting' || iTableCounter,
            pSelectStatement    =>  pSqlStatement,
            pOwner              =>  '$MVUSERNAME',
            pFastRefresh        =>  TRUE
        );
    END LOOP;
	
	CALL mv\$createMaterializedView
        (
            pViewName           => 'mvtesting101',
            pSelectStatement    =>  pSqlStatement,
            pOwner              =>  '$MVUSERNAME',
            pFastRefresh        =>  TRUE,
			pParallel			=> 'Y',
			pParallelJobs		=> 4,
			pParallelColumn		=> 'created',
			pParallelAlias		=> 't1'
        );

    RAISE NOTICE 'Simple Snapshot Creation took %', clock_timestamp() - tStartTime;
END
\$do\$;

EOF5
}


function dropmvschema
{

echo "INFO: Dropping MV's" >> $LOG_FILE

PGPASSWORD=$MVPASSWORD
psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF5 >> $LOG_FILE 2>&1

DO
\$do\$
DECLARE
	cResult         CHAR(1)     := NULL;
	iTableCounter   SMALLINT    := 10;
BEGIN
	FOR iTableCounter IN 10 .. 101 
    	LOOP
    	CALL mv\$removeMaterializedView( 'mvtesting' || iTableCounter, '$MVUSERNAME' );
    	END LOOP;
END
\$do\$;

EOF5

echo "INFO: Dropping MV logs" >> $LOG_FILE

PGPASSWORD=$SOURCEPASSWORD
psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF4 >> $LOG_FILE 2>&1

DO
\$do\$
DECLARE
    cResult         CHAR(1)     := NULL;
BEGIN
    CALL mv\$removeMaterializedViewLog( 'test1', '$SOURCEUSERNAME' );
    CALL mv\$removeMaterializedViewLog( 'test2', '$SOURCEUSERNAME' );
    CALL mv\$removeMaterializedViewLog( 'test3', '$SOURCEUSERNAME' );
    CALL mv\$removeMaterializedViewLog( 'test4', '$SOURCEUSERNAME' );
    CALL mv\$removeMaterializedViewLog( 'test5', '$SOURCEUSERNAME' );
    CALL mv\$removeMaterializedViewLog( 'test6', '$SOURCEUSERNAME' );
END
\$do\$;

EOF4


echo "INFO: Dropping MV user $MVUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASS

echo "INFO: Run Cron revoke permissions from $MVUSERNAME user" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER << EOFC >> $LOG_FILE 2>&1
	
	REVOKE USAGE ON SCHEMA cron FROM $MVUSERNAME;
	REVOKE ALL ON SCHEMA cron FROM $MVUSERNAME;
	REVOKE ALL PRIVILEGES ON SCHEMA cron FROM $MVUSERNAME;
	REVOKE ALL ON ALL TABLES in schema cron FROM $MVUSERNAME;
	REVOKE ALL ON ALL sequences in schema cron FROM $MVUSERNAME;

	\q
	
EOFC

psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF3 >> $LOG_FILE 2>&1
 
 REVOKE USAGE ON FOREIGN SERVER pgmv\$cron_instance TO $MVUSERNAME;
 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $MVUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $MVUSERNAME to $PGUSERNAME;
 REVOKE USAGE ON FOREIGN SERVER pgmv\$_instance FROM $MVUSERNAME;
 ALTER SCHEMA $MVUSERNAME OWNER TO $PGUSERNAME;
 DROP SCHEMA $MVUSERNAME CASCADE;
 REVOKE $SOURCEUSERNAME from $MVUSERNAME;
 revoke $SOURCEUSERNAME from $MODULEOWNER;
 revoke $MVUSERNAME  from $MODULEOWNER;
 revoke $MODULEOWNER from $MVUSERNAME ;
 revoke $MVUSERNAME from $PGUSERNAME;
 revoke USAGE ON SCHEMA $MODULEOWNER from $MVUSERNAME ;
 DROP USER $MVUSERNAME;
 
EOF3
}

function dropsourceschema
{
echo "INFO: Dropping Schema user $SCHEMAUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASS

echo "INFO: Run Cron revoke permissions from $MVUSERNAME user" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER << EOFC >> $LOG_FILE 2>&1
	
	REVOKE USAGE ON SCHEMA cron FROM $SCHEMAUSERNAME;
	REVOKE ALL ON SCHEMA cron FROM $SCHEMAUSERNAME;
	REVOKE ALL PRIVILEGES ON SCHEMA cron FROM $SCHEMAUSERNAME;
	REVOKE ALL ON ALL TABLES in schema cron FROM $SCHEMAUSERNAME;
	REVOKE ALL ON ALL sequences in schema cron FROM $SCHEMAUSERNAME;

	\q
	
EOFC

psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF4 >> $LOG_FILE 2>&1
 
 REVOKE USAGE ON FOREIGN SERVER pgmv\$cron_instance TO $SOURCEUSERNAME;
 REVOKE USAGE ON SCHEMA cron TO $SOURCEUSERNAME;
 REVOKE ALL ON SCHEMA cron TO $SOURCEUSERNAME;
 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $SOURCEUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $SOURCEUSERNAME to $PGUSERNAME;
 REVOKE USAGE ON FOREIGN SERVER pgmv\$_instance FROM $SOURCEUSERNAME;
 ALTER SCHEMA $SOURCEUSERNAME OWNER TO $PGUSERNAME;
 DROP SCHEMA $SOURCEUSERNAME CASCADE;
 REVOKE $SOURCEUSERNAME from $PGUSERNAME;
 REVOKE $SOURCEUSERNAME FROM $MODULEOWNER;
 REVOKE ALL ON SCHEMA $MODULEOWNER FROM $SOURCEUSERNAME;
 DROP USER $SOURCEUSERNAME;


EOF4
}


function dropmodule
{
echo "INFO: Dropping modules objects" >> $LOG_FILE

PGPASSWORD=$PGPASS

echo "INFO: Run Cron revoke permissions from $MODULEOWNER user" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER << EOFC >> $LOG_FILE 2>&1
	
	REVOKE USAGE ON SCHEMA cron FROM $MODULEOWNER;
	REVOKE ALL ON SCHEMA cron FROM $MODULEOWNER;
	REVOKE ALL PRIVILEGES ON SCHEMA cron FROM $MODULEOWNER;
	REVOKE ALL ON ALL TABLES in schema cron FROM $MODULEOWNER;
	REVOKE ALL ON ALL sequences in schema cron FROM $MODULEOWNER;

	\q
	
EOFC

psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF1 >> $LOG_FILE 2>&1

 REVOKE USAGE ON FOREIGN SERVER pgmv\$cron_instance TO $MODULEOWNER;
 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $MODULEOWNER;
 REVOKE USAGE ON FOREIGN SERVER pgmv\$_instance FROM $MODULEOWNER;
 DROP SERVER IF EXISTS pgmv\$cron_instance CASCADE;
 DROP SERVER IF EXISTS pgmv\$_instance CASCADE;
 DROP EXTENSION IF EXISTS postgres_fdw;
 DROP EXTENSION IF EXISTS dblink;
 DROP SCHEMA $MODULEOWNER CASCADE;
 DROP ROLE pgmv\$_role;
 REVOKE $MODULEOWNER from $PGUSERNAME;
 DROP EXTENSION "uuid-ossp";
 DROP ROLE $MODULEOWNER;
 DROP ROLE pgmv\$_execute;
 DROP ROLE pgmv\$_view;
 DROP ROLE pgmv\$_usage;

EOF1
}

function testing1
{
echo "INFO: Updating test1 table $SCHEMAUSERNAME " >> $LOG_FILE

PGPASSWORD=$SOURCEPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF3 >> $LOG_FILE 2>&1

UPDATE $SOURCEUSERNAME.test1 set code='yo' where code='hello';

EOF3


echo "INFO: Refreshing the MV's" >> $LOG_FILE

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF6 >> $LOG_FILE 2>&1

DO
\$do\$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
    iTableCounter   SMALLINT    := 10;
BEGIN
    FOR iTableCounter IN 10 .. 20
    LOOP
    CALL mv\$refreshMaterializedView
    (
        pViewName       => 'mvtesting' || iTableCounter,
        pOwner          => '$MVUSERNAME',
        pFastRefresh    =>  TRUE
    );
    END LOOP;
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;

DO
\$do\$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
    iTableCounter   SMALLINT    := 10;
BEGIN
    FOR iTableCounter IN 70 .. 101
    LOOP
    CALL mv\$refreshMaterializedView
    (
        pViewName       => 'mvtesting' || iTableCounter,
        pOwner          => '$MVUSERNAME',
        pFastRefresh    =>  TRUE
    );
    END LOOP;
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;


DO
\$do\$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
    iTableCounter   SMALLINT    := 10;
BEGIN
    FOR iTableCounter IN 21 .. 69
    LOOP
    CALL mv\$refreshMaterializedView
    (
        pViewName       => 'mvtesting' || iTableCounter,
        pOwner          => '$MVUSERNAME',
        pFastRefresh    =>  TRUE
    );
    END LOOP;
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;

EOF6

echo "INFO: Check the MV's Data" >> $LOG_FILE

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF6 >> $LOG_FILE 2>&1

DO
\$\$
DECLARE
    each_row   record;
    testout bigint;
BEGIN
   FOR each_row IN
    SELECT 'select count(*) from ' || table_name || ' where test1_code=''hello'';' AS alter_sql
		FROM information_schema.tables
		WHERE table_schema = 'testpocview'
   LOOP
     EXECUTE each_row.alter_sql into testout;
	 raise info '%', each_row.alter_sql;
	 raise info '%', testout;
   END LOOP;
END
\$\$;

EOF6


echo "test output $checktestoutput" >> $LOG_FILE

checkuser_checktestoutput=`cat $LOG_FILE |grep "INFO:  1" | wc -l`

if [ $checkuser_checktestoutput -gt 0 ]; then
     echo "ERROR Problems with the test data" >> $LOG_FILE
     echo "Output from checks" >> $LOG_FILE
     echo "$checktestoutput" >> $LOG_FILE
     else
     echo "Test data checks all ok" | tee -a $LOG

fi


}


function problemcheck
{
#Check for Problem 
chk_errors=`cat $LOG_FILE |grep -E "ERROR|error|Error|refused|failed" |wc -l`
if [ $chk_errors -lt 1 ]; then
 echo "Pipeline run type $INSTALLTYPE ran with no issues" | tee -a $LOG_FILE
 echo "Run completion time - $(date)" | tee -a $LOG_FILE
else
 echo "Problems with run type $INSTALLTYPE check logfile for detailed view - $LOG_FILE" | tee -a $LOG_FILE
 runerrors=`cat $LOG_FILE |grep -E "ERROR|error|Error|refused|failed"`
 echo "Problems with the run below" | tee -a $LOG_FILE 
 echo "$runerrors" | tee -a $LOG_FILE 
 echo "Run completion time - $(date)" | tee -a $LOG_FILE
exit 1
fi
}

#Install Type
if [ "$INSTALLTYPE" = all ]; then
echo "Stage 1: Creating the fast refresh module objects in schemas $MODULEOWNER" | tee -a $LOG_FILE
buildmodule
echo "Stage 2: Creating the schemas $SOURCEUSERNAME and $MVUSERNAME" | tee -a $LOG_FILE
createsourceschema
createmvschema
echo "Stage 3: Creating the test objects and data in schema $SOURCEUSERNAME" | tee -a $LOG_FILE
createtestdata
echo "Stage 4: Creating the MV logs in $SOURCEUSERNAME" | tee -a $LOG_FILE
createmvlogs
echo "Stage 5: Creating 90 test MV's in schema $MVUSERNAME" | tee -a $LOG_FILE
createtestmv
echo "Stage 6: Test phase " | tee -a $LOG_FILE
echo "Stage 6.1: Update 1 row and refresh all MV's " | tee -a $LOG_FILE
testing1
echo "Stage 7: Dropping the test harness objects" | tee -a $LOG_FILE
dropmvschema
dropsourceschema
dropmodule
echo "Stage 8: Check for problems" | tee -a $LOG_FILE
problemcheck
elif [ "$INSTALLTYPE" = build ]; then
echo "Stage 1: Creating the fast refresh module objects in schemas $MODULEOWNER" | tee -a $LOG_FILE
buildmodule
echo "Stage 2: Creating the schemas $SOURCEUSERNAME and $MVUSERNAME" | tee -a $LOG_FILE
createsourceschema
createmvschema
echo "Stage 3: Creating the test objects and data in schema $SOURCEUSERNAME" | tee -a $LOG_FILE
createtestdata
echo "Stage 4: Creating the MV logs in $SOURCEUSERNAME" | tee -a $LOG_FILE
createmvlogs
echo "Stage 5: Creating 90 test MV's in schema $MVUSERNAME" | tee -a $LOG_FILE
createtestmv
echo "Stage 6: Check for problems" | tee -a $LOG_FILE
problemcheck
elif [ "$INSTALLTYPE" = test ]; then
echo "Stage 1: Test phase " | tee -a $LOG_FILE
echo "Stage 1.1: Update 1 row and refresh all MV's " | tee -a $LOG_FILE
testing1
echo "Stage 2: Check for problems" | tee -a $LOG_FILE
problemcheck
elif [ "$INSTALLTYPE" = destroy ]; then
echo "Stage 1: Dropping the test harness objects" | tee -a $LOG_FILE
dropmvschema
dropsourceschema
dropmodule
echo "Stage 2: Check for problems" | tee -a $LOG_FILE
problemcheck
else
echo "$(date): Incorrect option must be all, build, test or destroy"
exit 1

fi
