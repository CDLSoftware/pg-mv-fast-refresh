#! /bin/bash
#File: create_test_harness.sh
#Desc:
#
# Amendment History:
# Date:      Who:       Desc:
# 19/09/19   T.Mullen   Initial;
#


. ../module_set_variables.sh
export LOG_FILE=/tmp/test_harness_install_`date +%Y%m%d-%H%M`.log
echo "INFO: Set variables" >> $LOG_FILE
echo "INFO: LOG_FILE parameter set to $LOG_FILE" >> $LOG_FILE
echo "INFO: MODULEOWNER parameter set to $MODULEOWNER" >> $LOG_FILE
echo "INFO: PGUSERNAME parameter set to $PGUSERNAME" >> $LOG_FILE
echo "INFO: SOURCEUSERNAME parameter set to $SOURCEUSERNAME" >> $LOG_FILE
echo "INFO: MVUSERNAME parameter set to $PGUSERNAME" >> $LOG_FILE
echo "INFO: HOSTNAME parameter set to $HOSTNAME" >> $LOG_FILE
echo "INFO: PORT parameter set to $PORT" >> $LOG_FILE
echo "INFO: DBNAME parameter set to $DBNAME" >> $LOG_FILE
echo "INFO: MODULE_HOME parameter set to $MODULE_HOME" >> $LOG_FILE

PGPASSWORD=$PGPASSWORD

function createsourceschema
{

echo "INFO: Creating Source Schema $SOURCEUSERNAME " >> $LOG_FILE

psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF1 >> $LOG_FILE 2>&1

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

EOF1

}

function createmvschema
{

echo "INFO: Creating MV Schema $MVUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF2 >> $LOG_FILE 2>&1

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
GRANT $MVUSERNAME  TO $MODULEOWNER;
GRANT $MODULEOWNER TO $MVUSERNAME;
GRANT USAGE ON SCHEMA $MODULEOWNER TO $MVUSERNAME;
GRANT ALL ON SCHEMA $SOURCEUSERNAME TO $MODULEOWNER;


GRANT   $SOURCEUSERNAME,$MVUSERNAME     TO dbadmin;
GRANT   $SOURCEUSERNAME,$MVUSERNAME   TO   $MODULEOWNER;

GRANT   pgmv\$_view, pgmv\$_usage                     TO      $MVUSERNAME;
GRANT   pgmv\$_view, pgmv\$_usage,    pgmv\$_execute   TO      $SOURCEUSERNAME;
GRANT USAGE ON FOREIGN SERVER pgmv\$_instance TO $MVUSERNAME;

EOF2
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
	id, lookup_id, code)
	VALUES (1, 10, 'hello');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code)
	VALUES (2, 20, 'bye');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code)
	VALUES (3, 30, 'cya');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code)
	VALUES (4, 50, 'goodbye');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code)
	VALUES (5, 50, 'hi');

INSERT INTO $SOURCEUSERNAME.test1(
	id, lookup_id, code)
	VALUES (6, 20, 'bye');

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
    cResult := $MODULEOWNER.mv\$createMaterializedViewlog( 'test1','$SOURCEUSERNAME');
    cResult := $MODULEOWNER.mv\$createMaterializedViewlog( 'test2','$SOURCEUSERNAME');
    cResult := $MODULEOWNER.mv\$createMaterializedViewlog( 'test3','$SOURCEUSERNAME');
    cResult := $MODULEOWNER.mv\$createMaterializedViewlog( 'test4','$SOURCEUSERNAME');
    cResult := $MODULEOWNER.mv\$createMaterializedViewlog( 'test5','$SOURCEUSERNAME');
    cResult := $MODULEOWNER.mv\$createMaterializedViewlog( 'test6','$SOURCEUSERNAME');

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
    pSqlStatement   TEXT;
BEGIN

    pSqlStatement := '
SELECT test1.id test1_id,
test1.lookup_id test1_lookup_id,
test1.code test1_code,
test2.id test2_id,
test2.description test2_desc,
test2.metavals_id test2_metavals_id,
test2.age test2_age,
test3.lookup_id test3_lookup_id,
test3.lookup_code test3_lookup_code,
test3.lookup_description test3_lookup_desc,
test4.metavals_id test4_metavals_id,
test4.code test4_code,
test4.description test4_desc,
test5.id test5_id,
test5.rep_ind test5_rep_ind,
test5.trans_id test5_trans_id,
test6.trans_id test6_trans_id,
test6.payment_reference test6_payment_ref
FROM
test1
INNER JOIN test2 ON test1.id = test2.id
LEFT JOIN test3 ON test1.lookup_id = test3.lookup_id
LEFT JOIN test4 ON test2.metavals_id = test4.metavals_id
INNER JOIN test5 ON test1.id = test5.id
LEFT JOIN test6 ON test5.trans_id = test6.trans_id';
    cResult := mv\$createMaterializedView
    (
        pViewName           => 'mv_fast_refresh_funct_test',
        pSelectStatement    =>  pSqlStatement,
        pOwner              => '$MVUSERNAME',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;

EOF5
}

createsourceschema
createmvschema
createtestdata
createmvlogs
createtestmv

echo "INFO: Build Complete check logfile for status - $LOG_FILE"
