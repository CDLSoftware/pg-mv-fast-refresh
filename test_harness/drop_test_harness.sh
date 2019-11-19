#! /bin/bash
#File: drop_test_harness.sh
#Desc:
#
# Amendment History:
# Date:      Who:       Desc:
# 19/09/19   T.Mullen   Initial;
#

. ../module_set_variables.sh

export LOG_FILE=/tmp/test_harness_drop_`date +%Y%m%d-%H%M`.log
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

PGPASS=$PGPASSWORD


function dropmvdata
{
PGPASSWORD=$MVPASSWORD

echo "INFO: Dropping MV and MV Logs " >> $LOG_FILE

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF1 >> $LOG_FILE 2>&1


DO
\$do\$
DECLARE
    cResult         CHAR(1)     := NULL;
BEGIN
    cResult := mv\$removeMaterializedView( 'mv_fast_refresh_funct_test', 'testpocmv' );
END
\$do\$;

EOF1

PGPASSWORD=$SOURCEPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF2 >> $LOG_FILE 2>&1

DO
\$do\$
DECLARE
    cResult         CHAR(1)     := NULL;
BEGIN
    cResult := mv\$removeMaterializedViewLog( 't1', 'testpocsource' );
    cResult := mv\$removeMaterializedViewLog( 't2', 'testpocsource' );
    cResult := mv\$removeMaterializedViewLog( 't3', 'testpocsource' );
    cResult := mv\$removeMaterializedViewLog( 't4', 'testpocsource' );
    cResult := mv\$removeMaterializedViewLog( 't5', 'testpocsource' );
    cResult := mv\$removeMaterializedViewLog( 't6', 'testpocsource' );
END
\$do\$;

EOF2

}

function dropmvschema
{

echo "INFO: Dropping MV user $MVUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASS


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF3 >> $LOG_FILE 2>&1

 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $MVUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $MVUSERNAME to $PGUSERNAME;
 ALTER SCHEMA $MVUSERNAME OWNER TO $PGUSERNAME;
 DROP SCHEMA $MVUSERNAME CASCADE;
 REVOKE $SOURCEUSERNAME from $MVUSERNAME;
 revoke pgmv$_role from $MVUSERNAME;
 revoke $SOURCEUSERNAME from $MODULEOWNER;
 revoke $MVUSERNAME  from $MODULEOWNER;
 revoke $MODULEOWNER from $MVUSERNAME ;
 revoke $MVUSERNAME from $PGUSERNAME;
 REVOKE ALL PRIVILEGES ON SCHEMA $MVUSERNAME from $MVUSERNAME;
 revoke USAGE ON SCHEMA $MODULEOWNER from $MVUSERNAME ;
 DROP USER $MVUSERNAME;

EOF3
}

function dropsourceschema
{
echo "INFO: Dropping Schema user $SCHEMAUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASS


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF4 >> $LOG_FILE 2>&1

 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $SOURCEUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $SOURCEUSERNAME to $PGUSERNAME;
 ALTER SCHEMA $SOURCEUSERNAME OWNER TO $PGUSERNAME;
 DROP SCHEMA $SOURCEUSERNAME CASCADE;
 REVOKE $SOURCEUSERNAME from $PGUSERNAME;
 REVOKE $SOURCEUSERNAME FROM $MODULEOWNER;
 REVOKE USAGE ON SCHEMA $SOURCEUSERNAME FROM $MODULEOWNER;
 REVOKE ALL ON SCHEMA $MODULEOWNER FROM $SOURCEUSERNAME;
 DROP USER $SOURCEUSERNAME;


EOF4
}

dropmvdata
dropmvschema
dropsourceschema


echo "INFO: Drop Complete check logfile for status - $LOG_FILE"
