#! /bin/bash
#File: dropFastRefreshDataSchemas.sh
#Desc:
#
# Amendment History:
# Date:      Who:       Desc:
# 28/10/19   T.Mullen   Initial;
#


. ./module_set_variables.sh
export LOG_FILE=/tmp/dropFastRefreshDataSchemas_`date +%Y%m%d-%H%M`.log
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

function dropmvschema
{

echo "INFO: Dropping MV user $MVUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASS


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF3 >> $LOG_FILE 2>&1

 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $MVUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $MVUSERNAME to $PGUSERNAME;
 REVOKE USAGE ON FOREIGN SERVER pgmv\$_instance FROM $MVUSERNAME;
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
echo "INFO: Dropping Schema user $SOURCEUSERNAME " >> $LOG_FILE

PGPASSWORD=$PGPASS


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF3 >> $LOG_FILE 2>&1

 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $SOURCEUSERNAME;
 GRANT ALL PRIVILEGES ON SCHEMA $SOURCEUSERNAME to $PGUSERNAME;
 REVOKE USAGE ON FOREIGN SERVER pgmv\$_instance FROM $SOURCEUSERNAME;
 ALTER SCHEMA $SOURCEUSERNAME OWNER TO $PGUSERNAME;
 DROP SCHEMA $SOURCEUSERNAME CASCADE;
 REVOKE $SOURCEUSERNAME from $PGUSERNAME;
 REVOKE $SOURCEUSERNAME FROM $MODULEOWNER;
 REVOKE USAGE ON SCHEMA $SOURCEUSERNAME FROM $MODULEOWNER;
 REVOKE ALL ON SCHEMA $MODULEOWNER FROM $SOURCEUSERNAME;
 DROP USER $SOURCEUSERNAME;


EOF3
}


function truncatemoduletbls
{
echo "INFO: Truncating modules tables" >> $LOG_FILE

PGPASSWORD=$MODULEOWNERPASS


psql --host=$HOSTNAME --port=$PORT --username=$MODULEOWNER --dbname=$DBNAME << EOF4 >> $LOG_FILE 2>&1

truncate table pgmview_logs;
truncate table pgmviews;
truncate table pgmviews_oj_details;


EOF4
}


read -p "Are you sure you want to remove the schemas - $MVUSERNAME and $SOURCEUSERNAME (y/n)?" choice
case "$choice" in
  y|Y ) echo "yes selected the schemas - $MVUSERNAME and $SOURCEUSERNAME will be dropped"
        dropmvschema
        dropsourceschema
        truncatemoduletbls;;
  n|N ) echo "no selected so exiting";;
  * ) echo "invalid choice exiting";;
esac


echo "INFO: Drop Complete check logfile for status - $LOG_FILE"
