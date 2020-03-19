#! /bin/bash
#File: dropFastRefreshModule.sh
#Desc:
#
# Amendment History:
# Date:      Who:       Desc:
# 28/10/19   T.Mullen   Initial;
#


. ./module_set_variables.sh
export LOG_FILE=/tmp/dropFastRefreshModule_`date +%Y%m%d-%H%M`.log
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


function dropmodule
{
echo "INFO: Truncating modules tables" >> $LOG_FILE

PGPASSWORD=$PGPASS


psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME << EOF1 >> $LOG_FILE 2>&1

 REVOKE ALL PRIVILEGES ON DATABASE "$DBNAME" from $MODULEOWNER;
 DROP USER MAPPING IF EXISTS FOR $MODULEOWNER SERVER pgmv\$_instance;
 DROP SERVER IF EXISTS pgmv\$_instance CASCADE;
 DROP EXTENSION IF EXISTS postgres_fdw;
 DROP EXTENSION IF EXISTS dblink;
 DROP SCHEMA $MODULEOWNER CASCADE;
 DROP ROLE pgmv\$_role;
 REVOKE $MODULEOWNER from $PGUSERNAME;
 DROP EXTENSION IF EXISTS "uuid-ossp";
 DROP ROLE $MODULEOWNER;
 DROP ROLE pgmv\$_execute;
 DROP ROLE pgmv\$_view;
 DROP ROLE pgmv\$_usage;

EOF1
}


read -p "Are you sure you want to remove the module schema - $MODULEOWNER (y/n)?" choice
case "$choice" in
  y|Y ) echo "yes selected the schemas - $MODULEOWNER will be dropped"
        dropmodule;;
  n|N ) echo "no selected so exiting";;
  * ) echo "invalid choice exiting";;
esac


echo "INFO: Drop Module Schema complete check logfile for status - $LOG_FILE"
