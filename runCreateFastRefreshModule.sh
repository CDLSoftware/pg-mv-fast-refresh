#! /bin/bash

. ./$MODULE_HOME/module_set_variables.sh

echo "INFO: Set variables" >> $LOG_FILE
echo "INFO: LOG_FILE parameter set to $LOG_FILE" >> $LOG_FILE
echo "INFO: MODULEOWNER parameter set to $MODULEOWNER" >> $LOG_FILE
echo "INFO: PGUSERNAME parameter set to $PGUSERNAME" >> $LOG_FILE
echo "INFO: HOSTNAME parameter set to $HOSTNAME" >> $LOG_FILE
echo "INFO: PORT parameter set to $PORT" >> $LOG_FILE
echo "INFO: DBNAME parameter set to $DBNAME" >> $LOG_FILE
echo "INFO: MODULE_HOME parameter set to $MODULE_HOME" >> $LOG_FILE

chmod 771 -R $MODULE_HOME/

echo "INFO: Run $MODULEOWNER schema build script" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER -v PGUSERNAME=$PGUSERNAME -v DBNAME=$DBNAME << EOF1 >> $LOG_FILE 2>&1

    \i :MODULE_HOME/BuildScripts/createModuleOwnerSchema.sql;
	
	\q
	
EOF1

PGPASSWORD=$MODULEOWNERPASS

echo "INFO: Run $MODULEOWNER schema object build scripts" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$MODULEOWNER --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME << EOF2 >> $LOG_FILE 2>&1
  
   \i :MODULE_HOME/BuildScripts/mvConstants.sql;
   \i :MODULE_HOME/BuildScripts/mvSimpleFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvComplexFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvApplicationFunctions.sql;
	
  \q

EOF2

$MODULE_HOME/module_error_chks.sh

exitcode=$?
if [ $exitcode != 0 ]; then
	exit 1
fi

exit