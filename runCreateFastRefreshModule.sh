#! /bin/bash

. ./module_set_variables.sh

echo "INFO: Set variables" >> $LOG_FILE
echo "INFO: LOG_FILE parameter set to $LOG_FILE" >> $LOG_FILE
echo "INFO: MODULEOWNER parameter set to $MODULEOWNER" >> $LOG_FILE
echo "INFO: PGUSERNAME parameter set to $PGUSERNAME" >> $LOG_FILE
echo "INFO: HOSTNAME parameter set to $HOSTNAME" >> $LOG_FILE
echo "INFO: PORT parameter set to $PORT" >> $LOG_FILE
echo "INFO: DBNAME parameter set to $DBNAME" >> $LOG_FILE
echo "INFO: MODULE_HOME parameter set to $MODULE_HOME" >> $LOG_FILE
echo "INFO: INSTALL_TYPE parameter set to $INSTALL_TYPE" >> $LOG_FILE

if [ "$INSTALL_TYPE" == "FULL" ]; then

echo "INFO: Fast Refresh Module FULL Install started at `date`" >> $LOG_FILE

elif [ "$INSTALL_TYPE" == "UPDATE" ]; then

echo "INFO: Fast Refresh Module UPDATE patch started at `date`" >> $LOG_FILE

fi

chmod 771 -R $MODULE_HOME/

export PATCHVERSION="$(cat $MODULE_HOME/read_my_version.txt)"
export PATCHVERSION=$(echo $PATCHVERSION | tr -d ' ')

echo "Patch version: $PATCHVERSION" >> $LOG_FILE


function versioncontrol
{

echo "INFO: Run $MODULEOWNER version control script" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$MODULEOWNER --dbname=$DBNAME ON_ERROR_STOP=1 -v MODULE_HOME=$MODULE_HOME -v MODULEOWNER=$MODULEOWNER -v PATCHVERSION=$PATCHVERSION << EOFV >> $LOG_FILE 2>&1

    \i :MODULE_HOME/UpdateScripts/V704_create_table_pgmviews_version_control.sql;
    \i :MODULE_HOME/BuildScripts/versionCompatibility.sql;

	\q
	
EOFV

exitcode=$?
if [ $exitcode != 0 ]; then
	exit 1
fi

}

if [ "$INSTALL_TYPE" == "FULL" ]; then

echo "INFO: Run $MODULEOWNER schema build script" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER -v PGUSERNAME=$PGUSERNAME -v DBNAME=$DBNAME -v HOSTNAME=$HOSTNAME -v PORT=$PORT << EOF1 >> $LOG_FILE 2>&1

    \i :MODULE_HOME/BuildScripts/createModuleOwnerSchema.sql;

	\q
	
EOF1

echo "INFO: Run Cron setup script" >> $LOG_FILE
echo "INFO: Connect to postgres database via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER << EOF2 >> $LOG_FILE 2>&1
	
	\i :MODULE_HOME/BuildScripts/createCronSetup.sql;

	\q
	
EOF2

PGPASSWORD=$MODULEOWNERPASS

versioncontrol

echo "INFO: Run $MODULEOWNER schema object build scripts" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE
  psql --host=$HOSTNAME --port=$PORT --username=$MODULEOWNER --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER -v HOSTNAME=$HOSTNAME -v PORT=$PORT << EOF3 >> $LOG_FILE 2>&1
  
	SET search_path = :MODULEOWNER,catalog,public;
	
	-- Used to run materialized view build insert in parallel sessions
	CREATE SERVER IF NOT EXISTS pgmv\$cron_instance FOREIGN DATA WRAPPER postgres_fdw options ( dbname 'postgres', port :'PORT', host :'HOSTNAME', connect_timeout '2', keepalives_count '5' );
	CREATE USER MAPPING IF NOT EXISTS for :MODULEOWNER SERVER pgmv\$cron_instance OPTIONS (user :'MODULEOWNER', password :'MODULEOWNERPASS');
	GRANT USAGE ON FOREIGN SERVER pgmv\$cron_instance TO :MODULEOWNER;

   \i :MODULE_HOME/BuildScripts/mvTypes.sql;
   \i :MODULE_HOME/BuildScripts/mvConstants.sql;
   \i :MODULE_HOME/BuildScripts/mvSimpleFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvComplexFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvApplicationFunctions.sql;
   \i :MODULE_HOME/BuildScripts/mvTriggerFunction.sql;
	
  \q

EOF3

elif [ "$INSTALL_TYPE" == "UPDATE" ]; then

touch $MODULE_HOME/fast_refresh_module_update_patch_objects.sql
chmod 771 $MODULE_HOME/fast_refresh_module_update_patch_objects.sql
truncate -s 0 $MODULE_HOME/fast_refresh_module_update_patch_objects.sql

for file in $MODULE_HOME/UpdateScripts/*.sql

do

UPDATE_SCRIPTS_SQL=$(echo "\\i $file;")$'\n'

echo $UPDATE_SCRIPTS_SQL >> $MODULE_HOME/fast_refresh_module_update_patch_objects.sql

done

UPDATE_FUNCTIONS=$(echo "\\i $MODULE_HOME/BuildScripts/mvTypes.sql;")$'\n'
echo $UPDATE_FUNCTIONS >> $MODULE_HOME/fast_refresh_module_update_patch_objects.sql
UPDATE_FUNCTIONS=$(echo "\\i $MODULE_HOME/BuildScripts/mvConstants.sql;")$'\n'
echo $UPDATE_FUNCTIONS >> $MODULE_HOME/fast_refresh_module_update_patch_objects.sql
UPDATE_FUNCTIONS=$(echo "\\i $MODULE_HOME/BuildScripts/mvSimpleFunctions.sql;")$'\n'
echo $UPDATE_FUNCTIONS >> $MODULE_HOME/fast_refresh_module_update_patch_objects.sql
UPDATE_FUNCTIONS=$(echo "\\i $MODULE_HOME/BuildScripts/mvComplexFunctions.sql;")$'\n'
echo $UPDATE_FUNCTIONS >> $MODULE_HOME/fast_refresh_module_update_patch_objects.sql
UPDATE_FUNCTIONS=$(echo "\\i $MODULE_HOME/BuildScripts/mvApplicationFunctions.sql;")$'\n'
echo $UPDATE_FUNCTIONS >> $MODULE_HOME/fast_refresh_module_update_patch_objects.sql

 psql --host=$HOSTNAME --port=$PORT --username=$PGUSERNAME --dbname=postgres -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER << EOF4 >> $LOG_FILE 2>&1
	
	\i :MODULE_HOME/BuildScripts/createCronSetup.sql;

	\q
	
EOF4

PGPASSWORD=$MODULEOWNERPASS

versioncontrol

echo "INFO: Run $MODULEOWNER schema UPDATE patch scripts" >> $LOG_FILE
echo "INFO: Connect to postgres database $DBNAME via PSQL session" >> $LOG_FILE

psql --host=$HOSTNAME --port=$PORT --username=$MODULEOWNER --dbname=$DBNAME -v MODULE_HOME=$MODULE_HOME -v MODULEOWNERPASS=$MODULEOWNERPASS -v MODULEOWNER=$MODULEOWNER -v HOSTNAME=$HOSTNAME -v PORT=$PORT  << EOF5 >> $LOG_FILE 2>&1
 
	SET search_path = :MODULEOWNER,catalog,public;
 
   \i :MODULE_HOME/fast_refresh_module_update_patch_objects.sql;

EOF5

fi

$MODULE_HOME/module_error_chks.sh

echo "Check log file - $LOG_FILE"

exitcode=$?
if [ $exitcode != 0 ]; then
	exit 1
fi

if [ "$INSTALL_TYPE" == "FULL" ]; then

echo "INFO: Fast Refresh Module FULL Install finished at `date`" >> $LOG_FILE

elif [ "$INSTALL_TYPE" == "UPDATE" ]; then

echo "INFO: Fast Refresh Module UPDATE patch finished at `date`" >> $LOG_FILE

fi

exit
