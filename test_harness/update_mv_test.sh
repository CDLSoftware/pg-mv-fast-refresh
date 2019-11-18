#! /bin/bash
#File: update_mv_test.sh
#Desc:
#
# Amendment History:
# Date:      Who:       Desc:
# 19/09/19   T.Mullen   Initial;
#


. ../module_set_variables.sh
export LOG_FILE=/tmp/update_mv_`date +%Y%m%d-%H%M`.log
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


echo "INFO: Changing a row for table test1"
echo "UPDATE $SOURCEUSERNAME.test1 set code='yo' where code='hello'"
read -p "Press the enter key to do the update..."

PGPASSWORD=$SOURCEPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF1

UPDATE $SOURCEUSERNAME.test1 set code='yo' where code='hello'


EOF1


echo "INFO: Check the output from the MV"
echo "Select * from mv_fast_refresh_funct_test order by test1_id;"
read -p "Press the enter key to see the MV output..."

PGPASSWORD=$MVPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF2

select * from mv_fast_refresh_funct_test order by test1_id;

EOF2

echo "INFO: Check the output from the MV we still have code hello for test1_id"
echo "INFO: Lets do a MV fast refresh to sync the MV with the change"
read -p "Press the enter key to do the MV refresh..."

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF3

DO
\$do\$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv\$refreshMaterializedView
    (
        pViewName       => 'mv_fast_refresh_funct_test',
        pOwner          => '$MVUSERNAME',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;

EOF3

echo "INFO: Now Check the output from the MV the code for test1_id is now yo"
read -p "Press the enter key to see the MV output..."

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF4

select * from mv_fast_refresh_funct_test order by test1_id;

EOF4

echo "INFO: Now lets change the code back to hello"
echo "UPDATE $SOURCEUSERNAME.test1 set code='hello' where code='yo'"
read -p "Press the enter key to do the update..."

PGPASSWORD=$SOURCEPASSWORD


psql --host=$HOSTNAME --port=$PORT --username=$SOURCEUSERNAME --dbname=$DBNAME << EOF5

UPDATE $SOURCEUSERNAME.test1 set code='hello' where code='yo'


EOF5

PGPASSWORD=$MVPASSWORD

echo "INFO: Lets do a MV fast refresh to sync the MV with the change"
read -p "Press the enter key to do the MV refresh..."

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF6

DO
\$do\$
DECLARE
    tStartTime  TIMESTAMP   := clock_timestamp();
    cResult     CHAR(1)     := NULL;
BEGIN
    cResult := mv\$refreshMaterializedView
    (
        pViewName       => 'mv_fast_refresh_funct_test',
        pOwner          => '$MVUSERNAME',
        pFastRefresh    =>  TRUE
    );
    RAISE NOTICE 'Fast Snapshot Refresh took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;

EOF6

echo "INFO: Now Check the output from the MV the code for test1_id its back to hello"
read -p "Press the enter key to see the MV output..."

psql --host=$HOSTNAME --port=$PORT --username=$MVUSERNAME --dbname=$DBNAME << EOF7

select * from mv_fast_refresh_funct_test order by test1_id;

EOF7
