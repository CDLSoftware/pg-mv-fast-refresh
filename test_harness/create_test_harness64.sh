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

export testname=$1

PGPASSWORD=$PGPASSWORD

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
SELECT t1.id t1_id,
t1.lookup_id t1_lookup_id,
t1.code t1_code,
t2.id t2_id,
t2.description t2_desc,
t2.metavals_id t2_metavals_id,
t2.age t2_age,
t3.lookup_id t3_lookup_id,
t3.lookup_code t3_lookup_code,
t3.lookup_description t3_lookup_desc,
t4.metavals_id t4_metavals_id,
t4.code t4_code,
t4.description t4_desc,
t5.id t5_id,
t5.rep_ind t5_rep_ind,
t5.trans_id t5_trans_id,
t6.trans_id t6_trans_id,
t6.payment_reference t6_payment_ref
FROM
t1
INNER JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t1.lookup_id = t3.lookup_id
LEFT JOIN t4 ON t2.metavals_id = t4.metavals_id
INNER JOIN t5 ON t1.id = t5.id
LEFT JOIN t6 ON t5.trans_id = t6.trans_id';
    cResult := mv\$createMaterializedView
    (
        pViewName           => 'mv_fast_refresh_funct_test_$testname',
        pSelectStatement    =>  pSqlStatement,
        pOwner              => '$MVUSERNAME',
        pFastRefresh        =>  TRUE
    );
    RAISE NOTICE 'Complex Materialized View creation took % % %', clock_timestamp() - tStartTime, chr(10), chr(10);
END
\$do\$;

EOF5
echo "INFO: Build Complete check logfile for status - $LOG_FILE"
