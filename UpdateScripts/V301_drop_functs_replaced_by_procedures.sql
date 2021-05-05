DO $$
DECLARE

each_row RECORD;

BEGIN

	FOR each_row IN (select 'DROP FUNCTION IF EXISTS '||proname AS drop_function
	   FROM pg_proc 
	   W prokind = 'f'
	   AND proname IN ('mv$addindextomvlog$table',
			'mv$addindextomvlog$table',
			'mv$addrow$tomv$table',
			'mv$addrow$tosourcetable',
			'mv$clearspentpgmviewlogs',
			'mv$createmvlog$table',
			'mv$createmvlogtrigger',
			'mv$deletematerializedviewrows',
			'mv$deletepgmview',
			'mv$deletepgmviewojdetails',
			'mv$deletepgmviewlog',
			'mv$droptable',
			'mv$droptrigger',
			'mv$grantselectprivileges',
			'mv$insertpgmviewlogs',
			'mv$removerow$fromsourcetable',
			'mv$truncatematerializedview',
			'mv$clearallpgmvlogtablebits',
			'mv$clearpgmvlogtablebits',
			'mv$clearpgmviewlogbit',
			'mv$createpgmv$table',
			'mv$insertmaterializedviewrows',
			'mv$insertpgmview',
			'mv$insertouterjoinrows',
			'mv$insertpgmviewouterjoindetails',
			'mv$executemvfastrefresh',
			'mv$refreshmaterializedviewfast',
			'mv$refreshmaterializedviewfull',
			'mv$setpgmviewlogbit',
			'mv$updatematerializedviewrows',
			'mv$updateouterjoincolumnsnull',
			'mv$creatematerializedview',
			'mv$creatematerializedviewlog',
			'mv$refreshmaterializedview',
			'mv$removematerializedview',
			'mv$removematerializedviewlog')) LOOP
			
		EXECUTE ech_row.drop_function;
		
	END LOOP;
	
END $$;