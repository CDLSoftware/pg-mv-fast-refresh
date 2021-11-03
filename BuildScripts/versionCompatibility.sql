CREATE OR REPLACE FUNCTION version_compatibility(IN pis_version TEXT, IN pis_module_owner TEXT)
RETURNS void
AS
$BODY$
DECLARE

ls_version CHARACTER VARYING(300) := pis_version;

ls_existing_version CHARACTER VARYING(300);

ls_sql TEXT;

lt_start_time TIMESTAMP(6) WITHOUT TIME ZONE;
lt_end_time TIMESTAMP(6) WITHOUT TIME ZONE;

ln_new_version		INTEGER;

ln_existing_version INTEGER;

ln_reformat_version	INTEGER;

ln_version_count	INTEGER := 0;

BEGIN

    SELECT
        clock_timestamp()
        INTO STRICT lt_start_time;
		
RAISE INFO 'Apply version control in table PG$MVIEWS_VERSION_CONTROL start time: %', lt_start_time;
		
SELECT count(1) INTO ln_version_count
FROM pg$mviews_version_control;

ls_sql := 'INSERT INTO '||pis_module_owner||'.pg$mviews_version_control(version, live_version_flag, created)
	VALUES('''||ls_version||''', ''Y'', clock_timestamp())
	ON CONFLICT (version)
	DO NOTHING';

IF ln_version_count = 0 THEN

	EXECUTE ls_sql;

ELSE

	SELECT version INTO ls_existing_version
	FROM pg$mviews_version_control 
	WHERE live_version_flag = 'Y';

	SELECT REPLACE(ls_version,'.','')::INTEGER INTO ln_new_version;
	SELECT REPLACE(ls_existing_version,'.','')::INTEGER INTO ln_existing_version;
	
	RAISE INFO '%',ln_new_version;
	RAISE INFO '%',ln_existing_version;

	IF LENGTH(ln_existing_version::TEXT) > LENGTH(ln_new_version::TEXT) THEN

		SELECT rpad(ln_new_version::TEXT, LENGTH(ln_existing_version::TEXT)::INTEGER, '000000000000'::TEXT)::INTEGER INTO ln_reformat_version;
		
		IF ln_reformat_version > ln_existing_version THEN
		
			EXECUTE ls_sql;
			
		ELSE
		
			RAISE EXCEPTION 'Got exception:
			ERROR: Patch compatibility requirements not met - this has already been applied. The pg$mviews_version_control table latest build version is % whilst the patch version is % - exiting...', ls_existing_version, ls_version;
			
		END IF;
			
	ELSIF LENGTH(ln_new_version::TEXT) > LENGTH(ln_existing_version::TEXT) THEN
	
		SELECT rpad(ln_existing_version::TEXT, LENGTH(ln_new_version::TEXT)::INTEGER, '000000000000'::TEXT)::INTEGER INTO ln_reformat_version;
		
		IF ln_new_version > ln_reformat_version THEN
		
			EXECUTE ls_sql;
			
		ELSE
		
			RAISE EXCEPTION 'Got exception:
			ERROR: Patch compatibility requirements not met - this has already been applied. The pg$mviews_version_control table latest build version is % whilst the patch version is % - exiting...', ls_existing_version, ls_version;
			
		END IF;
		
	ELSIF LENGTH(ln_new_version::TEXT) = LENGTH(ln_existing_version::TEXT) THEN
	
		IF ln_new_version > ln_existing_version THEN
	
			EXECUTE ls_sql;	
			
		ELSIF ln_new_version = ln_existing_version THEN
		
			EXECUTE ls_sql;	

		ELSE
		
			RAISE EXCEPTION 'Got exception:
			ERROR: Patch compatibility requirements not met - this has already been applied. The pg$mviews_version_control table latest build version is % whilst the patch version is % - exiting...', ls_existing_version, ls_version;
			
		END IF;
		
	END IF;
	
	EXECUTE 'UPDATE '||pis_module_owner||'.pg$mviews_version_control SET live_version_flag = ''N'' WHERE version <> '''||ls_version||'''';
	
END IF;

    SELECT
        clock_timestamp()
        INTO STRICT lt_end_time;
		
RAISE INFO 'Apply version control in table PG$MVIEWS_VERSION_CONTROL completion time: %', lt_end_time;

END;
$BODY$
LANGUAGE  plpgsql;

SELECT version_compatibility(:'PATCHVERSION',:'MODULEOWNER');

drop function version_compatibility;

