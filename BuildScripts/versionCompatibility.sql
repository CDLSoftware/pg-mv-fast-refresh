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

ln_separator_count  INTEGER := 0;

ls_existing_version_first INTEGER;

ls_existing_version_last INTEGER;

ls_version_first INTEGER;

ls_version_last INTEGER;

BEGIN

    SELECT
        clock_timestamp()
        INTO STRICT lt_start_time;
	
	
   	SELECT COUNT(*) FROM regexp_matches(ls_version, '\.', 'g') INTO ln_separator_count;
	
	IF ln_separator_count != 1 THEN
		
		RAISE EXCEPTION 'Got exception:
			ERROR 1: Patch version format % is not compatible. Ensure one separator is used - exiting...', ls_version;
			
	END IF;
	
	RAISE INFO 'Apply version control in table PG$MVIEWS_VERSION_CONTROL start time: %', lt_start_time;

	EXECUTE 'SELECT count(1)
	FROM '||pis_module_owner||'.pg$mviews_version_control'
	INTO ln_version_count;

	ls_sql := 'INSERT INTO '||pis_module_owner||'.pg$mviews_version_control(version, live_version_flag, created)
		VALUES('''||ls_version||''', ''Y'', clock_timestamp())
		ON CONFLICT (version)
		DO NOTHING';

	IF ln_version_count = 0 THEN

		EXECUTE ls_sql;

	ELSE

		EXECUTE 'SELECT version
		FROM '||pis_module_owner||'.pg$mviews_version_control
		WHERE live_version_flag = ''Y'''
		INTO ls_existing_version;

        SELECT split_part(ls_version, '.', '1')::INTEGER INTO ls_version_first;
        SELECT split_part(ls_version, '.', '2')::INTEGER INTO ls_version_last;

        SELECT split_part(ls_existing_version, '.', '1')::INTEGER INTO ls_existing_version_first;
        SELECT split_part(ls_existing_version, '.', '2')::INTEGER INTO ls_existing_version_last;

        IF ls_version_first > ls_existing_version_first THEN

            EXECUTE ls_sql;	

        ELSEIF ls_version_first = ls_existing_version_first THEN

            IF ls_version_last >= ls_existing_version_last THEN

                EXECUTE ls_sql;

            ELSE

                RAISE EXCEPTION 'Got exception:
                ERROR 2: Patch compatibility requirements not met - this has already been applied. The pg$mviews_version_control table latest build version is % whilst the patch version is % - exiting...', ls_existing_version, ls_version;

            END IF;

        ELSE

            RAISE EXCEPTION 'Got exception:
            ERROR 3: Patch compatibility requirements not met - this has already been applied. The pg$mviews_version_control table latest build version is % whilst the patch version is % - exiting...', ls_existing_version, ls_version;

        END IF;

		EXECUTE 'UPDATE '||pis_module_owner||'.pg$mviews_version_control SET live_version_flag = ''N'' WHERE version <> '''||ls_version||'''';

	END IF;

		SELECT
			clock_timestamp()
			INTO STRICT lt_end_time;

	RAISE INFO 'Apply version control in table PG$MVIEW_VERSION_CONTROL completion time: %', lt_end_time;

END;
$BODY$
LANGUAGE  plpgsql;

SELECT version_compatibility(:'PATCHVERSION',:'MODULEOWNER');

drop function version_compatibility;

