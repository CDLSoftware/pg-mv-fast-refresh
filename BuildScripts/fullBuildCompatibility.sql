CREATE OR REPLACE FUNCTION fullbuild_compatibility(IN pis_module_owner TEXT)
RETURNS void
AS
$BODY$
DECLARE

lt_start_time 	TIMESTAMP(6) WITHOUT TIME ZONE;
lt_end_time 	TIMESTAMP(6) WITHOUT TIME ZONE;
ln_count		INTEGER := 0;

BEGIN

	SELECT
        clock_timestamp()
        INTO STRICT lt_start_time;
		
	RAISE INFO 'Full build compatibility check start time: %', lt_start_time;
		
	EXECUTE 'SELECT count(1)
	FROM '||pis_module_owner||'.pg$mview_logs'
	INTO ln_count;
	
	IF ln_count = 0 THEN
	
		RAISE INFO 'Full build is compatible check successful.';
		
	ELSE
	
		RAISE EXCEPTION 'Got exception:
                ERROR: Full build compatibility requirements not met. Configuration exists in table pg$mview_logs which is not supported as this would cause triggers to be dropped - exiting...';

	END IF;

	SELECT
		clock_timestamp()
		INTO STRICT lt_end_time;

	RAISE INFO 'Full build compatibility check completion time: %', lt_end_time;

END;
$BODY$
LANGUAGE  plpgsql;

SELECT fullbuild_compatibility(:'MODULEOWNER');

drop function fullbuild_compatibility;

